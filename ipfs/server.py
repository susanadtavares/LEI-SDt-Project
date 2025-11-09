from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from sentence_transformers import SentenceTransformer
import requests
import uvicorn
import json
import base64
import numpy as np
import os
from pathlib import Path
from datetime import datetime, timedelta
import hashlib
import uuid
from typing import Dict, Set

app = FastAPI(title="IPFS Upload API")

IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"

VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
PENDING_UPLOADS_DIR = "pending_uploads"

Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(PENDING_UPLOADS_DIR).mkdir(exist_ok=True)

print("A carregar modelo SentenceTransformer...")
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("Modelo carregado com sucesso!")

voting_sessions: Dict[str, dict] = {}
my_peer_id = None

# Track de peers do sistema e não do IPFS global
peers: Dict[str, datetime] = {}
PEER_TIMEOUT = timedelta(seconds=30)

def get_my_peer_id():
    global my_peer_id
    if my_peer_id:
        return my_peer_id
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=5)
        if response.status_code == 200:
            my_peer_id = response.json()['ID']
            return my_peer_id
    except:
        pass
    return "unknown"

def register_peer(peer_id: str):
    peers[peer_id] = datetime.now()
    cleanup_inactive_peers()

def cleanup_inactive_peers():
    now = datetime.now()
    inactive = [pid for pid, last_seen in peers.items() 
                if now - last_seen > PEER_TIMEOUT]
    for pid in inactive:
        del peers[pid]
        print(f"⚠️  Peer removido por inatividade: {pid[:16]}...")

def get_peers_count():
    cleanup_inactive_peers()
    
    my_id = get_my_peer_id()
    if my_id not in peers:
        register_peer(my_id)
    
    count = len(peers)
    return count

def broadcast_heartbeat():
    try:
        message_data = {
            "type": "peer_heartbeat",
            "peer_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat()
        }
        
        message_json = json.dumps(message_data)
        
        requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=message_json.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )
    except:
        pass

def load_document_vector():
    if os.path.exists(VECTOR_FILE):
        with open(VECTOR_FILE, 'r') as f:
            return json.load(f)
    else:
        return {
            "version_confirmed": 0,
            "version_pending": 0,
            "documents_confirmed": [],
            "documents_pending": [],
            "documents_pending_approval": [],
            "documents_rejected": [],
            "last_updated": None
        }

def save_document_vector(vector_data):
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)

def create_voting_session(doc_id, filename, content):
    total_peers = get_peers_count()
    required_votes = (total_peers // 2) + 1  # 50% + 1
    
    voting_sessions[doc_id] = {
        "doc_id": doc_id,
        "filename": filename,
        "content": content,
        "status": "pending_approval",
        "total_peers": total_peers,
        "required_votes": required_votes,
        "votes_approve": set(),
        "votes_reject": set(),
        "created_at": datetime.now().isoformat(),
        "decided_at": None,
        "final_decision": None
    }
    
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    with open(temp_file, 'wb') as f:
        f.write(content)
    
    return voting_sessions[doc_id]

def process_vote(doc_id, peer_id, vote_type):
    if doc_id not in voting_sessions:
        return {"status": "error", "message": "Sessão de votação não encontrada"}
    
    session = voting_sessions[doc_id]
    
    if session["status"] != "pending_approval":
        return {"status": "error", "message": "Votação já encerrada"}
    
    session["votes_approve"].discard(peer_id)
    session["votes_reject"].discard(peer_id)
    
    if vote_type == "approve":
        session["votes_approve"].add(peer_id)
    elif vote_type == "reject":
        session["votes_reject"].add(peer_id)
    
    approve_count = len(session["votes_approve"])
    reject_count = len(session["votes_reject"])
    required = session["required_votes"]
    
    decision = None
    
    if approve_count >= required:
        decision = "approved"
        session["status"] = "approved"
        session["final_decision"] = "approved"
        session["decided_at"] = datetime.now().isoformat()
        
        result = finalize_approved_document(doc_id)
        return result
    
    elif reject_count >= required:
        decision = "rejected"
        session["status"] = "rejected"
        session["final_decision"] = "rejected"
        session["decided_at"] = datetime.now().isoformat()
        
        # Remove arquivo temporário
        finalize_rejected_document(doc_id)
        
        propagate_decision(doc_id, "rejected")
        
        return {
            "status": "rejected",
            "message": "Documento rejeitado pela maioria",
            "votes_approve": approve_count,
            "votes_reject": reject_count,
            "required_votes": required
        }
    
    return {
        "status": "voting",
        "message": "Voto registado, a aguardar mais votos",
        "votes_approve": approve_count,
        "votes_reject": reject_count,
        "required_votes": required,
        "votes_remaining": required - max(approve_count, reject_count)
    }

def finalize_approved_document(doc_id):
    session = voting_sessions[doc_id]
    filename = session["filename"]
    
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    with open(temp_file, 'rb') as f:
        content = f.read()
    
    try:
        files = {'file': (filename, content)}
        response = requests.post(
            f"{IPFS_API_URL}/add",
            files=files,
            params={'pin': 'true'}
        )
        
        if response.status_code != 200:
            return {"status": "error", "message": "Falha ao adicionar ao IPFS"}
        
        result = response.json()
        cid = result['Hash']
        
        # Cria embeddings
        text_content = extract_text_from_file(content, filename)
        embeddings = generate_embeddings(text_content)
        
        # Atualiza vetor
        new_version, updated_vector = add_document_to_vector(cid, filename, embeddings, confirmed=True)
        
        # Guarda embedding
        np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings)
        
        # Remove arquivo temporário
        os.remove(temp_file)
        
        # Notifica peers
        propagate_decision(doc_id, "approved", cid=cid, embeddings=embeddings, version=new_version)
        
        return {
            "status": "approved",
            "message": "Documento aprovado e adicionado ao IPFS",
            "cid": cid,
            "filename": filename,
            "vector_version": new_version,
            "votes_approve": len(session["votes_approve"]),
            "votes_reject": len(session["votes_reject"]),
            "required_votes": session["required_votes"]
        }
    
    except Exception as e:
        return {"status": "error", "message": f"Erro ao processar documento: {str(e)}"}

def finalize_rejected_document(doc_id):
    session = voting_sessions[doc_id]
    filename = session["filename"]
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    # Adiciona ao vetor como rejeitado
    vector = load_document_vector()
    vector.setdefault("documents_rejected", [])
    vector["documents_rejected"].append({
        "doc_id": doc_id,
        "filename": filename,
        "rejected_at": datetime.now().isoformat(),
        "votes_reject": len(session["votes_reject"]),
        "votes_approve": len(session["votes_approve"])
    })
    save_document_vector(vector)

def add_document_to_vector(cid, filename, embeddings, confirmed=False):
    vector = load_document_vector()
    
    vector.setdefault("version_confirmed", 0)
    vector.setdefault("version_pending", vector["version_confirmed"])
    vector.setdefault("documents_confirmed", [])
    vector.setdefault("documents_pending", [])
    
    new_version = vector["version_pending"] + 1
    
    doc_entry = {
        "cid": cid,
        "filename": filename,
        "added_at": datetime.now().isoformat(),
        "embedding_shape": list(embeddings.shape),
        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy",
        "confirmed": confirmed
    }
    
    if confirmed:
        vector["documents_confirmed"].append(doc_entry)
        vector["version_confirmed"] = new_version
        print(f"✅ Documento confirmado — versão {new_version}")
    else:
        vector["documents_pending"].append(doc_entry)
        vector["version_pending"] = new_version
        print(f"🕒 Documento pendente — versão {new_version}")
    
    vector["last_updated"] = datetime.now().isoformat()
    save_document_vector(vector)
    
    return new_version, vector

def generate_embeddings(text_content):
    try:
        embedding = embedding_model.encode(text_content, convert_to_numpy=True)
        return embedding
    except Exception as e:
        print(f"Erro ao criar embedding: {e}")
        raise

def extract_text_from_file(content, filename):
    try:
        text = content.decode('utf-8')
        return text
    except UnicodeDecodeError:
        return f"Document: {filename}"

def propagate_proposal(doc_id, filename):
    try:
        session = voting_sessions[doc_id]
        
        message_data = {
            "type": "document_proposal",
            "doc_id": doc_id,
            "filename": filename,
            "total_peers": session["total_peers"],
            "required_votes": session["required_votes"],
            "timestamp": datetime.now().isoformat(),
            "from_peer": get_my_peer_id()
        }
        
        message_json = json.dumps(message_data)
        
        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=message_json.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )
        
        if response.status_code == 200:
            print(f"📢 Proposta enviada: {filename} (ID: {doc_id})")
            return True
        return False
    
    except Exception as e:
        print(f"Erro ao propagar proposta: {e}")
        return False

def propagate_decision(doc_id, decision, cid=None, embeddings=None, version=None):
    try:
        session = voting_sessions.get(doc_id)
        if not session:
            return False
        
        message_data = {
            "type": f"document_{decision}",
            "doc_id": doc_id,
            "filename": session["filename"],
            "decision": decision,
            "votes_approve": len(session["votes_approve"]),
            "votes_reject": len(session["votes_reject"]),
            "timestamp": datetime.now().isoformat(),
            "from_peer": get_my_peer_id()
        }
        
        if decision == "approved" and cid:
            message_data["cid"] = cid
            message_data["version"] = version
            if embeddings is not None:
                message_data["embeddings"] = embeddings.tolist()
                message_data["embedding_shape"] = list(embeddings.shape)
        
        message_json = json.dumps(message_data)
        
        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=message_json.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )
        
        if response.status_code == 200:
            print(f"✅ Decisão propagada: {decision.upper()} - {session['filename']}")
            return True
        return False
    
    except Exception as e:
        print(f"Erro ao propagar decisão: {e}")
        return False

def propagate_vote(doc_id, vote_type):
    try:
        message_data = {
            "type": "peer_vote",
            "doc_id": doc_id,
            "vote": vote_type,
            "peer_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat()
        }
        
        message_json = json.dumps(message_data)
        
        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=message_json.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )
        
        return response.status_code == 200
    
    except Exception as e:
        print(f"Erro ao propagar voto: {e}")
        return False

# ============= ENDPOINTS =============

@app.get("/")
def root():
    vector = load_document_vector()
    return {
        "message": "API IPFS ativa",
        "status": "running",
        "version": "3.0",
        "document_vector_version": vector.get("version_confirmed", 0),
        "total_confirmed": len(vector.get("documents_confirmed", [])),
        "total_pending_approval": len(voting_sessions),
        "total_rejected": len(vector.get("documents_rejected", [])),
        "peers": get_peers_count(),
        "active_peers": list(peers.keys()),
        "endpoints": {
            "upload": "/upload (POST)",
            "vector": "/vector (GET)",
            "vote": "/vote/{doc_id}/{vote_type} (POST)",
            "voting_status": "/voting-status (GET)",
            "voting_status_doc": "/voting-status/{doc_id} (GET)",
            "documents": "/documents (GET)",
            "info": "/info/{cid} (GET)",
            "download": "/download/{cid} (GET)",
            "embedding": "/vector/{cid}/embedding (GET)",
            "search": "/search/{query} (GET)",
            "delete": "/document/{cid} (DELETE)",
            "status": "/status (GET)",
            "peers": "/peers (GET)",
            "notifications": "/notifications (GET - SSE Stream)",
            "docs": "/docs"
        }
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        filename = file.filename
        content = await file.read()
        
        print(f"\n{'='*60}")
        print(f"NOVO UPLOAD RECEBIDO: {filename}")
        print(f"{'='*60}")
        
        doc_id = str(uuid.uuid4())
        
        session = create_voting_session(doc_id, filename, content)
        
        print(f"📋 Sessão de votação criada")
        print(f"   └─ Doc ID: {doc_id}")
        print(f"   └─ Peers no sistema: {session['total_peers']}")
        print(f"   └─ Votos necessários: {session['required_votes']}")
        
        propagated = propagate_proposal(doc_id, filename)
        
        print(f"{'='*60}\n")
        
        return {
            'status': 'pending_approval',
            'message': 'Documento enviado para votação',
            'doc_id': doc_id,
            'filename': filename,
            'total_peers': session['total_peers'],
            'required_votes': session['required_votes'],
            'propagated': propagated
        }
    
    except Exception as e:
        print(f"Erro: {str(e)}")
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.post("/vote/{doc_id}/{vote_type}")
def vote_on_document(doc_id: str, vote_type: str):
    if vote_type not in ["approve", "reject"]:
        return JSONResponse(
            content={"error": "vote_type deve ser 'approve' ou 'reject'"},
            status_code=400
        )
    
    peer_id = get_my_peer_id()
    
    result = process_vote(doc_id, peer_id, vote_type)
    
    propagate_vote(doc_id, vote_type)
    
    return result

@app.get("/voting-status")
def get_all_voting_status():
    status_list = []
    
    for doc_id, session in voting_sessions.items():
        status_list.append({
            "doc_id": doc_id,
            "filename": session["filename"],
            "status": session["status"],
            "votes_approve": len(session["votes_approve"]),
            "votes_reject": len(session["votes_reject"]),
            "required_votes": session["required_votes"],
            "total_peers": session["total_peers"],
            "created_at": session["created_at"],
            "decided_at": session.get("decided_at"),
            "final_decision": session.get("final_decision")
        })
    
    return {
        "total_sessions": len(voting_sessions),
        "sessions": status_list
    }

@app.get("/voting-status/{doc_id}")
def get_voting_status(doc_id: str):
    if doc_id not in voting_sessions:
        return JSONResponse(
            content={"error": "Sessão de votação não encontrada"},
            status_code=404
        )
    
    session = voting_sessions[doc_id]
    
    return {
        "doc_id": doc_id,
        "filename": session["filename"],
        "status": session["status"],
        "votes_approve": len(session["votes_approve"]),
        "votes_reject": len(session["votes_reject"]),
        "required_votes": session["required_votes"],
        "total_peers": session["total_peers"],
        "votes_remaining": session["required_votes"] - max(len(session["votes_approve"]), len(session["votes_reject"])),
        "created_at": session["created_at"],
        "decided_at": session.get("decided_at"),
        "final_decision": session.get("final_decision"),
        "peers_voted_approve": list(session["votes_approve"]),
        "peers_voted_reject": list(session["votes_reject"])
    }

@app.get("/vector")
def get_document_vector():
    vector = load_document_vector()
    return {
        'version_confirmed': vector.get('version_confirmed', 0),
        'total_confirmed': len(vector.get('documents_confirmed', [])),
        'total_rejected': len(vector.get('documents_rejected', [])),
        'total_pending_approval': len(voting_sessions),
        'last_updated': vector.get('last_updated'),
        'documents_confirmed': vector.get('documents_confirmed', []),
        'documents_rejected': vector.get('documents_rejected', [])
    }

@app.get("/info/{cid}")
def file_info(cid: str):
    try:
        response = requests.post(
            f"{IPFS_API_URL}/object/stat",
            params={'arg': cid},
            timeout=10
        )
        
        if response.status_code != 200:
            return JSONResponse(
                content={'error': 'CID não encontrado no IPFS'},
                status_code=404
            )
        
        ipfs_info = response.json()
        
        has_embedding = os.path.exists(f"{EMBEDDINGS_DIR}/{cid}.npy")
        
        vector = load_document_vector()
        doc_info = None
        
        for doc in vector.get('documents_confirmed', []):
            if doc.get('cid') == cid:
                doc_info = doc
                break
        
        return {
            'cid': cid,
            'ipfs_info': ipfs_info,
            'has_embedding': has_embedding,
            'embedding_file': f"{EMBEDDINGS_DIR}/{cid}.npy" if has_embedding else None,
            'document_info': doc_info,
            'gateway_url': f'http://localhost:8080/ipfs/{cid}',
            'download_url': f'http://localhost:8080/ipfs/{cid}?download=true',
            'api_download': f'http://localhost:5000/download/{cid}',
            'api_embedding': f'http://localhost:5000/vector/{cid}/embedding' if has_embedding else None
        }
    
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/download/{cid}")
def download_file(cid: str):
    try:
        response = requests.post(
            f"{IPFS_API_URL}/cat",
            params={'arg': cid},
            timeout=30
        )
        
        if response.status_code != 200:
            return JSONResponse(
                content={'error': 'Ficheiro não encontrado'},
                status_code=404
            )
        
        # Tenta obter o nome do ficheiro
        vector = load_document_vector()
        filename = "downloaded_file"
        
        for doc in vector.get('documents_confirmed', []):
            if doc.get('cid') == cid:
                filename = doc.get('filename', filename)
                break
        
        return StreamingResponse(
            iter([response.content]),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/vector/{cid}/embedding")
def get_embedding(cid: str):
    try:
        embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
        if not os.path.exists(embedding_path):
            return JSONResponse(
                content={'error': 'Embedding não encontrado para este CID'},
                status_code=404
            )
        
        embedding = np.load(embedding_path)
        
        vector = load_document_vector()
        doc_info = None
        
        for doc in vector.get('documents_confirmed', []):
            if doc.get('cid') == cid:
                doc_info = doc
                break
        
        # Estatísticas do embedding
        stats = {
            'mean': float(np.mean(embedding)),
            'std': float(np.std(embedding)),
            'min': float(np.min(embedding)),
            'max': float(np.max(embedding)),
            'norm': float(np.linalg.norm(embedding))
        }
        
        return {
            'cid': cid,
            'shape': list(embedding.shape),
            'dimension': int(embedding.shape[0]),
            'embedding': embedding.tolist(),
            'statistics': stats,
            'document_info': doc_info,
            'embedding_file': embedding_path
        }
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/documents")
def list_all_documents():
    vector = load_document_vector()
    
    confirmed = []
    for doc in vector.get('documents_confirmed', []):
        confirmed.append({
            'cid': doc.get('cid'),
            'filename': doc.get('filename'),
            'added_at': doc.get('added_at'),
            'status': 'confirmed',
            'embedding_shape': doc.get('embedding_shape'),
            'has_embedding': os.path.exists(f"{EMBEDDINGS_DIR}/{doc.get('cid')}.npy"),
            'info_url': f'/info/{doc.get("cid")}',
            'download_url': f'/download/{doc.get("cid")}',
            'embedding_url': f'/vector/{doc.get("cid")}/embedding'
        })
    
    pending_approval = []
    for doc_id, session in voting_sessions.items():
        pending_approval.append({
            'doc_id': doc_id,
            'filename': session['filename'],
            'status': session['status'],
            'votes_approve': len(session['votes_approve']),
            'votes_reject': len(session['votes_reject']),
            'required_votes': session['required_votes'],
            'created_at': session['created_at']
        })
    
    rejected = vector.get('documents_rejected', [])
    
    return {
        'total_confirmed': len(confirmed),
        'total_pending_approval': len(pending_approval),
        'total_rejected': len(rejected),
        'documents_confirmed': confirmed,
        'documents_pending_approval': pending_approval,
        'documents_rejected': rejected
    }

@app.get("/search/{query}")
def search_documents(query: str):
    vector = load_document_vector()
    results = []
    
    query_lower = query.lower()
    
    for doc in vector.get('documents_confirmed', []):
        filename = doc.get('filename', '').lower()
        if query_lower in filename:
            results.append({
                'cid': doc.get('cid'),
                'filename': doc.get('filename'),
                'added_at': doc.get('added_at'),
                'info_url': f'/info/{doc.get("cid")}',
                'download_url': f'/download/{doc.get("cid")}',
                'embedding_url': f'/vector/{doc.get("cid")}/embedding'
            })
    
    return {
        'query': query,
        'results_count': len(results),
        'results': results
    }

@app.delete("/document/{cid}")
def delete_document(cid: str):
    try:
        vector = load_document_vector()
        
        original_count = len(vector.get('documents_confirmed', []))
        vector['documents_confirmed'] = [
            doc for doc in vector.get('documents_confirmed', [])
            if doc.get('cid') != cid
        ]
        
        if len(vector['documents_confirmed']) < original_count:
            save_document_vector(vector)
            
            embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
            if os.path.exists(embedding_path):
                os.remove(embedding_path)
            
            return {
                'status': 'success',
                'message': 'Documento removido do vetor',
                'cid': cid,
                'note': 'Ficheiro ainda existe no IPFS'
            }
        else:
            return JSONResponse(
                content={'error': 'CID não encontrado no vetor'},
                status_code=404
            )
    
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/status")
def ipfs_status():
    try:
        response = requests.post(f"{IPFS_API_URL}/version", timeout=5)
        if response.status_code == 200:
            version_info = response.json()
            vector = load_document_vector()
            return {
                'status': 'connected',
                'ipfs_version': version_info.get('Version'),
                'peer_id': get_my_peer_id(),
                'peers': get_peers_count(),
                'active_peers_list': list(peers.keys()),
                'vector_version': vector.get('version_confirmed', 0),
                'total_documents': len(vector.get('documents_confirmed', [])),
                'pending_votes': len(voting_sessions),
                'message': 'Sistema ativo'
            }
    except:
        return JSONResponse(
            content={'status': 'disconnected', 'message': 'IPFS não acessível'},
            status_code=503
        )

@app.get("/peers")
def get_peers():
    cleanup_inactive_peers()
    return {
        'total_peers': len(peers),
        'peers': [
            {
                'peer_id': pid,
                'last_seen': last_seen.isoformat(),
                'active': (datetime.now() - last_seen).seconds < 30
            }
            for pid, last_seen in peers.items()
        ]
    }

@app.get("/notifications")
async def get_notifications():
    async def event_stream():
        try:
            print(f"📡 Cliente conectado ao stream")
            
            my_id = get_my_peer_id()
            register_peer(my_id)
            broadcast_heartbeat()
            
            response = requests.post(
                f"{IPFS_API_URL}/pubsub/sub",
                params={'arg': CANAL_PUBSUB},
                stream=True,
                timeout=None
            )
            
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Conectado', 'canal': CANAL_PUBSUB, 'peer_id': my_id})}\n\n"
            
            for line in response.iter_lines():
                if line:
                    try:
                        msg = json.loads(line)
                        data_encoded = msg.get('data', '')
                        
                        try:
                            data_decoded = base64.b64decode(data_encoded).decode('utf-8')
                        except:
                            data_decoded = data_encoded
                        
                        try:
                            message_obj = json.loads(data_decoded)
                            msg_type = message_obj.get('type')
                            
                            if msg_type == 'peer_heartbeat':
                                peer_id = message_obj.get('peer_id')
                                if peer_id:
                                    register_peer(peer_id)
                                continue
                            
                            elif msg_type == 'document_proposal':
                                doc_id = message_obj['doc_id']
                                filename = message_obj['filename']
                                
                                if doc_id not in voting_sessions:
                                    voting_sessions[doc_id] = {
                                        "doc_id": doc_id,
                                        "filename": filename,
                                        "status": "pending_approval",
                                        "total_peers": message_obj['total_peers'],
                                        "required_votes": message_obj['required_votes'],
                                        "votes_approve": set(),
                                        "votes_reject": set(),
                                        "created_at": message_obj['timestamp'],
                                        "decided_at": None,
                                        "final_decision": None,
                                        "received_from_peer": True
                                    }
                                
                                notification = {
                                    'type': 'document_proposal',
                                    'doc_id': doc_id,
                                    'filename': filename,
                                    'required_votes': message_obj['required_votes'],
                                    'total_peers': message_obj['total_peers'],
                                    'from_peer': message_obj.get('from_peer', 'unknown')[:20]
                                }
                                yield f"data: {json.dumps(notification)}\n\n"
                                print(f"📩 Nova proposta recebida: {filename}")
                            
                            elif msg_type == 'peer_vote':
                                doc_id = message_obj['doc_id']
                                vote = message_obj['vote']
                                peer_id = message_obj['peer_id']
                                
                                if doc_id in voting_sessions:
                                    result = process_vote(doc_id, peer_id, vote)
                                    
                                    notification = {
                                        'type': 'peer_vote',
                                        'doc_id': doc_id,
                                        'vote': vote,
                                        'peer_id': peer_id[:20],
                                        'result': result
                                    }
                                    yield f"data: {json.dumps(notification)}\n\n"
                            
                            elif msg_type == 'document_approved':
                                doc_id = message_obj['doc_id']
                                cid = message_obj.get('cid')
                                
                                if 'embeddings' in message_obj and cid:
                                    embeddings_array = np.array(message_obj['embeddings'])
                                    np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings_array)
                                    
                                    vector = load_document_vector()
                                    vector["documents_confirmed"].append({
                                        "cid": cid,
                                        "filename": message_obj["filename"],
                                        "added_at": message_obj["timestamp"],
                                        "embedding_shape": message_obj.get("embedding_shape"),
                                        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy",
                                        "confirmed": True
                                    })
                                    vector["version_confirmed"] = message_obj.get("version", vector.get("version_confirmed", 0) + 1)
                                    save_document_vector(vector)
                                
                                if doc_id in voting_sessions:
                                    del voting_sessions[doc_id]
                                
                                notification = {
                                    'type': 'document_approved',
                                    'doc_id': doc_id,
                                    'filename': message_obj['filename'],
                                    'cid': cid,
                                    'votes_approve': message_obj.get('votes_approve'),
                                    'votes_reject': message_obj.get('votes_reject')
                                }
                                yield f"data: {json.dumps(notification)}\n\n"
                                print(f"✅ Documento aprovado: {message_obj['filename']}")
                            
                            elif msg_type == 'document_rejected':
                                doc_id = message_obj['doc_id']
                                
                                if doc_id in voting_sessions:
                                    del voting_sessions[doc_id]
                                
                                notification = {
                                    'type': 'document_rejected',
                                    'doc_id': doc_id,
                                    'filename': message_obj['filename'],
                                    'votes_approve': message_obj.get('votes_approve'),
                                    'votes_reject': message_obj.get('votes_reject')
                                }
                                yield f"data: {json.dumps(notification)}\n\n"
                                print(f"❌ Documento rejeitado: {message_obj['filename']}")
                        
                        except json.JSONDecodeError:
                            continue
                    
                    except Exception as e:
                        print(f"⚠️  Erro: {e}")
                        continue
        
        except Exception as e:
            print(f"❌ Erro no stream: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ============= INIT =============

if __name__ == "__main__":
    print("\n" + "="*60)
    print("IPFS Upload API")
    print("="*60)
    print(f"Servidor: http://0.0.0.0:5000")
    print(f"Documentação: http://localhost:5000/docs")
    print(f"Canal PubSub: {CANAL_PUBSUB}")
    print(f"Modelo: SentenceTransformer (all-MiniLM-L6-v2)")
    print(f"Dimensão embeddings: 384")
    print("="*60)
    
    my_id = get_my_peer_id()
    register_peer(my_id)
    
    print(f"Peer ID: {my_id}")
    print(f"Peers no sistema: {get_peers_count()}")
    
    # Enviar heartbeat inicial
    broadcast_heartbeat()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info"
    )
