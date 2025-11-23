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
import threading
import time
import subprocess

# ======================
# CONFIGURAÇÃO GLOBAL
# ======================

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
peers: Dict[str, datetime] = {}
PEER_TIMEOUT = timedelta(seconds=30)
LEADER_HEARTBEAT_INTERVAL = 5

# ======================
# PEER MANAGEMENT
# ======================

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
    inactive = [pid for pid, last_seen in peers.items() if now - last_seen > PEER_TIMEOUT]
    for pid in inactive:
        del peers[pid]
        print(f"⚠️  Peer removido por inatividade: {pid[:16]}...")

def get_peers_count():
    cleanup_inactive_peers()
    my_id = get_my_peer_id()
    if my_id not in peers:
        register_peer(my_id)
    return len(peers)

# ======================
# HEARTBEAT NORMAL
# ======================

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

# ======================
# HEARTBEAT DO LÍDER
# ======================

def broadcast_leader_heartbeat():
    try:
        vector = load_document_vector()
        pending_proposals = []

        for doc_id, session in voting_sessions.items():
            if session['status'] == 'pending_approval':
                pending_proposals.append({
                    'doc_id': doc_id,
                    'filename': session['filename'],
                    'votes_approve': len(session['votes_approve']),
                    'votes_reject': len(session['votes_reject']),
                    'required_votes': session['required_votes']
                })

        message_data = {
            "type": "leader_heartbeat",
            "leader_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat(),
            "pending_proposals": pending_proposals,
            "total_confirmed": len(vector.get('documents_confirmed', [])),
            "total_rejected": len(vector.get('documents_rejected', [])),
            "total_peers": get_peers_count(),
            "active_peers": list(peers.keys())
        }

        message_json = json.dumps(message_data)

        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        process.communicate(input=message_json.encode('utf-8'), timeout=5)

        print(f"💓 Heartbeat líder enviado | Pendentes: {len(pending_proposals)}")

    except Exception as e:
        print(f"⚠️ Erro ao enviar heartbeat líder: {e}")

def leader_heartbeat_loop():
    print("🔄 Leader heartbeat loop iniciado")
    while True:
        try:
            broadcast_leader_heartbeat()
            time.sleep(LEADER_HEARTBEAT_INTERVAL)
        except Exception as e:
            print(f"❌ Erro no heartbeat loop: {e}")
            time.sleep(LEADER_HEARTBEAT_INTERVAL)

# ======================
# DOCUMENT VECTOR
# ======================

def load_document_vector():
    if os.path.exists(VECTOR_FILE):
        with open(VECTOR_FILE, 'r') as f:
            return json.load(f)
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

# ======================
# FAISS INDEX
# ======================

def rebuild_faiss():
    try:
        import faiss
    except:
        print("⚠️ FAISS não instalado, ignorando reconstrução.")
        return

    vector = load_document_vector()
    documents = vector.get("documents_confirmed", [])
    embeddings_list = []

    for doc in documents:
        emb_path = doc.get("embedding_file")
        if emb_path and os.path.exists(emb_path):
            embeddings_list.append(np.load(emb_path).astype('float32'))

    if len(embeddings_list) == 0:
        print("ℹ️ Nenhum embedding disponível para reconstruir FAISS.")
        return

    matrix = np.vstack(embeddings_list).astype('float32')
    index = faiss.IndexFlatL2(matrix.shape[1])
    index.add(matrix)
    faiss.write_index(index, "faiss.index")

    print(f"🔄 FAISS reconstruído ({len(documents)} documentos)")

# ======================
# VECTOR COMMIT
# ======================

def broadcast_vector_commit():
    try:
        vector = load_document_vector()

        commit_payload = {
            "type": "vector_commit",
            "version": vector["version_confirmed"],
            "documents_confirmed": vector["documents_confirmed"],
        }

        commit_payload["hash"] = hashlib.sha256(
            json.dumps(commit_payload["documents_confirmed"], sort_keys=True).encode()
        ).hexdigest()

        message_json = json.dumps(commit_payload)

        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        process.communicate(input=message_json.encode('utf-8'), timeout=5)

        print(f"📢 COMMIT enviado (versão {commit_payload['version']})")

    except Exception as e:
        print(f"❌ Erro ao enviar commit: {e}")

def apply_vector_commit(data):
    try:
        version = data["version"]
        docs = data["documents_confirmed"]
        received_hash = data["hash"]

        computed = hashlib.sha256(
            json.dumps(docs, sort_keys=True).encode()
        ).hexdigest()

        if computed != received_hash:
            print("❌ Commit rejeitado: hash inválido!")
            return

        new_vector = load_document_vector()
        new_vector["documents_confirmed"] = docs
        new_vector["version_confirmed"] = version
        new_vector["last_updated"] = datetime.now().isoformat()

        save_document_vector(new_vector)
        print(f"📥 Commit aplicado (versão {version})")

        rebuild_faiss()

    except Exception as e:
        print(f"❌ Erro ao aplicar commit: {e}")

# ============================================================
#                   UPLOAD & VOTAÇÃO
# ============================================================

def create_voting_session(doc_id, filename, content):
    """
    Cria uma nova sessão de votação para um upload.
    """
    total_peers = get_peers_count()
    required_votes = (total_peers // 2) + 1  # maioria simples (50% + 1)

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
    """
    Processa voto remoto ou local.
    """
    if doc_id not in voting_sessions:
        return {"status": "error", "message": "Sessão não encontrada"}

    session = voting_sessions[doc_id]

    if session["status"] != "pending_approval":
        return {"status": "error", "message": "Votação já encerrada"}

    # remover votos anteriores do mesmo peer
    session["votes_approve"].discard(peer_id)
    session["votes_reject"].discard(peer_id)

    if vote_type == "approve":
        session["votes_approve"].add(peer_id)
    else:
        session["votes_reject"].add(peer_id)

    approve_count = len(session["votes_approve"])
    reject_count = len(session["votes_reject"])
    required = session["required_votes"]

    # MAIORIA DE APROVAÇÃO
    if approve_count >= required:
        session["status"] = "approved"
        session["final_decision"] = "approved"
        session["decided_at"] = datetime.now().isoformat()

        return finalize_approved_document(doc_id)

    # MAIORIA DE REJEIÇÃO
    elif reject_count >= required:
        session["status"] = "rejected"
        session["final_decision"] = "rejected"
        session["decided_at"] = datetime.now().isoformat()

        finalize_rejected_document(doc_id)
        propagate_decision(doc_id, "rejected")

        return {
            "status": "rejected",
            "message": "Documento rejeitado",
            "votes_approve": approve_count,
            "votes_reject": reject_count,
            "required_votes": required
        }

    return {
        "status": "voting",
        "message": "A aguardar mais votos",
        "votes_approve": approve_count,
        "votes_reject": reject_count,
        "required_votes": required,
        "votes_remaining": required - max(approve_count, reject_count)
    }


# ============================================================
#                   EXTRAÇÃO & EMBEDDINGS
# ============================================================

def extract_text_from_file(content, filename):
    try:
        return content.decode('utf-8')
    except:
        return f"Document: {filename}"


def generate_embeddings(text_content):
    try:
        return embedding_model.encode(text_content, convert_to_numpy=True)
    except Exception as e:
        print("Erro ao gerar embedding:", e)
        raise


# ============================================================
#                   ADICIONAR AO VETOR
# ============================================================

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


# ============================================================
#                   PROPAGAÇÃO PUBSUB
# ============================================================

def propagate_proposal(doc_id, filename, cid=None):
    try:
        session = voting_sessions[doc_id]

        vector = load_document_vector()
        total_confirmed = len(vector.get("documents_confirmed", []))

        message_data = {
            "type": "document_proposal",
            "doc_id": doc_id,
            "filename": filename,
            "total_peers": session["total_peers"],
            "required_votes": session["required_votes"],
            "total_confirmed": total_confirmed,
            "timestamp": datetime.now().isoformat(),
            "from_peer": get_my_peer_id()
        }

        if cid:
            message_data["cid"] = cid

        msg = json.dumps(message_data)

        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=msg.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )

        if response.status_code == 200:
            print(f"📢 Proposta enviada: {filename} ({doc_id})")
            return True

        return False

    except Exception as e:
        print("Erro ao propagar proposta:", e)
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

        msg = json.dumps(message_data)

        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=msg.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )

        return response.status_code == 200

    except Exception as e:
        print("Erro ao propagar voto:", e)
        return False


def propagate_decision(doc_id, decision, cid=None, embeddings=None, version=None):
    try:
        session = voting_sessions.get(doc_id)
        if not session:
            return False

        vector = load_document_vector()
        total_confirmed = len(vector.get("documents_confirmed", []))

        message_data = {
            "type": f"document_{decision}",
            "doc_id": doc_id,
            "filename": session["filename"],
            "decision": decision,
            "votes_approve": len(session["votes_approve"]),
            "votes_reject": len(session["votes_reject"]),
            "total_confirmed": total_confirmed,
            "timestamp": datetime.now().isoformat(),
            "from_peer": get_my_peer_id()
        }

        if decision == "approved" and cid:
            message_data["cid"] = cid
            message_data["version"] = version
            if embeddings is not None:
                message_data["embeddings"] = embeddings.tolist()
                message_data["embedding_shape"] = list(embeddings.shape)

        msg = json.dumps(message_data)

        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=msg.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )

        if response.status_code == 200:
            print(f"📡 Decisão propagada: {decision.upper()} - {session['filename']}")
            return True

        return False

    except Exception as e:
        print("Erro ao propagar decisão:", e)
        return False

# ============================================================
#          FINALIZAÇÃO DE DOCUMENTOS (APPROVE / REJECT)
# ============================================================

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

        # ==== EMBEDDINGS =====
        text_content = extract_text_from_file(content, filename)
        embeddings = generate_embeddings(text_content)

        new_version, updated_vector = add_document_to_vector(
            cid,
            filename,
            embeddings,
            confirmed=True
        )

        np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings)
        os.remove(temp_file)

        # ==== PROPAGA DECISÃO ====
        propagate_decision(
            doc_id, "approved",
            cid=cid,
            embeddings=embeddings,
            version=new_version
        )

        # ==== COMMIT PARA TODOS ====
        print("📡 A enviar commit final para todos os peers...")
        broadcast_vector_commit()

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


# ============================================================
#                    ENDPOINT: UPLOAD
# ============================================================

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

        print(f"📋 Sessão criada:")
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
        print("Erro:", e)
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )


# ============================================================
#            ENDPOINT: ESTADO DE TODAS AS VOTAÇÕES
# ============================================================

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


# ============================================================
#      ENDPOINT: ESTADO DE UMA ÚNICA VOTAÇÃO ESPECÍFICA
# ============================================================

@app.get("/voting-status/{doc_id}")
def get_voting_status(doc_id: str):
    if doc_id in voting_sessions:
        s = voting_sessions[doc_id]
        return {
            "doc_id": doc_id,
            "filename": s["filename"],
            "status": s["status"],
            "votes_approve": len(s["votes_approve"]),
            "votes_reject": len(s["votes_reject"]),
            "required_votes": s["required_votes"],
            "total_peers": s["total_peers"],
            "votes_remaining": s["required_votes"] - max(len(s["votes_approve"]), len(s["votes_reject"])),
            "created_at": s["created_at"],
            "decided_at": s.get("decided_at"),
            "final_decision": s.get("final_decision"),
            "peers_voted_approve": list(s["votes_approve"]),
            "peers_voted_reject": list(s["votes_reject"])
        }

    # Caso a sessão não exista localmente — tenta reconstruir
    try:
        matches = [fn for fn in os.listdir(PENDING_UPLOADS_DIR) if fn.startswith(f"{doc_id}_")]
        if matches:
            fname = matches[0]
            temp_path = os.path.join(PENDING_UPLOADS_DIR, fname)
            orig_filename = "_".join(fname.split("_")[1:])
            with open(temp_path, 'rb') as rf:
                content = rf.read()

            total_peers = get_peers_count()
            required_votes = (total_peers // 2) + 1

            voting_sessions[doc_id] = {
                "doc_id": doc_id,
                "filename": orig_filename,
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

            s = voting_sessions[doc_id]
            print(f"🔁 Sessão reconstruída a partir de ficheiro pendente: {temp_path}")

            return {
                "doc_id": doc_id,
                "filename": s["filename"],
                "status": s["status"],
                "votes_approve": len(s["votes_approve"]),
                "votes_reject": len(s["votes_reject"]),
                "required_votes": s["required_votes"],
                "total_peers": s["total_peers"],
                "votes_remaining": s["required_votes"] - max(len(s["votes_approve"]), len(s["votes_reject"])),
                "created_at": s["created_at"],
                "decided_at": s.get("decided_at"),
                "final_decision": s.get("final_decision"),
                "peers_voted_approve": list(s["votes_approve"]),
                "peers_voted_reject": list(s["votes_reject"])
            }

    except Exception as e:
        print("Erro ao reconstruir sessão:", e)

    return JSONResponse(
        content={"error": "Sessão não encontrada"},
        status_code=404
    )

# ============================================================
#                   ENDPOINT: /vector
# ============================================================

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


# ============================================================
#                 ENDPOINT: /info/{cid}
# ============================================================

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
            'api_download': f'/download/{cid}',
            'api_embedding': f'/vector/{cid}/embedding' if has_embedding else None
        }

    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )


# ============================================================
#               ENDPOINT: /download/{cid}
# ============================================================

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

# ============================================================
#           ENDPOINT: /vector/{cid}/embedding
# ============================================================

@app.get("/vector/{cid}/embedding")
def get_embedding(cid: str):
    try:
        embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"

        if not os.path.exists(embedding_path):
            return JSONResponse(
                content={'error': 'Embedding não encontrado'},
                status_code=404
            )

        embedding = np.load(embedding_path)

        vector = load_document_vector()
        doc_info = None

        for doc in vector.get('documents_confirmed', []):
            if doc.get('cid') == cid:
                doc_info = doc
                break

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

# ============================================================
#                   ENDPOINT: /documents
# ============================================================

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
    for doc_id, s in voting_sessions.items():
        pending_approval.append({
            'doc_id': doc_id,
            'filename': s['filename'],
            'status': s['status'],
            'votes_approve': len(s['votes_approve']),
            'votes_reject': len(s['votes_reject']),
            'required_votes': s['required_votes'],
            'created_at': s['created_at']
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


# ============================================================
#                   ENDPOINT: /search/{query}
# ============================================================

@app.get("/search/{query}")
def search_documents(query: str):
    vector = load_document_vector()
    results = []

    q = query.lower()

    for doc in vector.get('documents_confirmed', []):
        filename = doc.get('filename', '').lower()
        if q in filename:
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


# ============================================================
#                 ENDPOINT: DELETE DOCUMENT
# ============================================================

@app.delete("/document/{cid}")
def delete_document(cid: str):
    try:
        vector = load_document_vector()

        before = len(vector.get('documents_confirmed', []))
        vector['documents_confirmed'] = [
            d for d in vector.get('documents_confirmed', [])
            if d.get('cid') != cid
        ]

        if len(vector['documents_confirmed']) < before:
            save_document_vector(vector)

            emb_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
            if os.path.exists(emb_path):
                os.remove(emb_path)

            return {
                'status': 'success',
                'message': 'Documento removido do vetor',
                'cid': cid,
                'note': 'O ficheiro ainda existe no IPFS'
            }

        return JSONResponse(
            content={'error': 'CID não encontrado'},
            status_code=404
        )

    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )


# ============================================================
#                       ENDPOINT: /status
# ============================================================

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

# ============================================================
#                     ENDPOINT: /peers
# ============================================================

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

# ============================================================
#            APPLY VECTOR COMMIT (RECEBIDO DO LÍDER)
# ============================================================

def apply_vector_commit(data):
    """
    Processa commit recebido do líder:
    - Valida hash
    - Substitui vetor local
    - Reconstrói index FAISS (se existisse)
    """
    try:
        version = data["version"]
        docs = data["documents_confirmed"]
        received_hash = data["hash"]

        computed = hashlib.sha256(
            json.dumps(docs, sort_keys=True).encode()
        ).hexdigest()

        if computed != received_hash:
            print("❌ Commit rejeitado — hash inválido!")
            return

        vector = load_document_vector()
        vector["documents_confirmed"] = docs
        vector["version_confirmed"] = version
        vector["last_updated"] = datetime.now().isoformat()
        save_document_vector(vector)

        print(f"📥 Commit aplicado (versão {version})")

        rebuild_faiss()

    except Exception as e:
        print(f"Erro ao aplicar commit: {e}")


def rebuild_faiss():
    """
    Placeholder — se quiseres FAISS real, adiciono.
    """
    print("🔧 (FAISS) Reconstrução ignorada (placeholder)")


# ============================================================
#               SSE STREAM — /notifications
# ============================================================

@app.get("/notifications")
async def get_notifications():
    async def event_stream():
        try:
            print("📡 Cliente ligado ao SSE")

            # Regista peer e envia heartbeat
            my_id = get_my_peer_id()
            register_peer(my_id)
            broadcast_heartbeat()

            # Subscreve ao canal IPFS
            response = requests.post(
                f"{IPFS_API_URL}/pubsub/sub",
                params={'arg': CANAL_PUBSUB},
                stream=True,
                timeout=None
            )

            # Envia evento inicial
            yield f"data: {json.dumps({'type': 'connected', 'peer_id': my_id, 'canal': CANAL_PUBSUB})}\n\n"

            for line in response.iter_lines():
                if not line:
                    continue

                try:
                    msg = json.loads(line)
                except:
                    continue

                data_encoded = msg.get("data")

                # Base64 decode
                try:
                    data_decoded = base64.b64decode(data_encoded).decode()
                except:
                    continue

                try:
                    obj = json.loads(data_decoded)
                except:
                    continue

                msg_type = obj.get("type")

                # Peer heartbeat (não envia ao cliente)
                if msg_type == "peer_heartbeat":
                    pid = obj.get("peer_id")
                    if pid:
                        register_peer(pid)
                    continue

                # NOVA PROPOSTA
                if msg_type == "document_proposal":
                    notif = {
                        "type": "document_proposal",
                        "doc_id": obj["doc_id"],
                        "filename": obj["filename"],
                        "required_votes": obj["required_votes"],
                        "total_peers": obj["total_peers"],
                        "from_peer": obj.get("from_peer")
                    }
                    yield f"data: {json.dumps(notif)}\n\n"
                    continue

                # VOTO
                if msg_type == "peer_vote":
                    notif = {
                        "type": "peer_vote",
                        "doc_id": obj["doc_id"],
                        "vote": obj["vote"],
                        "peer_id": obj["peer_id"]
                    }
                    yield f"data: {json.dumps(notif)}\n\n"
                    continue

                # APROVADO
                if msg_type == "document_approved":
                    notif = {
                        "type": "document_approved",
                        "doc_id": obj["doc_id"],
                        "filename": obj["filename"],
                        "cid": obj.get("cid")
                    }
                    yield f"data: {json.dumps(notif)}\n\n"
                    continue

                # REJEITADO
                if msg_type == "document_rejected":
                    notif = {
                        "type": "document_rejected",
                        "doc_id": obj["doc_id"],
                        "filename": obj["filename"]
                    }
                    yield f"data: {json.dumps(notif)}\n\n"
                    continue

                # VECTOR COMMIT
                if msg_type == "vector_commit":
                    apply_vector_commit(obj)

                    notif = {
                        "type": "vector_commit",
                        "version": obj.get("version")
                    }
                    yield f"data: {json.dumps(notif)}\n\n"
                    continue

        except Exception as e:
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


# ============================================================
#       BACKGROUND LISTENER PARA PROPOSTAS & SINCRONIZAÇÃO
# ============================================================

def pubsub_bg_listener():
    try:
        print("📡 [BG] A subscrever canal PubSub...")

        resp = requests.post(
            f"{IPFS_API_URL}/pubsub/sub",
            params={'arg': CANAL_PUBSUB},
            stream=True,
            timeout=None
        )

        print("✅ [BG] Subscrito!")

        for line in resp.iter_lines():

            if not line:
                continue

            try:
                raw = line.decode(errors="ignore")
                msg = json.loads(raw)
            except:
                continue

            data_encoded = msg.get("data")
            if not data_encoded:
                continue

            # Decode base64
            try:
                decoded = base64.b64decode(data_encoded).decode()
            except:
                continue

            try:
                obj = json.loads(decoded)
            except:
                continue

            msg_type = obj.get("type")

            # HEARTBEAT
            if msg_type == "peer_heartbeat":
                pid = obj.get("peer_id")
                if pid:
                    register_peer(pid)
                continue

            # PROPOSTA
            if msg_type == "document_proposal":
                doc_id = obj.get("doc_id")
                filename = obj.get("filename")
                sender = obj.get("from_peer")

                # evitar loop
                if sender == get_my_peer_id():
                    continue

                if doc_id not in voting_sessions:
                    print(f"📥 [BG] Nova proposta recebida: {filename}")

                    content = b""
                    cid = obj.get("cid")
                    if cid:
                        try:
                            r = requests.post(f"{IPFS_API_URL}/cat", params={"arg": cid}, timeout=20)
                            if r.status_code == 200:
                                content = r.content
                        except:
                            pass

                    create_voting_session(doc_id, filename, content)
                continue

            # VOTO
            if msg_type == "peer_vote":
                process_vote(obj["doc_id"], obj["peer_id"], obj["vote"])
                continue

            # APROVADO / REJEITADO
            if msg_type in ("document_approved", "document_rejected"):
                doc_id = obj["doc_id"]

                if doc_id in voting_sessions:
                    del voting_sessions[doc_id]

                if msg_type == "document_approved" and "embeddings" in obj:
                    cid = obj["cid"]
                    arr = np.array(obj["embeddings"])
                    np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", arr)

                    vector = load_document_vector()
                    vector["documents_confirmed"].append({
                        "cid": cid,
                        "filename": obj["filename"],
                        "added_at": obj["timestamp"],
                        "embedding_shape": obj["embedding_shape"],
                        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy",
                        "confirmed": True
                    })
                    vector["version_confirmed"] = obj.get("version", vector["version_confirmed"] + 1)
                    save_document_vector(vector)

                continue

            # COMMIT
            if msg_type == "vector_commit":
                apply_vector_commit(obj)
                continue

    except Exception as e:
        print(f"❌ [BG] Listener falhou: {e}")

# ============================================================
#                        HEARTBEAT NORMAL
# ============================================================

def broadcast_heartbeat():
    """
    Heartbeat simples (todos os peers enviam).
    """
    try:
        data = {
            "type": "peer_heartbeat",
            "peer_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat()
        }

        msg = json.dumps(data)

        requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=msg.encode(),
            timeout=5
        )

    except Exception:
        pass


# ============================================================
#                      HEARTBEAT DO LÍDER
# ============================================================

def broadcast_leader_heartbeat():
    """
    Envia estado agregado do sistema.
    """
    try:
        vector = load_document_vector()

        pending = []
        for doc_id, sess in voting_sessions.items():
            if sess["status"] == "pending_approval":
                pending.append({
                    "doc_id": doc_id,
                    "filename": sess["filename"],
                    "votes_approve": len(sess["votes_approve"]),
                    "votes_reject": len(sess["votes_reject"]),
                    "required_votes": sess["required_votes"]
                })

        payload = {
            "type": "leader_heartbeat",
            "leader_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat(),
            "pending_proposals": pending,
            "total_confirmed": len(vector.get("documents_confirmed", [])),
            "total_rejected": len(vector.get("documents_rejected", [])),
            "total_peers": get_peers_count(),
            "active_peers": list(peers.keys())
        }

        msg = json.dumps(payload)

        # CLI (mais confiável que HTTP API)
        process = subprocess.Popen(
            ["ipfs", "pubsub", "pub", CANAL_PUBSUB],
            stdin=subprocess.PIPE
        )
        process.communicate(input=msg.encode(), timeout=5)

        print(f"💓 Leader heartbeat enviado | Pendentes: {len(pending)}")

    except Exception as e:
        print("Erro no leader heartbeat:", e)


# ============================================================
#                     LEADER HEARTBEAT LOOP
# ============================================================

def leader_heartbeat_loop():
    """
    O líder envia heartbeats a cada 5 segundos.
    """
    print("🔄 Leader heartbeat loop iniciado")

    while True:
        try:
            broadcast_leader_heartbeat()
        except Exception as e:
            print("Erro no loop de heartbeat:", e)

        time.sleep(5)


# ============================================================
#                          MAIN
# ============================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("IPFS Upload API — Sistema de Votação Distribuído")
    print("=" * 60)
    print("Servidor iniciado em http://0.0.0.0:5000")
    print("Swagger:        http://localhost:5000/docs")
    print("PubSub Canal:   ", CANAL_PUBSUB)
    print("Modelo:         SentenceTransformer")
    print("Dimensão:       384")
    print("=" * 60)

    # ============================================================
    #           REGISTO DO PEER LOCAL
    # ============================================================

    my_id = get_my_peer_id()
    register_peer(my_id)
    print(f"Peer ID: {my_id}")
    print(f"Peers ativos: {get_peers_count()}")

    # ============================================================
    #     INICIA O LISTENER EM BACKGROUND (PubSub)
    # ============================================================

    try:
        t = threading.Thread(target=pubsub_bg_listener, daemon=True)
        t.start()
        print("🔁 Listener PubSub em background iniciado.")
    except Exception as e:
        print("Erro ao iniciar listener PubSub:", e)

    # ============================================================
    #            HEARTBEAT DO LÍDER (THREAD)
    # ============================================================

    try:
        lt = threading.Thread(target=leader_heartbeat_loop, daemon=True)
        lt.start()
        print("💓 Leader Heartbeat thread ativa.")
    except Exception as e:
        print("Erro ao iniciar leader heartbeat:", e)

    # Heartbeat inicial
    broadcast_heartbeat()

    # ============================================================
    #                  ARRANQUE DO SERVIDOR
    # ============================================================

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info",
        timeout_keep_alive=120
    )
