from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from sentence_transformers import SentenceTransformer
import requests
import uvicorn
import json
import numpy as np
import faiss
import os
from pathlib import Path
from datetime import datetime, timedelta
import hashlib
import uuid
import threading
import time
import subprocess
from typing import Dict, List
from enum import Enum

app = FastAPI(title="IPFS Distributed System")

# Configuração
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
FAISS_INDEX_FILE = "faiss_index.bin"

# Timeouts
HEARTBEAT_INTERVAL = 5
LEADER_TIMEOUT = 10
PEER_TIMEOUT = timedelta(seconds=30)

Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(TEMP_EMBEDDINGS_DIR).mkdir(exist_ok=True)

print("🔄 A carregar modelo SentenceTransformer...")
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("✅ Modelo carregado!")

# FAISS Index
faiss_index = faiss.IndexFlatL2(384)
faiss_cid_map = []

# Estados RAFT
class NodeState(Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

# Estado global
current_state = NodeState.FOLLOWER
current_term = 0
voted_for = None
leader_id = None
my_peer_id = None
last_heartbeat_received = None

# Sistema de votação e confirmação
voting_sessions: Dict[str, dict] = {}
pending_confirmations: Dict[int, dict] = {}
system_peers: Dict[str, datetime] = {}

# Threads
running = False


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
    system_peers[peer_id] = datetime.now()
    cleanup_inactive_peers()


def cleanup_inactive_peers():
    now = datetime.now()
    inactive = [pid for pid, last_seen in system_peers.items() 
                if now - last_seen > PEER_TIMEOUT]
    for pid in inactive:
        del system_peers[pid]


def get_system_peers_count():
    cleanup_inactive_peers()
    my_id = get_my_peer_id()
    if my_id not in system_peers:
        register_peer(my_id)
    return len(system_peers)


def calculate_vector_hash(vector_data: dict) -> str:
    cids = [doc['cid'] for doc in vector_data.get('documents_confirmed', [])]
    vector_str = json.dumps(cids, sort_keys=True)
    return hashlib.sha256(vector_str.encode()).hexdigest()


def publish_to_pubsub(message_data: dict) -> bool:
    try:
        message_json = json.dumps(message_data)
        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(input=message_json.encode('utf-8'), timeout=5)
        return process.returncode == 0
    except Exception as e:
        print(f"❌ Erro ao publicar: {e}")
        return False


# ============= FAISS =============

def load_faiss_index():
    global faiss_index, faiss_cid_map
    
    if os.path.exists(FAISS_INDEX_FILE):
        try:
            faiss_index = faiss.read_index(FAISS_INDEX_FILE)
            vector = load_document_vector()
            faiss_cid_map = [doc['cid'] for doc in vector.get('documents_confirmed', [])]
            print(f"✅ FAISS carregado: {faiss_index.ntotal} vetores")
        except Exception as e:
            print(f"⚠️  Ficheiro FAISS corrompido, a criar novo: {e}")
            try:
                os.remove(FAISS_INDEX_FILE)
            except:
                pass
            faiss_index = faiss.IndexFlatL2(384)
            faiss_cid_map = []
            print("✅ Novo índice FAISS criado")
    else:
        print("✅ Novo índice FAISS criado")


def save_faiss_index():
    try:
        faiss.write_index(faiss_index, FAISS_INDEX_FILE)
        print(f"✅ FAISS guardado: {faiss_index.ntotal} vetores")
    except Exception as e:
        print(f"❌ Erro ao guardar FAISS: {e}")


def update_faiss_index_after_commit(cid: str):
    global faiss_index, faiss_cid_map
    
    embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
    
    if not os.path.exists(embedding_path):
        print(f"⚠️  Embedding não encontrado: {cid}")
        return False
    
    try:
        embedding = np.load(embedding_path)
        faiss_index.add(embedding.reshape(1, -1))
        faiss_cid_map.append(cid)
        save_faiss_index()
        print(f"✅ FAISS atualizado: {cid[:16]}... (total: {faiss_index.ntotal})")
        return True
    except Exception as e:
        print(f"❌ Erro ao atualizar FAISS: {e}")
        return False


# ============= HEARTBEAT =============

def send_leader_heartbeat():
    if current_state != NodeState.LEADER:
        return
    
    vector = load_document_vector()
    pending_proposals = [
        {"doc_id": doc_id, "filename": session['filename'], "status": session['status']}
        for doc_id, session in voting_sessions.items()
        if session['status'] == 'pending_approval'
    ]
    
    message_data = {
        "type": "leader_heartbeat",
        "leader_id": get_my_peer_id(),
        "term": current_term,
        "timestamp": datetime.now().isoformat(),
        "version_confirmed": vector.get('version_confirmed', 0),
        "total_confirmed": len(vector.get('documents_confirmed', [])),
        "pending_proposals": pending_proposals
    }
    
    publish_to_pubsub(message_data)


def heartbeat_loop():
    while running:
        if current_state == NodeState.LEADER:
            send_leader_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)


# ============= ELEIÇÃO =============

def start_election():
    global current_state, current_term, voted_for
    
    current_state = NodeState.CANDIDATE
    current_term += 1
    voted_for = get_my_peer_id()
    
    print(f"\n🗳️  ELEIÇÃO: Term {current_term}")
    
    message_data = {
        "type": "election_request_vote",
        "candidate_id": get_my_peer_id(),
        "term": current_term,
        "timestamp": datetime.now().isoformat()
    }
    
    publish_to_pubsub(message_data)
    time.sleep(5)
    become_leader()


def become_leader():
    global current_state, leader_id
    current_state = NodeState.LEADER
    leader_id = get_my_peer_id()
    print(f"\n👑 PROMOVIDO A LÍDER")
    send_leader_heartbeat()


def become_follower(new_term: int, new_leader: str):
    global current_state, current_term, voted_for, leader_id
    current_state = NodeState.FOLLOWER
    current_term = new_term
    voted_for = None
    leader_id = new_leader


def check_leader_alive():
    if current_state == NodeState.LEADER or last_heartbeat_received is None:
        return True
    time_since_hb = (datetime.now() - last_heartbeat_received).total_seconds()
    return time_since_hb <= LEADER_TIMEOUT


def election_timer_loop():
    import random
    while running:
        timeout = random.uniform(15, 30)
        time.sleep(timeout)
        if current_state == NodeState.FOLLOWER and not check_leader_alive():
            start_election()


# ============= VOTAÇÃO =============

def create_voting_session(doc_id, filename, content):
    total_peers = get_system_peers_count()
    required_votes = (total_peers // 2) + 1
    
    voting_sessions[doc_id] = {
        "doc_id": doc_id,
        "filename": filename,
        "content": content,
        "status": "pending_approval",
        "total_peers": total_peers,
        "required_votes": required_votes,
        "votes_approve": set(),
        "votes_reject": set(),
        "created_at": datetime.now().isoformat()
    }
    
    temp_file = f"{TEMP_EMBEDDINGS_DIR}/upload_{doc_id}_{filename}"
    with open(temp_file, 'wb') as f:
        f.write(content)
    
    return voting_sessions[doc_id]


def process_vote(doc_id, peer_id, vote_type):
    if doc_id not in voting_sessions:
        return {"status": "error", "message": "Sessão não encontrada"}
    
    session = voting_sessions[doc_id]
    
    if session["status"] != "pending_approval":
        return {"status": "error", "message": "Votação encerrada"}
    
    session["votes_approve"].discard(peer_id)
    session["votes_reject"].discard(peer_id)
    
    if vote_type == "approve":
        session["votes_approve"].add(peer_id)
    elif vote_type == "reject":
        session["votes_reject"].add(peer_id)
    
    approve_count = len(session["votes_approve"])
    reject_count = len(session["votes_reject"])
    required = session["required_votes"]
    
    if approve_count >= required:
        session["status"] = "approved"
        result = finalize_approved_document(doc_id)
        return result
    
    elif reject_count >= required:
        session["status"] = "rejected"
        finalize_rejected_document(doc_id)
        
        message_data = {
            "type": "document_rejected",
            "doc_id": doc_id,
            "filename": session["filename"],
            "votes_approve": approve_count,
            "votes_reject": reject_count,
            "timestamp": datetime.now().isoformat()
        }
        publish_to_pubsub(message_data)
        
        return {
            "status": "rejected",
            "message": "Documento rejeitado",
            "votes_approve": approve_count,
            "votes_reject": reject_count
        }
    
    return {
        "status": "voting",
        "votes_approve": approve_count,
        "votes_reject": reject_count,
        "required_votes": required
    }


def finalize_approved_document(doc_id):
    session = voting_sessions[doc_id]
    filename = session["filename"]
    
    temp_file = f"{TEMP_EMBEDDINGS_DIR}/upload_{doc_id}_{filename}"
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
        
        cid = response.json()['Hash']
        
        text_content = extract_text_from_file(content, filename)
        embeddings = generate_embeddings(text_content)
        
        os.remove(temp_file)
        
        vector = load_document_vector()
        new_version = vector.get("version_confirmed", 0) + 1
        
        request_version_confirmation(new_version, cid, filename, embeddings)
        
        return {
            "status": "approved",
            "message": "Aprovado - Aguardando confirmação",
            "cid": cid,
            "filename": filename,
            "version": new_version
        }
    
    except Exception as e:
        return {"status": "error", "message": str(e)}


def finalize_rejected_document(doc_id):
    session = voting_sessions[doc_id]
    filename = session["filename"]
    temp_file = f"{TEMP_EMBEDDINGS_DIR}/upload_{doc_id}_{filename}"
    
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    vector = load_document_vector()
    vector.setdefault("documents_rejected", [])
    vector["documents_rejected"].append({
        "doc_id": doc_id,
        "filename": filename,
        "rejected_at": datetime.now().isoformat()
    })
    save_document_vector(vector)


# ============= CONFIRMAÇÃO =============

def request_version_confirmation(version: int, cid: str, filename: str, embeddings: np.ndarray):
    np.save(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", embeddings)
    
    vector = load_document_vector()
    vector["version_pending"] = version
    vector.setdefault("documents_temp", [])
    vector["documents_temp"].append({
        "cid": cid,
        "filename": filename,
        "added_at": datetime.now().isoformat(),
        "embedding_shape": list(embeddings.shape),
        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy"
    })
    save_document_vector(vector)
    
    vector_hash = calculate_vector_hash(vector)
    
    message_data = {
        "type": "version_update_request",
        "leader_id": get_my_peer_id(),
        "term": current_term,
        "version": version,
        "cid": cid,
        "filename": filename,
        "vector_hash": vector_hash,
        "embeddings": embeddings.tolist(),
        "embedding_shape": list(embeddings.shape),
        "timestamp": datetime.now().isoformat()
    }
    
    pending_confirmations[version] = {
        "cid": cid,
        "vector_hash": vector_hash,
        "confirmations": {},
        "required": (get_system_peers_count() // 2) + 1,
        "created_at": datetime.now()
    }
    
    publish_to_pubsub(message_data)
    print(f"📋 Confirmação solicitada: v{version}, CID: {cid[:16]}...")


def process_peer_confirmation(peer_id: str, version: int, peer_hash: str):
    if version not in pending_confirmations:
        return
    
    session = pending_confirmations[version]
    expected_hash = session["vector_hash"]
    
    if peer_hash == expected_hash:
        session["confirmations"][peer_id] = peer_hash
        confirm_count = len(session["confirmations"])
        required = session["required"]
        
        print(f"✅ Confirmação {confirm_count}/{required} de {peer_id[:16]}...")
        
        if confirm_count >= required:
            send_version_commit(version)
    else:
        print(f"❌ Hash incorreta de {peer_id[:16]}...")


# ============= COMMIT =============

def send_version_commit(version: int):
    if version not in pending_confirmations:
        return
    
    session = pending_confirmations[version]
    cid = session["cid"]
    
    print(f"\n🎯 COMMIT: Versão {version}")
    
    message_data = {
        "type": "version_commit",
        "leader_id": get_my_peer_id(),
        "term": current_term,
        "version": version,
        "cid": cid,
        "timestamp": datetime.now().isoformat()
    }
    
    success = publish_to_pubsub(message_data)
    
    if success:
        temp_path = f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy"
        final_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
        if os.path.exists(temp_path):
            os.rename(temp_path, final_path)
        
        vector = load_document_vector()
        if vector.get("documents_temp"):
            for temp_doc in vector["documents_temp"]:
                if temp_doc["cid"] == cid:
                    temp_doc["confirmed"] = True
                    vector["documents_confirmed"].append(temp_doc)
            
            vector["version_confirmed"] = version
            vector["documents_temp"] = []
            vector["version_pending"] = 0
            save_document_vector(vector)
        
        update_faiss_index_after_commit(cid)
        del pending_confirmations[version]


# ============= PERSISTÊNCIA =============

def load_document_vector():
    """Carrega vetor de documentos com proteção contra corrupção"""
    if os.path.exists(VECTOR_FILE):
        try:
            with open(VECTOR_FILE, 'r') as f:
                content = f.read().strip()
                
                if not content:
                    return {
                        "version_confirmed": 0,
                        "version_pending": 0,
                        "documents_confirmed": [],
                        "documents_temp": [],
                        "documents_rejected": [],
                        "last_updated": None
                    }
                
                return json.loads(content)
        
        except json.JSONDecodeError as e:
            print(f"⚠️  JSON corrompido: {e}")
            print("⚠️  A criar novo ficheiro...")
            
            # Renomeia ficheiro corrompido para backup
            backup_file = f"document_vector_backup_{int(time.time())}.json"
            try:
                os.rename(VECTOR_FILE, backup_file)
                print(f"✅ Backup criado: {backup_file}")
            except:
                try:
                    os.remove(VECTOR_FILE)
                except:
                    pass
            
            return {
                "version_confirmed": 0,
                "version_pending": 0,
                "documents_confirmed": [],
                "documents_temp": [],
                "documents_rejected": [],
                "last_updated": None
            }
        
        except Exception as e:
            print(f"⚠️  Erro ao ler JSON: {e}")
            return {
                "version_confirmed": 0,
                "version_pending": 0,
                "documents_confirmed": [],
                "documents_temp": [],
                "documents_rejected": [],
                "last_updated": None
            }
    
    # Ficheiro não existe
    return {
        "version_confirmed": 0,
        "version_pending": 0,
        "documents_confirmed": [],
        "documents_temp": [],
        "documents_rejected": [],
        "last_updated": None
    }


def save_document_vector(vector_data):
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)


def generate_embeddings(text_content):
    return embedding_model.encode(text_content, convert_to_numpy=True)


def extract_text_from_file(content, filename):
    try:
        return content.decode('utf-8')
    except UnicodeDecodeError:
        return f"Document: {filename}"


# ============= ENDPOINTS =============

@app.get("/")
def root():
    vector = load_document_vector()
    return {
        "message": "Sistema Distribuído Completo",
        "version": "5.0",
        "node_state": current_state.value,
        "is_leader": current_state == NodeState.LEADER,
        "leader_id": leader_id[:20] if leader_id else None,
        "version_confirmed": vector.get("version_confirmed", 0),
        "faiss_vectors": faiss_index.ntotal,
        "system_peers": get_system_peers_count()
    }


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if current_state != NodeState.LEADER:
        return JSONResponse(
            content={
                "error": "Não sou líder",
                "leader_id": leader_id,
                "current_state": current_state.value,
                "my_peer_id": get_my_peer_id()
            },
            status_code=403
        )
    
    try:
        filename = file.filename
        content = await file.read()
        
        print(f"\n{'='*60}")
        print(f"📤 UPLOAD: {filename}")
        print(f"{'='*60}")
        
        doc_id = str(uuid.uuid4())
        session = create_voting_session(doc_id, filename, content)
        
        message_data = {
            "type": "document_proposal",
            "doc_id": doc_id,
            "filename": filename,
            "total_peers": session["total_peers"],
            "required_votes": session["required_votes"],
            "timestamp": datetime.now().isoformat(),
            "from_peer": get_my_peer_id()
        }
        
        publish_to_pubsub(message_data)
        
        print(f"✅ Proposta enviada")
        print(f"{'='*60}\n")
        
        return {
            'status': 'pending_approval',
            'doc_id': doc_id,
            'filename': filename,
            'required_votes': session['required_votes']
        }
    
    except Exception as e:
        return JSONResponse(content={'error': str(e)}, status_code=500)


@app.get("/status")
def system_status():
    vector = load_document_vector()
    return {
        'node_state': current_state.value,
        'is_leader': current_state == NodeState.LEADER,
        'leader_id': leader_id,
        'peer_id': get_my_peer_id(),
        'system_peers': get_system_peers_count(),
        'version_confirmed': vector.get('version_confirmed', 0),
        'faiss_vectors': faiss_index.ntotal,
        'total_confirmed': len(vector.get('documents_confirmed', []))
    }


@app.post("/force-leader")
def force_leader():
    global current_state, leader_id, current_term
    
    current_state = NodeState.LEADER
    leader_id = get_my_peer_id()
    current_term += 1
    
    send_leader_heartbeat()
    
    print(f"\n👑 FORÇADO A SER LÍDER (Term {current_term})")
    
    return {
        "status": "ok",
        "message": "Promovido a líder",
        "node_state": current_state.value,
        "leader_id": leader_id,
        "term": current_term
    }


@app.get("/faiss/search")
def faiss_search(query: str, k: int = 5):
    try:
        query_embedding = generate_embeddings(query)
        distances, indices = faiss_index.search(query_embedding.reshape(1, -1), k)
        
        results = []
        for idx, dist in zip(indices[0], distances[0]):
            if idx < len(faiss_cid_map):
                results.append({
                    "cid": faiss_cid_map[idx],
                    "distance": float(dist),
                    "similarity": 1.0 / (1.0 + float(dist))
                })
        
        return {"query": query, "results": results}
    except Exception as e:
        return JSONResponse(content={'error': str(e)}, status_code=500)


@app.get("/notifications")
async def get_notifications():
    async def event_stream():
        global last_heartbeat_received
        
        try:
            my_id = get_my_peer_id()
            register_peer(my_id)
            
            process = subprocess.Popen(
                ['ipfs', 'pubsub', 'sub', CANAL_PUBSUB],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0
            )
            
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            
            buffer = ""
            
            while running:
                byte = process.stdout.read(1)
                if not byte:
                    break
                
                char = byte.decode('utf-8', errors='ignore')
                buffer += char
                
                if char == '}':
                    try:
                        message_obj = json.loads(buffer)
                        buffer = ""
                        
                        msg_type = message_obj.get('type')
                        sender = message_obj.get('from', message_obj.get('peer_id', message_obj.get('leader_id')))
                        
                        if sender == my_id and msg_type not in ['version_update_request', 'version_commit', 'document_proposal']:
                            continue
                        
                        if msg_type == 'leader_heartbeat':
                            leader_id_msg = message_obj.get('leader_id')
                            term = message_obj.get('term', 0)
                            last_heartbeat_received = datetime.now()
                            if term >= current_term:
                                become_follower(term, leader_id_msg)
                        
                        elif msg_type == 'document_proposal':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename')
                            
                            if doc_id not in voting_sessions:
                                voting_sessions[doc_id] = {
                                    "doc_id": doc_id,
                                    "filename": filename,
                                    "status": "pending_approval",
                                    "total_peers": message_obj.get('total_peers', 1),
                                    "required_votes": message_obj.get('required_votes', 1),
                                    "votes_approve": set(),
                                    "votes_reject": set(),
                                    "received_from_peer": True
                                }
                                
                                print(f"\n📢 Proposta: {filename}")
                                yield f"data: {json.dumps({'type': 'document_proposal', 'doc_id': doc_id, 'filename': filename})}\n\n"
                        
                        elif msg_type == 'peer_vote':
                            doc_id = message_obj.get('doc_id')
                            vote = message_obj.get('vote')
                            peer_id_vote = message_obj.get('peer_id')
                            
                            if doc_id in voting_sessions:
                                result = process_vote(doc_id, peer_id_vote, vote)
                        
                        elif msg_type == 'version_update_request':
                            version = message_obj.get('version')
                            cid = message_obj.get('cid')
                            embeddings_list = message_obj.get('embeddings')
                            
                            print(f"\n📥 Atualização: v{version}")
                            
                            if embeddings_list:
                                embeddings_array = np.array(embeddings_list)
                                np.save(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", embeddings_array)
                            
                            vector = load_document_vector()
                            vector["version_pending"] = version
                            vector.setdefault("documents_temp", [])
                            vector["documents_temp"].append({
                                "cid": cid,
                                "filename": message_obj.get('filename'),
                                "added_at": datetime.now().isoformat()
                            })
                            save_document_vector(vector)
                            
                            local_hash = calculate_vector_hash(vector)
                            
                            confirm_msg = {
                                "type": "version_confirmation",
                                "peer_id": my_id,
                                "version": version,
                                "hash": local_hash,
                                "timestamp": datetime.now().isoformat()
                            }
                            
                            publish_to_pubsub(confirm_msg)
                        
                        elif msg_type == 'version_confirmation':
                            if current_state == NodeState.LEADER:
                                peer_id_conf = message_obj.get('peer_id')
                                version = message_obj.get('version')
                                peer_hash = message_obj.get('hash')
                                process_peer_confirmation(peer_id_conf, version, peer_hash)
                        
                        elif msg_type == 'version_commit':
                            version = message_obj.get('version')
                            cid = message_obj.get('cid')
                            
                            print(f"\n✅ COMMIT: v{version}")
                            
                            temp_path = f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy"
                            final_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
                            
                            if os.path.exists(temp_path):
                                os.rename(temp_path, final_path)
                            
                            vector = load_document_vector()
                            if vector.get("documents_temp"):
                                for temp_doc in vector["documents_temp"]:
                                    if temp_doc["cid"] == cid:
                                        temp_doc["confirmed"] = True
                                        vector["documents_confirmed"].append(temp_doc)
                                
                                vector["version_confirmed"] = version
                                vector["documents_temp"] = []
                                save_document_vector(vector)
                            
                            update_faiss_index_after_commit(cid)
                        
                        elif msg_type == 'peer_heartbeat':
                            peer_id_hb = message_obj.get('peer_id')
                            if peer_id_hb:
                                register_peer(peer_id_hb)
                    
                    except json.JSONDecodeError:
                        buffer = ""
                    except Exception as e:
                        buffer = ""
        
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )


def start_background_threads():
    global running
    running = True
    
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=election_timer_loop, daemon=True).start()


@app.on_event("startup")
async def startup_event():
    global current_state, leader_id, current_term
    
    my_id = get_my_peer_id()
    register_peer(my_id)
    load_faiss_index()
    
    # Torna-se líder imediatamente
    current_state = NodeState.LEADER
    leader_id = my_id
    current_term = 1
    
    print("\n" + "="*60)
    print("SISTEMA DISTRIBUÍDO")
    print("="*60)
    print(f"Peer ID: {my_id}")
    print(f"Estado: {current_state.value}")
    print(f"Term: {current_term}")
    print(f"FAISS: {faiss_index.ntotal} vetores")
    print("="*60)
    print("\n✅ SERVIDOR PRONTO!")
    print("="*60 + "\n")
    
    start_background_threads()
    
    # Envia heartbeat imediatamente
    send_leader_heartbeat()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")
