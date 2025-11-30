import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
from sentence_transformers import SentenceTransformer
import requests
import json
import numpy as np
import faiss
import os
import random
import threading
import time
import subprocess
import uuid
import hashlib
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Set
from enum import Enum

# ================= CONFIGURAÇÃO =================
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
HTTP_PORT = 5000
VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
FAISS_INDEX_FILE = "faiss_index.bin"

# Timeouts (Ajustados para estabilidade)
HEARTBEAT_INTERVAL = 4  # Intervalo entre heartbeats
LEADER_TIMEOUT = 12     # Tempo para considerar líder morto
PEER_TIMEOUT = 30       # Tempo para considerar peer inativo
ELECTION_TIMEOUT_MIN = 5
ELECTION_TIMEOUT_MAX = 10

# Diretórios
Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(TEMP_EMBEDDINGS_DIR).mkdir(exist_ok=True)

# Modelo SBERT
print("🔄 A carregar modelo SentenceTransformers...", end=" ", flush=True)
try:
    embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    print("✅")
except Exception as e:
    print(f"❌ Erro: {e}")
    embedding_model = None

# Estado do Nó
class NodeState(Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

# ================= VARIÁVEIS GLOBAIS =================
# Estado de Consenso
current_state = NodeState.FOLLOWER
current_term = 0
voted_for = None
my_peer_id = None
leader_id = None
last_leader_heartbeat = datetime.now()
running = True
api_started = False

# Estruturas de Dados Distribuídas
voting_sessions: Dict[str, dict] = {}           # Votações de documentos
pending_confirmations: Dict[int, dict] = {}     # Confirmações de versão (2PC)
system_peers: Dict[str, datetime] = {}          # Rastreio de peers ativos
votes_received: Set[str] = set()                # Votos recebidos na eleição

# FAISS
faiss_index = faiss.IndexFlatL2(384)
faiss_cid_map = []

# FastAPI App
app = FastAPI(title="IPFS Distributed Node", version="2.0")

# ================= GESTÃO DE PEERS =================

def register_peer(peer_id: str):
    """Regista ou atualiza timestamp de um peer"""
    system_peers[peer_id] = datetime.now()

def cleanup_inactive_peers():
    """Remove peers que não enviam heartbeat há muito tempo"""
    now = datetime.now()
    inactive = [
        pid for pid, last_seen in system_peers.items()
        if (now - last_seen).total_seconds() > PEER_TIMEOUT
    ]
    for pid in inactive:
        del system_peers[pid]
        print(f"⚠️  Peer {pid[:10]}... marcado como inativo")

def get_active_peers_count() -> int:
    """Retorna número de peers ativos (incluindo este nó)"""
    cleanup_inactive_peers()
    my_id = get_my_peer_id()
    if my_id not in system_peers:
        register_peer(my_id)
    return len(system_peers)

def calculate_required_votes() -> int:
    """Calcula maioria necessária dinamicamente"""
    total = get_active_peers_count()
    return (total // 2) + 1

# ================= FUNÇÕES AUXILIARES IPFS =================

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

def publish_to_pubsub(message_data: dict) -> bool:
    """Publica mensagem no canal IPFS PubSub"""
    try:
        message_json = json.dumps(message_data)
        subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE
        ).communicate(input=message_json.encode('utf-8'), timeout=5)
        return True
    except: 
        return False

def configure_ipfs_auto_discovery():
    """Configura IPFS para descoberta automática de peers"""
    print("🔧 A configurar IPFS...", end=" ", flush=True)
    try:
        subprocess.run(['ipfs', 'config', '--json', 'Discovery.MDNS.Enabled', 'true'], 
                      capture_output=True, timeout=5)
        subprocess.run(['ipfs', 'config', 'Pubsub.Router', 'gossipsub'], 
                      capture_output=True, timeout=5)
        print("✅")
    except:
        print("⚠️")

# ================= PERSISTÊNCIA E FAISS =================

def load_faiss_index():
    """Carrega índice FAISS do disco"""
    global faiss_index, faiss_cid_map
    if os.path.exists(FAISS_INDEX_FILE):
        try:
            faiss_index = faiss.read_index(FAISS_INDEX_FILE)
            vector_data = load_document_vector()
            faiss_cid_map = [doc['cid'] for doc in vector_data.get('documents_confirmed', [])]
            print(f"📚 FAISS: {faiss_index.ntotal} vetores carregados")
        except Exception as e:
            print(f"⚠️  Erro FAISS: {e}")
            faiss_index = faiss.IndexFlatL2(384)
            faiss_cid_map = []
    else:
        print("📚 FAISS: Índice novo criado")

def save_faiss_index():
    """Guarda índice FAISS no disco"""
    try:
        faiss.write_index(faiss_index, FAISS_INDEX_FILE)
    except Exception as e:
        print(f"❌ Erro ao guardar FAISS: {e}")

def update_faiss_after_commit(cid: str):
    """Adiciona embedding ao FAISS após commit"""
    global faiss_index, faiss_cid_map
    embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
    
    if os.path.exists(embedding_path):
        try:
            embedding = np.load(embedding_path)
            faiss_index.add(embedding.reshape(1, -1))
            faiss_cid_map.append(cid)
            save_faiss_index()
            print(f"📊 FAISS atualizado: {cid[:12]}... (Total: {faiss_index.ntotal})")
        except Exception as e:
            print(f"❌ Erro FAISS: {e}")

def load_document_vector() -> dict:
    """Carrega vetor de documentos do disco"""
    if os.path.exists(VECTOR_FILE):
        try:
            with open(VECTOR_FILE, 'r') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except:
            pass
    return {
        "version_confirmed": 0,
        "version_pending": 0,
        "documents_confirmed": [],
        "documents_temp": [],
        "documents_rejected": []
    }

def save_document_vector(vector_data: dict):
    """Guarda vetor de documentos no disco"""
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)

def calculate_vector_hash(vector_data: dict) -> str:
    """Calcula hash do estado atual (para consistência)"""
    cids = [doc['cid'] for doc in vector_data.get('documents_confirmed', [])]
    vector_str = json.dumps(cids, sort_keys=True)
    return hashlib.sha256(vector_str.encode()).hexdigest()

def generate_embeddings(text_content: str) -> np.ndarray:
    """Gera embeddings do texto"""
    if embedding_model:
        return embedding_model.encode(text_content, convert_to_numpy=True)
    else:
        # Fallback: vetor aleatório se modelo falhar
        return np.random.rand(384).astype('float32')

def extract_text_from_file(content: bytes, filename: str) -> str:
    """Extrai texto de um ficheiro"""
    try:
        return content.decode('utf-8')
    except:
        return f"Documento binário: {filename}"

# ================= API HTTP (Ativa quando Líder) =================

def start_http_server_thread():
    """Inicia servidor HTTP numa thread separada"""
    global api_started
    if not api_started:
        print("\n🌐 A LEVANTAR SERVIDOR HTTP (Porta 5000)...")
        api_started = True
        config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="critical")
        server = uvicorn.Server(config)
        threading.Thread(target=server.run, daemon=True).start()
        print("✅ API HTTP ATIVA! Este nó aceita uploads.\n")

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Endpoint para upload de ficheiros (só funciona se for Líder)"""
    if current_state != NodeState.LEADER:
        return JSONResponse(
            content={
                "error": "Não sou líder",
                "leader_id": leader_id,
                "hint": "Conecte ao líder ou aguarde reeleição"
            },
            status_code=403
        )
    
    try:
        filename = file.filename
        content = await file.read()
        print(f"\n{'='*60}")
        print(f"📥 UPLOAD HTTP: {filename} ({len(content)} bytes)")
        print(f"{'='*60}")
        
        doc_id = str(uuid.uuid4())
        temp_path = f"{TEMP_EMBEDDINGS_DIR}/upload_{doc_id}_{filename}"
        
        # Guarda temporariamente
        with open(temp_path, 'wb') as f:
            f.write(content)
        
        # Calcula votos necessários dinamicamente
        required = calculate_required_votes()
        
        # Cria sessão de votação
        voting_sessions[doc_id] = {
            "doc_id": doc_id,
            "filename": filename,
            "status": "pending_approval",
            "votes_approve": {get_my_peer_id()},  # Líder vota em si mesmo
            "votes_reject": set(),
            "required_votes": required,
            "total_peers": get_active_peers_count(),
            "temp_path": temp_path,
            "content": content
        }
        
        # Propaga proposta
        msg = {
            "type": "document_proposal",
            "doc_id": doc_id,
            "filename": filename,
            "term": current_term,
            "leader_id": get_my_peer_id(),
            "required_votes": required,
            "total_peers": get_active_peers_count()
        }
        publish_to_pubsub(msg)
        
        print(f"📢 Proposta enviada (Votos necessários: {required}/{get_active_peers_count()})")
        print(f"{'='*60}\n")
        
        return {
            "status": "pending_approval",
            "doc_id": doc_id,
            "filename": filename,
            "required_votes": required
        }
    
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/status")
def get_status():
    """Retorna estado do nó"""
    return {
        "peer_id": get_my_peer_id(),
        "state": current_state.value,
        "term": current_term,
        "leader": leader_id,
        "is_http_active": api_started,
        "active_peers": get_active_peers_count(),
        "faiss_vectors": faiss_index.ntotal
    }

@app.get("/")
def root():
    """Informação geral do sistema"""
    vector = load_document_vector()
    return {
        "system": "IPFS Distributed System",
        "version": "2.0",
        "node_state": current_state.value,
        "is_leader": current_state == NodeState.LEADER,
        "leader_id": leader_id[:20] if leader_id else None,
        "peer_id": get_my_peer_id()[:20],
        "term": current_term,
        "version_confirmed": vector.get("version_confirmed", 0),
        "total_documents": len(vector.get("documents_confirmed", [])),
        "faiss_vectors": faiss_index.ntotal,
        "active_peers": get_active_peers_count()
    }

# ================= CONSENSO RAFT =================

def send_heartbeat():
    """Líder envia heartbeat periódico"""
    if current_state == NodeState.LEADER:
        msg = {
            "type": "leader_heartbeat",
            "leader_id": get_my_peer_id(),
            "term": current_term,
            "timestamp": datetime.now().isoformat()
        }
        publish_to_pubsub(msg)
        print(f"💓 Heartbeat enviado (Term {current_term})   ", end='\r', flush=True)
    else:
        # Followers também enviam heartbeat (para rastreio)
        msg = {
            "type": "peer_heartbeat",
            "peer_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat()
        }
        publish_to_pubsub(msg)

def check_leader_alive() -> bool:
    """Verifica se o líder está vivo"""
    global last_leader_heartbeat
    if current_state == NodeState.LEADER:
        return True
    diff = (datetime.now() - last_leader_heartbeat).total_seconds()
    return diff <= LEADER_TIMEOUT

def start_election():
    """Inicia processo de eleição"""
    global current_state, current_term, voted_for, votes_received, last_leader_heartbeat
    
    current_state = NodeState.CANDIDATE
    current_term += 1
    voted_for = get_my_peer_id()
    votes_received = {get_my_peer_id()}
    last_leader_heartbeat = datetime.now()
    
    print(f"\n\n{'='*60}")
    print(f"🚨 LÍDER DESCONECTADO! INICIANDO ELEIÇÃO")
    print(f"{'='*60}")
    print(f"Term: {current_term}")
    print(f"Candidato: {get_my_peer_id()[:20]}...")
    print(f"{'='*60}\n")
    
    msg = {
        "type": "election_request_vote",
        "candidate_id": get_my_peer_id(),
        "term": current_term,
        "timestamp": datetime.now().isoformat()
    }
    publish_to_pubsub(msg)
    
    # Aguarda votos
    time.sleep(random.uniform(2, 4))
    
    # Verifica se ganhou
    required = calculate_required_votes()
    if len(votes_received) >= required:
        become_leader()
    else:
        print(f"❌ Eleição falhada ({len(votes_received)}/{required} votos)")

def become_leader():
    """Assume liderança"""
    global current_state, leader_id
    if current_state == NodeState.LEADER:
        return
    
    current_state = NodeState.LEADER
    leader_id = get_my_peer_id()
    
    print(f"\n{'='*60}")
    print(f"👑 ELEITO LÍDER!")
    print(f"{'='*60}")
    print(f"Peer ID: {leader_id[:40]}...")
    print(f"Term: {current_term}")
    print(f"Peers Ativos: {get_active_peers_count()}")
    print(f"{'='*60}\n")
    
    send_heartbeat()
    start_http_server_thread()

def become_follower(new_term: int, new_leader: str):
    """Reconhece novo líder"""
    global current_state, current_term, voted_for, leader_id, last_leader_heartbeat
    
    last_leader_heartbeat = datetime.now()
    
    if new_term >= current_term:
        current_term = new_term
        voted_for = None
        leader_id = new_leader
        
        if current_state != NodeState.FOLLOWER:
            print(f"\n📍 Novo líder reconhecido: {new_leader[:20]}... (Term {new_term})")
            current_state = NodeState.FOLLOWER

# ================= PROCESSAMENTO DE MENSAGENS =================

def pubsub_listener():
    """Thread que escuta mensagens PubSub"""
    global running, last_leader_heartbeat, current_term, current_state, voted_for, votes_received
    
    print("📡 A conectar ao PubSub...", flush=True)
    proc = subprocess.Popen(
        ['ipfs', 'pubsub', 'sub', CANAL_PUBSUB],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    print("✅ Conectado ao PubSub\n")
    
    while running:
        try:
            line = proc.stdout.readline()
            if not line:
                continue
            
            try:
                data = json.loads(line.decode('utf-8', errors='ignore'))
            except:
                continue
            
            msg_type = data.get('type')
            sender = data.get('leader_id') or data.get('peer_id') or data.get('candidate_id')
            
            if sender == get_my_peer_id():
                continue
            
            # === HEARTBEAT DE LÍDER ===
            if msg_type == 'leader_heartbeat':
                term = data.get('term', 0)
                if term >= current_term:
                    last_leader_heartbeat = datetime.now()
                    if current_state != NodeState.FOLLOWER:
                        become_follower(term, data.get('leader_id'))
            
            # === HEARTBEAT DE PEER ===
            elif msg_type == 'peer_heartbeat':
                peer_id = data.get('peer_id')
                if peer_id:
                    register_peer(peer_id)
            
            # === PEDIDO DE VOTO (ELEIÇÃO) ===
            elif msg_type == 'election_request_vote':
                term = data.get('term', 0)
                cand = data.get('candidate_id')
                if term > current_term:
                    current_term = term
                    current_state = NodeState.FOLLOWER
                    voted_for = cand
                    publish_to_pubsub({
                        "type": "election_vote",
                        "voter_id": get_my_peer_id(),
                        "candidate_id": cand,
                        "term": term
                    })
                    print(f"🗳️  Votei em {cand[:12]}... (Term {term})")
            
            # === VOTO RECEBIDO ===
            elif msg_type == 'election_vote':
                if current_state == NodeState.CANDIDATE and data.get('candidate_id') == get_my_peer_id():
                    votes_received.add(data.get('voter_id'))
                    print(f"✅ Voto recebido ({len(votes_received)}/{calculate_required_votes()})")
            
            # === PROPOSTA DE DOCUMENTO ===
            elif msg_type == 'document_proposal':
                doc_id = data.get('doc_id')
                filename = data.get('filename')
                if doc_id and doc_id not in voting_sessions:
                    voting_sessions[doc_id] = {
                        "doc_id": doc_id,
                        "filename": filename,
                        "status": "pending_approval",
                        "votes_approve": set(),
                        "votes_reject": set(),
                        "required_votes": data.get('required_votes', 1)
                    }
                    print(f"\n📢 PROPOSTA: {filename}")
                    # Auto-voto APPROVE
                    time.sleep(0.5)
                    publish_to_pubsub({
                        "type": "peer_vote",
                        "doc_id": doc_id,
                        "vote": "approve",
                        "peer_id": get_my_peer_id()
                    })
                    print(f"✅ Votei APPROVE")
            
            # === VOTO EM DOCUMENTO ===
            elif msg_type == 'peer_vote' and current_state == NodeState.LEADER:
                doc_id = data.get('doc_id')
                if doc_id in voting_sessions:
                    session = voting_sessions[doc_id]
                    peer_id = data.get('peer_id')
                    vote = data.get('vote')
                    
                    if vote == 'approve':
                        session['votes_approve'].add(peer_id)
                    elif vote == 'reject':
                        session['votes_reject'].add(peer_id)
                    
                    approve_count = len(session['votes_approve'])
                    required = session['required_votes']
                    
                    print(f"📊 Voto: {vote.upper()} | Status: {approve_count}/{required}")
                    
                    if approve_count >= required:
                        print(f"✅ CONSENSO ATINGIDO! Processando {session['filename']}...")
                        finalize_approved_document(doc_id)
            
            # === COMMIT DE VERSÃO ===
            elif msg_type == 'version_commit':
                cid = data.get('cid')
                version = data.get('version')
                filename = data.get('filename')
                embeddings_list = data.get('embeddings')
                
                print(f"\n📥 COMMIT: v{version} | CID: {cid[:12]}...")
                
                # Guarda embedding
                if embeddings_list:
                    emb_array = np.array(embeddings_list, dtype='float32')
                    np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", emb_array)
                
                # Atualiza vetor local
                vector = load_document_vector()
                vector['documents_confirmed'].append({
                    "cid": cid,
                    "filename": filename,
                    "version": version,
                    "committed_at": datetime.now().isoformat()
                })
                vector['version_confirmed'] = version
                save_document_vector(vector)
                
                # Atualiza FAISS
                update_faiss_after_commit(cid)
        
        except Exception as e:
            pass

def finalize_approved_document(doc_id: str):
    """Líder finaliza documento aprovado"""
    if doc_id not in voting_sessions:
        return
    
    session = voting_sessions.pop(doc_id)
    temp_path = session['temp_path']
    filename = session['filename']
    content = session['content']
    
    try:
        # Adiciona ao IPFS
        res = subprocess.run(['ipfs', 'add', '-Q', temp_path], capture_output=True, text=True)
        cid = res.stdout.strip()
        
        # Gera embeddings
        text = extract_text_from_file(content, filename)
        embeddings = generate_embeddings(text)
        
        # Guarda embedding temporariamente
        np.save(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", embeddings)
        
        # Remove ficheiro temporário
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # Atualiza versão
        vector = load_document_vector()
        new_version = vector.get('version_confirmed', 0) + 1
        
        # Publica commit
        publish_to_pubsub({
            "type": "version_commit",
            "cid": cid,
            "filename": filename,
            "version": new_version,
            "term": current_term,
            "embeddings": embeddings.tolist(),
            "leader_id": get_my_peer_id()
        })
        
        print(f"🚀 COMMIT ENVIADO: v{new_version} | CID: {cid}")
        
        # Atualiza localmente também
        vector['documents_confirmed'].append({
            "cid": cid,
            "filename": filename,
            "version": new_version,
            "committed_at": datetime.now().isoformat()
        })
        vector['version_confirmed'] = new_version
        save_document_vector(vector)
        
        # Move embedding para pasta final
        os.rename(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", f"{EMBEDDINGS_DIR}/{cid}.npy")
        
        # Atualiza FAISS
        update_faiss_after_commit(cid)
    
    except Exception as e:
        print(f"❌ Erro ao finalizar: {e}")

# ================= TIMERS =================

def heartbeat_timer():
    """Thread que envia heartbeats periodicamente"""
    while running:
        send_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)

def election_timer():
    """Thread que monitora líder e inicia eleições"""
    global last_leader_heartbeat
    time.sleep(5)  # Aguarda estabilização inicial
    last_leader_heartbeat = datetime.now()
    
    while running:
        time.sleep(random.uniform(1, 3))
        if current_state == NodeState.FOLLOWER:
            if not check_leader_alive():
                start_election()

# ================= MAIN =================

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"🚀 NÓ IPFS DISTRIBUÍDO (v2.0)")
    print(f"{'='*60}")
    print(f"Peer ID: {get_my_peer_id()}")
    print(f"{'='*60}\n")
    
    configure_ipfs_auto_discovery()
    load_faiss_index()
    
    # Regista-se como peer ativo
    register_peer(get_my_peer_id())
    
    if len(sys.argv) > 1 and sys.argv[1] == "--initial-leader":
        print("👑 Modo: LÍDER INICIAL\n")
        become_leader()
    else:
        print("👀 Modo: FOLLOWER\n")
        last_leader_heartbeat = datetime.now()
    
    # Inicia threads
    t1 = threading.Thread(target=pubsub_listener, daemon=True, name="PubSub")
    t2 = threading.Thread(target=heartbeat_timer, daemon=True, name="Heartbeat")
    t3 = threading.Thread(target=election_timer, daemon=True, name="Election")
    
    t1.start()
    t2.start()
    t3.start()
    
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 A encerrar nó...")
        running = False
        time.sleep(1)
