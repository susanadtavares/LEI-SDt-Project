import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
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
from typing import Dict
from enum import Enum

# ================= CONFIGURAÇÃO =================
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
HTTP_PORT = 5000
VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
FAISS_INDEX_FILE = "faiss_index.bin"

# Timeouts
HEARTBEAT_INTERVAL = 5
LEADER_TIMEOUT = 15  # Tempo limite para considerar líder desconectado
ELECTION_TIMEOUT_MIN = 10
ELECTION_TIMEOUT_MAX = 20

# Diretórios
Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(TEMP_EMBEDDINGS_DIR).mkdir(exist_ok=True)

# Modelo AI
print("🔄 A carregar modelo AI...")
try:
    embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    print("✅ Modelo carregado!")
except:
    print("⚠️ Modelo não encontrado (verifique internet ou instalação)")

# Estado do Nó
class NodeState(Enum):
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

# Variáveis Globais
current_state = NodeState.FOLLOWER
current_term = 0
voted_for = None
my_peer_id = None
leader_id = None
last_leader_heartbeat = datetime.now() # Inicializa com tempo atual
running = True
api_started = False # Flag para saber se o HTTP já está a correr

# Estruturas de Dados
voting_sessions: Dict[str, dict] = {}
pending_confirmations: Dict[int, dict] = {}
system_peers: Dict[str, datetime] = {}
votes_received = set()

# FAISS
faiss_index = faiss.IndexFlatL2(384)
faiss_cid_map = []

# FastAPI App (Inicialmente inativa para o mundo exterior)
app = FastAPI(title="IPFS Distributed Node")

# ================= FUNÇÕES AUXILIARES IPFS =================

def get_my_peer_id():
    global my_peer_id
    if my_peer_id: return my_peer_id
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=5)
        if response.status_code == 200:
            my_peer_id = response.json()['ID']
            return my_peer_id
    except: pass
    return "unknown"

def publish_to_pubsub(message_data: dict):
    try:
        message_json = json.dumps(message_data)
        subprocess.Popen(['ipfs', 'pubsub', 'pub', CANAL_PUBSUB], 
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                        ).communicate(input=message_json.encode('utf-8'), timeout=5)
        return True
    except: return False

def configure_ipfs_auto_discovery():
    """Ativa mDNS e Gossipsub para garantir conexão entre peers"""
    print("🔧 A configurar IPFS Auto-Discovery...")
    subprocess.run(['ipfs', 'config', '--json', 'Discovery.MDNS.Enabled', 'true'], capture_output=True)
    subprocess.run(['ipfs', 'config', 'Pubsub.Router', 'gossipsub'], capture_output=True)

def load_faiss_index():
    global faiss_index, faiss_cid_map
    if os.path.exists(FAISS_INDEX_FILE):
        try:
            faiss_index = faiss.read_index(FAISS_INDEX_FILE)
            if os.path.exists(VECTOR_FILE):
                with open(VECTOR_FILE, 'r') as f:
                    content = f.read().strip()
                    if content:
                        data = json.loads(content)
                        faiss_cid_map = [doc['cid'] for doc in data.get('documents_confirmed', [])]
        except:
            faiss_index = faiss.IndexFlatL2(384)

def save_document_vector(vector_data):
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)

def load_document_vector():
    if os.path.exists(VECTOR_FILE):
        try:
            with open(VECTOR_FILE, 'r') as f:
                content = f.read().strip()
                return json.loads(content) if content else {"version_confirmed": 0, "documents_confirmed": [], "documents_temp": []}
        except: pass
    return {"version_confirmed": 0, "documents_confirmed": [], "documents_temp": []}

# ================= LÓGICA DE SERVIDOR HTTP (Ativa quando Líder) =================

def start_http_server_thread():
    """Inicia o servidor HTTP numa thread separada"""
    global api_started
    if not api_started:
        print("\n🌐 A LEVANTAR SERVIDOR HTTP NA PORTA 5000...")
        api_started = True
        # Configuração do servidor Uvicorn
        config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="critical")
        server = uvicorn.Server(config)
        # Inicia numa thread para não bloquear o consenso
        threading.Thread(target=server.run, daemon=True).start()
        print("✅ API HTTP PRONTA! Aceita uploads.")

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Só aceita upload se for Líder
    if current_state != NodeState.LEADER:
        return JSONResponse(content={"error": "Não sou líder. Aguarde reeleição."}, status_code=403)
    
    try:
        filename = file.filename
        content = await file.read()
        print(f"\n📥 [HTTP] RECEBIDO UPLOAD: {filename}")
        
        doc_id = str(uuid.uuid4())
        
        # Guarda temporariamente
        temp_path = f"{TEMP_EMBEDDINGS_DIR}/upload_{doc_id}_{filename}"
        with open(temp_path, 'wb') as f: f.write(content)
        
        # Inicia votação
        voting_sessions[doc_id] = {
            "doc_id": doc_id, "filename": filename, "status": "pending_approval",
            "votes_approve": {get_my_peer_id()}, "votes_reject": set(),
            "required_votes": 1, # Simplificado para teste. Num sistema real seria (N/2)+1
            "temp_path": temp_path
        }
        
        # Propaga proposta
        msg = {
            "type": "document_proposal", "doc_id": doc_id, "filename": filename,
            "term": current_term, "leader_id": get_my_peer_id()
        }
        publish_to_pubsub(msg)
        
        return {"status": "pending_approval", "doc_id": doc_id}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/status")
def get_status():
    return {
        "peer_id": get_my_peer_id(),
        "state": current_state.value,
        "term": current_term,
        "leader": leader_id,
        "api_active": api_started
    }

# ================= LÓGICA DE CONSENSO (RAFT SIMPLIFICADO) =================

def send_heartbeat():
    """Líder envia heartbeat periódico"""
    if current_state == NodeState.LEADER:
        msg = {
            "type": "leader_heartbeat", "leader_id": get_my_peer_id(),
            "term": current_term, "timestamp": datetime.now().isoformat()
        }
        publish_to_pubsub(msg)
        print(f"💓 HB enviado (Term {current_term})", end='\r')

def check_leader_alive():
    """Verifica se o líder morreu baseando-se no last_leader_heartbeat"""
    global last_leader_heartbeat
    if current_state == NodeState.LEADER: return True
    
    # Calcula diferença de tempo
    diff = (datetime.now() - last_leader_heartbeat).total_seconds()
    
    # Se passar do limite, considera desconectado
    return diff <= LEADER_TIMEOUT

def start_election():
    """Inicia eleição quando deteta falha"""
    global current_state, current_term, voted_for, votes_received, last_leader_heartbeat
    
    current_state = NodeState.CANDIDATE
    current_term += 1
    voted_for = get_my_peer_id()
    votes_received = {get_my_peer_id()}
    last_leader_heartbeat = datetime.now() # Reset timer para dar tempo à eleição
    
    print(f"\n\n🚨 LÍDER FALHOU! A INICIAR ELEIÇÃO (Term {current_term})")
    print(f"🗳️  Eu ({get_my_peer_id()[:10]}...) sou candidato.")
    
    msg = {
        "type": "election_request_vote", "candidate_id": get_my_peer_id(),
        "term": current_term
    }
    publish_to_pubsub(msg)
    
    # Aguarda votos por 3 segundos
    time.sleep(3)
    
    # Se tiver votos suficientes (aqui > 0 porque votou em si mesmo e estamos a testar failover)
    if len(votes_received) >= 1:
        become_leader()

def become_leader():
    """Assume liderança"""
    global current_state, leader_id
    if current_state == NodeState.LEADER: return

    current_state = NodeState.LEADER
    leader_id = get_my_peer_id()
    
    print(f"\n{'='*40}")
    print(f"👑 GANHOU ELEIÇÃO E ASSUME LIDERANÇA")
    print(f"{'='*40}")
    
    # 1. Envia Heartbeat imediato
    send_heartbeat()
    
    # 2. ATIVA O SERVIDOR HTTP (Aqui está a magia)
    start_http_server_thread()

def become_follower(new_term, new_leader):
    """Reconhece outro líder"""
    global current_state, current_term, voted_for, leader_id, last_leader_heartbeat
    
    # Atualiza o timestamp do heartbeat SEMPRE que chama esta função
    last_leader_heartbeat = datetime.now()
    
    if new_term >= current_term:
        current_term = new_term
        voted_for = None
        leader_id = new_leader
        
        if current_state != NodeState.FOLLOWER:
            print(f"\n📍 Novo líder reconhecido: {new_leader[:15]}... (Term {new_term})")
            current_state = NodeState.FOLLOWER

# ================= PROCESSAMENTO DE MENSAGENS =================

def pubsub_listener():
    global running, last_leader_heartbeat, current_term, current_state, voted_for, votes_received
    
    print("📡 A conectar ao PubSub...")
    # Usa stdbuf para garantir que não há buffer no output do IPFS
    proc = subprocess.Popen(['ipfs', 'pubsub', 'sub', CANAL_PUBSUB], 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    while running:
        try:
            line = proc.stdout.readline()
            if not line: continue
            
            # Descodifica a mensagem
            try:
                data = json.loads(line.decode('utf-8', errors='ignore'))
            except: continue

            msg_type = data.get('type')
            sender = data.get('leader_id') or data.get('peer_id') or data.get('candidate_id')
            
            # Ignora mensagens do próprio nó
            if sender == get_my_peer_id(): continue

            # --- Lógica de Heartbeat ---
            if msg_type == 'leader_heartbeat':
                term = data.get('term', 0)
                if term >= current_term:
                    # ATUALIZAÇÃO CRÍTICA DO TIMESTAMP
                    last_leader_heartbeat = datetime.now()
                    become_follower(term, data.get('leader_id'))

            # --- Lógica de Eleição ---
            elif msg_type == 'election_request_vote':
                term = data.get('term', 0)
                cand = data.get('candidate_id')
                if term > current_term:
                    current_term = term
                    current_state = NodeState.FOLLOWER
                    voted_for = cand
                    # Vota sim
                    publish_to_pubsub({"type": "election_vote", "voter_id": get_my_peer_id(), "candidate_id": cand, "term": term})
                    print(f"🗳️  Votei em {cand[:10]}...")

            elif msg_type == 'election_vote':
                if current_state == NodeState.CANDIDATE and data.get('candidate_id') == get_my_peer_id():
                    votes_received.add(data.get('voter_id'))
                    print(f"✅ Voto recebido de {data.get('voter_id')[:10]}...")

            # --- Lógica de Documentos ---
            elif msg_type == 'document_proposal':
                doc_id = data.get('doc_id')
                print(f"📢 Proposta recebida: {data.get('filename')}. Voto APPROVE.")
                publish_to_pubsub({"type": "peer_vote", "doc_id": doc_id, "vote": "approve", "peer_id": get_my_peer_id()})

            elif msg_type == 'peer_vote' and current_state == NodeState.LEADER:
                doc_id = data.get('doc_id')
                if doc_id in voting_sessions:
                    session = voting_sessions[doc_id]
                    if data.get('vote') == 'approve':
                        session['votes_approve'].add(data.get('peer_id'))
                        # Se tiver votos suficientes
                        if len(session['votes_approve']) >= session['required_votes']:
                            print(f"✅ Aprovado! A finalizar {session['filename']}...")
                            finalize_upload(doc_id)

            elif msg_type == 'version_commit':
                cid = data.get('cid')
                print(f"📥 Commit recebido. Novo CID: {cid[:15]}...")
                update_local_vector(cid, data.get('filename'))

        except Exception as e:
            pass

def finalize_upload(doc_id):
    """Líder adiciona ao IPFS e avisa todos"""
    if doc_id not in voting_sessions: return
    session = voting_sessions.pop(doc_id)
    path = session['temp_path']
    
    # Adiciona ao IPFS
    res = subprocess.run(['ipfs', 'add', '-Q', path], capture_output=True, text=True)
    cid = res.stdout.strip()
    
    # Simula embedding para enviar
    emb = [0.1, 0.2] # Placeholder
    
    if os.path.exists(path): os.remove(path)
    
    # Publica Commit
    publish_to_pubsub({
        "type": "version_commit", "cid": cid, "filename": session['filename'],
        "term": current_term, "embeddings": emb
    })
    print(f"🚀 Commit enviado: {cid}")
    update_local_vector(cid, session['filename'])

def update_local_vector(cid, filename):
    """Atualiza JSON local"""
    vector = load_document_vector()
    vector['documents_confirmed'].append({"cid": cid, "filename": filename, "date": datetime.now().isoformat()})
    save_document_vector(vector)

# ================= TIMERS =================

def heartbeat_timer():
    while running:
        if current_state == NodeState.LEADER:
            send_heartbeat()
        time.sleep(HEARTBEAT_INTERVAL)

def election_timer():
    global last_leader_heartbeat
    # Aguarda estabilização inicial
    time.sleep(5)
    last_leader_heartbeat = datetime.now()
    
    while running:
        # Intervalo aleatório para evitar colisões
        time.sleep(random.uniform(1, 3))
        
        if current_state == NodeState.FOLLOWER:
            alive = check_leader_alive()
            # Debug visual do tempo
            # diff = (datetime.now() - last_leader_heartbeat).total_seconds()
            # print(f"DEBUG: Diff={diff:.1f}s (Limit={LEADER_TIMEOUT})", end='\r')
            
            if not alive:
                start_election()

# ================= ARRANQUE =================

if __name__ == "__main__":
    print(f"\n{'='*50}")
    print(f"🚀 NÓ IPFS UNIFICADO (SERVER + PEER)")
    print(f"🆔 Peer ID: {get_my_peer_id()}")
    print(f"{'='*50}\n")
    
    configure_ipfs_auto_discovery()
    load_faiss_index()
    
    # Argumento opcional para iniciar já como líder (para o PC A)
    if len(sys.argv) > 1 and sys.argv[1] == "--initial-leader":
        print("👑 Configuração: Iniciar como LÍDER INICIAL")
        become_leader()
    else:
        print("👀 Configuração: Iniciar como FOLLOWER")
        # Garante que timestamp inicial é válido para não disparar eleição imediata errada
        last_leader_heartbeat = datetime.now()
    
    # Inicia threads de monitorização
    t1 = threading.Thread(target=pubsub_listener, daemon=True)
    t2 = threading.Thread(target=heartbeat_timer, daemon=True)
    t3 = threading.Thread(target=election_timer, daemon=True)
    
    t1.start()
    t2.start()
    t3.start()
    
    try:
        while running: time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 A encerrar nó...")
        running = False
