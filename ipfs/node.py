from fastapi import FastAPI, File, UploadFile, Query
from fastapi.responses import JSONResponse, StreamingResponse
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel
import requests
import uvicorn
import json
import numpy as np
import os
import hashlib
import uuid
import threading
import time
import subprocess
import sys
import random
import signal
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List, Tuple
from enum import Enum

# ==============================================
# CONFIGURAÇÃO GLOBAL
# ==============================================
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
PENDING_UPLOADS_DIR = "pending_uploads"

# Timeouts e intervalos
LEADER_HEARTBEAT_INTERVAL = 5      # Heartbeat a cada 5s
PEER_TIMEOUT = 30                  # Peer inativo após 30s
LEADER_TIMEOUT = 15                # Líder considerado morto após 15s
ELECTION_TIMEOUT_MIN = 10          # Timeout mínimo para eleição inicial
ELECTION_TIMEOUT_MAX = 15          # Timeout máximo para eleição inicial
SESSION_TIMEOUT = 300              # Sessões antigas removidas após 5min
CONFIRMATION_TIMEOUT = 30          # Confirmações expiram após 30s

# Criar diretórios
for directory in [EMBEDDINGS_DIR, TEMP_EMBEDDINGS_DIR, PENDING_UPLOADS_DIR]:
    Path(directory).mkdir(exist_ok=True)

print("🔧 A carregar modelo SentenceTransformer...")
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("✅ Modelo carregado com sucesso!")

# ==============================================
# ENUMS
# ==============================================

class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

# ==============================================
# SEARCH REQUEST/RESULT
# ==============================================
class SearchRequest(BaseModel):
    prompt: str
    top_k: int = 5

class SearchInitResponse(BaseModel):
    id: str
    token: str


# ==============================================
# THREAD-SAFE NODE CONTEXT
# ==============================================

class NodeContext:
    def __init__(self):
        self._lock = threading.RLock()
        
        # Identidade
        self.peer_id: Optional[str] = None
        
        # Estado RAFT
        self.state: NodeState = NodeState.FOLLOWER
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        self.last_leader_heartbeat: Optional[datetime] = None
        
        # ✅ FIX: Timestamp de startup para eleição inicial
        self.startup_time: datetime = datetime.now()
        
        # RAFT - Eleição
        self.votes_received: Set[str] = set()
        self.current_election_term: int = 0
        
        # Tracking de peers
        self.peers: Dict[str, datetime] = {}
        
        # Votação de documentos
        self.voting_sessions: Dict[str, dict] = {}
        
        # Servidor HTTP
        self.http_server: Optional[uvicorn.Server] = None
        self.http_server_thread: Optional[threading.Thread] = None
        self.app: Optional[FastAPI] = None
        
        # Controlo
        self.running: bool = True
        
        # Confirmações de versão
        self.version_confirmations: Dict[int, Tuple[Set[Tuple[str, str]], datetime]] = {}
        
        # Estruturas temporárias
        self.temp_vectors: Dict[str, dict] = {}

        # Search requests and results
        """O lider regista aqui cada pesquisa iniciada para saber quem pode pedir o token e qual peer processou a prompt"""
        self.search_requests: Dict[str, dict] = {}   # id -> {token, peer_id, created_at}

        """cada peer guarda aqui o output do FAISS para cada ID"""
        self.search_results: Dict[str, dict] = {}    # id -> {results, peer_id, created_at}

        """garante que cada pedido de search é enviado a peers diferentes em round-robin (ciclo entre peers ativos)"""
        self.last_search_peer_index: int = 0
    
    def set_state(self, new_state: NodeState):
        """Thread-safe state transition"""
        with self._lock:
            old_state = self.state
            self.state = new_state
            
            if old_state != new_state:
                print(f"🔄 Transição: {old_state.value.upper()} → {new_state.value.upper()}")
    
    def is_leader(self) -> bool:
        with self._lock:
            return self.state == NodeState.LEADER
    
    def add_vote(self, doc_id: str, peer_id: str, vote_type: str) -> bool:
        """Thread-safe voting - retorna True se voto foi registado"""
        with self._lock:
            if doc_id not in self.voting_sessions:
                return False
            
            session = self.voting_sessions[doc_id]
            
            # Remover votos anteriores (idempotência)
            session["votes_approve"].discard(peer_id)
            session["votes_reject"].discard(peer_id)
            
            # Adicionar novo voto
            if vote_type == "approve":
                session["votes_approve"].add(peer_id)
            else:
                session["votes_reject"].add(peer_id)
            
            return True

node_ctx = NodeContext()

# ==============================================
# CONFIGURAÇÃO IPFS mDNS
# ==============================================

def configurar_ipfs_mdns():
    """Configura IPFS para descoberta automática via mDNS na mesh local"""
    try:
        print("🔧 A configurar IPFS mDNS...")
        
        # Ativar mDNS
        subprocess.run([
            'ipfs', 'config', '--json',
            'Discovery.MDNS.Enabled', 'true'
        ], check=True, capture_output=True, text=True)
        
        # Intervalo de descoberta (5 segundos)
        subprocess.run([
            'ipfs', 'config',
            'Discovery.MDNS.Interval', '5'
        ], check=True, capture_output=True, text=True)
        
        print("✅ mDNS configurado (auto-discovery ativo)")
        return True
    
    except subprocess.CalledProcessError as e:
        stderr = e.stderr if e.stderr else 'N/A'
        if "not found" not in stderr:
            print(f"⚠️ mDNS: {stderr.strip()}")
        return False
    
    except Exception as e:
        print(f"⚠️ Erro mDNS: {e}")
        return False

# ==============================================
# GARBAGE COLLECTOR
# ==============================================

def garbage_collector():
    """Remove sessões antigas, confirmações expiradas e peers inativos"""
    print("🗑️ Garbage collector iniciado")
    
    while node_ctx.running:
        time.sleep(60)  # Roda a cada minuto
        
        now = datetime.now()
        
        with node_ctx._lock:
            # Limpar sessões de votação antigas
            sessoes_removidas = 0
            for doc_id in list(node_ctx.voting_sessions.keys()):
                session = node_ctx.voting_sessions[doc_id]
                created = datetime.fromisoformat(session["created_at"])
                age = (now - created).total_seconds()
                
                if age > SESSION_TIMEOUT:
                    del node_ctx.voting_sessions[doc_id]
                    sessoes_removidas += 1
            
            if sessoes_removidas > 0:
                print(f"🗑️ Removidas {sessoes_removidas} sessões antigas")
            
            # Limpar confirmações de versão expiradas
            confirmacoes_removidas = 0
            for version in list(node_ctx.version_confirmations.keys()):
                peers_set, timestamp = node_ctx.version_confirmations[version]
                age = (now - timestamp).total_seconds()
                
                if age > CONFIRMATION_TIMEOUT:
                    del node_ctx.version_confirmations[version]
                    confirmacoes_removidas += 1
            
            if confirmacoes_removidas > 0:
                print(f"🗑️ Removidas {confirmacoes_removidas} confirmações expiradas")
            
            # Limpar peers inativos
            my_id = obter_peer_id()
            peers_removidos = 0
            for peer_id in list(node_ctx.peers.keys()):
                if peer_id == my_id:
                    continue
                
                last_seen = node_ctx.peers[peer_id]
                age = (now - last_seen).total_seconds()
                
                if age > PEER_TIMEOUT:
                    del node_ctx.peers[peer_id]
                    peers_removidos += 1
            
            if peers_removidos > 0:
                print(f"🗑️ Removidos {peers_removidos} peers inativos")

# ==============================================
# IPFS UTILITIES
# ==============================================

def obter_peer_id() -> str:
    if node_ctx.peer_id:
        return node_ctx.peer_id
    
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=5)
        if response.status_code == 200:
            node_ctx.peer_id = response.json()['ID']
            return node_ctx.peer_id
    except Exception as e:
        print(f"⚠️ Erro ao obter peer ID: {e}")
    
    return "unknown"

def adicionar_ao_ipfs(content: bytes, filename: str) -> Optional[str]:
    """Adiciona conteúdo ao IPFS com retry (3 tentativas)"""
    for tentativa in range(3):
        try:
            files = {'file': (filename, content)}
            response = requests.post(
                f"{IPFS_API_URL}/add",
                files=files,
                params={'pin': 'true'},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()['Hash']
        
        except Exception as e:
            if tentativa < 2:
                print(f"⚠️ Tentativa {tentativa+1}/3 falhou, retrying...")
                time.sleep(1)
            else:
                print(f"❌ Falha ao adicionar ao IPFS: {e}")
    
    return None

def obter_do_ipfs(cid: str) -> Optional[bytes]:
    """Obtém conteúdo do IPFS com retry (3 tentativas)"""
    for tentativa in range(3):
        try:
            response = requests.post(
                f"{IPFS_API_URL}/cat",
                params={'arg': cid},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.content
        
        except Exception as e:
            if tentativa < 2:
                time.sleep(1)
            else:
                print(f"❌ Falha ao obter do IPFS: {e}")
    
    return None

def registar_peer(peer_id: str):
    with node_ctx._lock:
        node_ctx.peers[peer_id] = datetime.now()

def obter_contagem_peers() -> int:
    my_id = obter_peer_id()
    
    with node_ctx._lock:
        if my_id not in node_ctx.peers:
            node_ctx.peers[my_id] = datetime.now()
        return len(node_ctx.peers)

# ==============================================
# VECTOR MANAGEMENT
# ==============================================

def carregar_vetor_documentos() -> dict:
    if os.path.exists(VECTOR_FILE):
        try:
            with open(VECTOR_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Erro ao carregar vetor: {e}")
    
    return {
        "version_confirmed": 0,
        "documents_confirmed": [],
        "documents_rejected": [],
        "last_updated": None
    }

def guardar_vetor_documentos(vector_data: dict):
    try:
        with open(VECTOR_FILE, 'w') as f:
            json.dump(vector_data, f, indent=2)
    except Exception as e:
        print(f"❌ Erro ao guardar vetor: {e}")

# ==============================================
# FAISS MANAGEMENT
# ==============================================

def reconstruir_faiss():
    """Reconstrói índice FAISS com embeddings confirmados"""
    try:
        import faiss
    except ImportError:
        return
    
    print("🔥 A reconstruir índice FAISS...")
    
    vector = carregar_vetor_documentos()
    embeddings = []
    
    for doc in vector.get("documents_confirmed", []):
        emb_file = doc.get("embedding_file")
        if emb_file and os.path.exists(emb_file):
            try:
                embeddings.append(np.load(emb_file).astype('float32'))
            except Exception as e:
                print(f"⚠️ Erro ao carregar embedding: {e}")
    
    if not embeddings:
        print("ℹ️ Sem embeddings para indexar")
        return
    
    try:
        matrix = np.vstack(embeddings).astype('float32')
        index = faiss.IndexFlatL2(matrix.shape[1])
        index.add(matrix)
        faiss.write_index(index, "faiss_index.faiss")
        
        print(f"✅ FAISS reconstruído: {len(embeddings)} documentos")
    except Exception as e:
        print(f"❌ Erro ao reconstruir FAISS: {e}")

def atualizar_faiss_apos_commit():
    """Move embeddings temp/ para embeddings/ e reconstrói FAISS"""
    print("🔄 Atualização FAISS após COMMIT...")
    
    temp_dir = Path(TEMP_EMBEDDINGS_DIR)
    emb_dir = Path(EMBEDDINGS_DIR)
    
    moved = 0
    for temp_file in temp_dir.glob("*.npy"):
        try:
            dest_file = emb_dir / temp_file.name
            temp_file.rename(dest_file)
            moved += 1
            print(f"   ✅ {temp_file.name} → embeddings/")
        except Exception as e:
            print(f"   ⚠️ Erro ao mover {temp_file.name}: {e}")
    
    if moved > 0:
        print(f"📦 {moved} embeddings movidos para permanentes")
        reconstruir_faiss()
    else:
        print("ℹ️ Nenhum embedding temporário para mover")

# ==============================================
# CONFIRMAÇÃO DE VERSÃO
# ==============================================

def calcular_hash_vetor(documents: List[dict]) -> str:
    """Calcula SHA256 do vetor de documentos"""
    vetor_str = json.dumps(documents, sort_keys=True)
    return hashlib.sha256(vetor_str.encode()).hexdigest()

def processar_pedido_confirmacao(version: int, documents: List[dict], cid: str, embedding_cid: str):
    """
    Peer recebe pedido de confirmação do líder
    """
    print(f"\n{'='*60}")
    print(f"📥 Pedido de confirmação (v{version})")
    print(f"{'='*60}")
    
    vector = carregar_vetor_documentos()
    versao_atual = vector.get("version_confirmed", 0)
    
    if version <= versao_atual:
        print(f"⚠️ Conflito: recebida={version}, atual={versao_atual}")
        return None
    
    print(f"📥 A baixar embeddings do IPFS (CID: {embedding_cid[:16]}...)")
    emb_bytes = obter_do_ipfs(embedding_cid)
    
    if not emb_bytes:
        print(f"❌ Falha ao baixar embeddings")
        return None
    
    try:
        embeddings = np.frombuffer(emb_bytes, dtype=np.float32)
        print(f"✅ Embeddings recebidos: {embeddings.shape}")
    except Exception as e:
        print(f"❌ Erro ao deserializar embeddings: {e}")
        return None
    
    temp_emb_path = f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy"
    np.save(temp_emb_path, embeddings)
    print(f"💾 Guardado em: {temp_emb_path}")
    
    hash_calculado = calcular_hash_vetor(documents)
    
    with node_ctx._lock:
        node_ctx.temp_vectors[cid] = {
            "embeddings": embeddings,
            "version": version,
            "hash": hash_calculado,
            "documents": documents
        }
    
    print(f"🔢 Hash: {hash_calculado[:16]}...")
    print(f"✅ Confirmação preparada")
    print(f"{'='*60}\n")
    
    return hash_calculado

def enviar_confirmacao_ao_lider(version: int, hash_vetor: str):
    """Envia hash de confirmação ao líder via PubSub"""
    try:
        mensagem = {
            "type": "version_confirmation",
            "peer_id": obter_peer_id(),
            "version": version,
            "hash": hash_vetor,
            "timestamp": datetime.now().isoformat()
        }
        
        if publicar_mensagem(mensagem):
            print(f"📤 Confirmação enviada (v{version})")
            return True
        
        return False
    
    except Exception as e:
        print(f"❌ Erro ao enviar confirmação: {e}")
        return False

# ==============================================
# COMMIT
# ==============================================

def processar_commit_lider(version: int, hash_recebido: str):
    """Peer recebe COMMIT do líder"""
    print(f"\n{'='*60}")
    print(f"📥 COMMIT recebido (v{version})")
    print(f"{'='*60}")
    
    temp_data = None
    temp_cid = None
    
    with node_ctx._lock:
        for cid, data in node_ctx.temp_vectors.items():
            if data["version"] == version and data["hash"] == hash_recebido:
                temp_data = data
                temp_cid = cid
                break
    
    if not temp_data:
        print(f"⚠️ Estrutura temporária não encontrada (v{version})")
        return False
    
    print(f"✅ Hash validado: {hash_recebido[:16]}...")
    
    vector = carregar_vetor_documentos()
    vector["documents_confirmed"] = temp_data["documents"]
    vector["version_confirmed"] = version
    vector["last_updated"] = datetime.now().isoformat()
    guardar_vetor_documentos(vector)
    
    print(f"✅ Vetor atualizado: v{version}")
    
    with node_ctx._lock:
        if temp_cid in node_ctx.temp_vectors:
            del node_ctx.temp_vectors[temp_cid]
    
    atualizar_faiss_apos_commit()
    
    print(f"{'='*60}\n")
    return True

def enviar_commit(version: int, hash_commit: str):
    """Líder envia COMMIT para todos os peers após maioria"""
    print(f"\n{'='*60}")
    print(f"📡 A enviar COMMIT (v{version})")
    print(f"{'='*60}")
    
    mensagem = {
        "type": "vector_commit",
        "version": version,
        "hash": hash_commit,
        "timestamp": datetime.now().isoformat(),
        "leader_id": obter_peer_id()
    }
    
    if publicar_mensagem(mensagem):
        print(f"✅ COMMIT enviado para todos os peers")
        print(f"{'='*60}\n")
        return True
    
    return False

# ==============================================
# PUBSUB
# ==============================================

def publicar_mensagem(mensagem: dict) -> bool:
    """Publica mensagem no canal PubSub utilizando a CLI do IPFS"""
    try:
        mensagem_json = json.dumps(mensagem)
        
        result = subprocess.run(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            input=mensagem_json.encode('utf-8'),
            capture_output=True,
            timeout=5
        )
        
        return result.returncode == 0
    
    except Exception as e:
        print(f"⚠️ Erro ao publicar mensagem: {e}")
        return False

def enviar_heartbeat():
    """Envia heartbeat (líder ou peer)"""
    if node_ctx.is_leader():
        vector = carregar_vetor_documentos()
        
        with node_ctx._lock:
            pendentes = []
            for doc_id, s in node_ctx.voting_sessions.items():
                if s['status'] == 'pending_approval':
                    pendentes.append({
                        'doc_id': doc_id,
                        'filename': s['filename'],
                        'votes_approve': len(s['votes_approve']),
                        'required_votes': s['required_votes']
                    })
        
        mensagem = {
            "type": "leader_heartbeat",
            "leader_id": obter_peer_id(),
            "term": node_ctx.current_term,
            "timestamp": datetime.now().isoformat(),
            "pending_proposals": pendentes,
            "total_confirmed": len(vector.get('documents_confirmed', [])),
            "total_peers": obter_contagem_peers()
        }
        
        publicar_mensagem(mensagem)
        registar_peer(obter_peer_id())
        
        if len(pendentes) > 0 or random.random() < 0.05:
            print(f"💓 Líder HB | Pendentes: {len(pendentes)}")
    
    else:
        mensagem = {
            "type": "peer_heartbeat",
            "peer_id": obter_peer_id(),
            "state": node_ctx.state.value,
            "timestamp": datetime.now().isoformat()
        }
        
        publicar_mensagem(mensagem)
        registar_peer(obter_peer_id())

# ==============================================
# RAFT: ELEIÇÃO COM TRACKING DE VOTOS
# ==============================================

def iniciar_eleicao():
    """Inicia eleição RAFT com tracking correto de votos"""
    print(f"\n{'='*60}")
    print("🗳️ A INICIAR ELEIÇÃO RAFT")
    print(f"{'='*60}")
    
    with node_ctx._lock:
        node_ctx.state = NodeState.CANDIDATE
        node_ctx.current_term += 1
        node_ctx.voted_for = obter_peer_id()
        node_ctx.leader_id = None
        term = node_ctx.current_term
        
        node_ctx.votes_received = {obter_peer_id()}
        node_ctx.current_election_term = term
    
    print(f"📊 Term: {term}")
    print(f"✅ Voto próprio registado")
    
    mensagem = {
        "type": "request_vote",
        "candidate_id": obter_peer_id(),
        "term": term,
        "timestamp": datetime.now().isoformat()
    }
    
    publicar_mensagem(mensagem)
    print(f"📤 Pedido de votos enviado")
    print(f"{'='*60}")
    
    for i in range(30):
        time.sleep(0.1)
        
        with node_ctx._lock:
            votos_recebidos = len(node_ctx.votes_received)
            total_peers = len(node_ctx.peers)
            votos_necessarios = (total_peers // 2) + 1
        
        if i % 5 == 0:
            print(f"⏳ Votos: {votos_recebidos}/{votos_necessarios} (de {total_peers} peers)")
        
        if votos_recebidos >= votos_necessarios:
            print(f"\n✅ MAIORIA ATINGIDA: {votos_recebidos}/{total_peers} votos")
            tornar_se_lider()
            return
    
    print(f"\n❌ Eleição falhou (timeout)")
    with node_ctx._lock:
        node_ctx.state = NodeState.FOLLOWER
        node_ctx.voted_for = None
    
    print(f"{'='*60}\n")

def processar_pedido_voto(candidate_id: str, term: int):
    """Processa pedido de voto recebido via PubSub"""
    my_id = obter_peer_id()
    
    if candidate_id == my_id:
        return
    
    with node_ctx._lock:
        if term > node_ctx.current_term:
            node_ctx.current_term = term
            node_ctx.state = NodeState.FOLLOWER
            node_ctx.voted_for = None
            node_ctx.leader_id = None
        
        if term == node_ctx.current_term and node_ctx.voted_for is None:
            node_ctx.voted_for = candidate_id
            
            mensagem = {
                "type": "vote_response",
                "voter_id": my_id,
                "candidate_id": candidate_id,
                "term": term,
                "vote_granted": True,
                "timestamp": datetime.now().isoformat()
            }
            
            publicar_mensagem(mensagem)
            print(f"✅ Voto concedido a {candidate_id[:16]}... (term {term})")

def processar_resposta_voto(voter_id: str, candidate_id: str, term: int, vote_granted: bool):
    """Processa resposta de voto recebida via PubSub"""
    my_id = obter_peer_id()
    
    if candidate_id != my_id:
        return
    
    with node_ctx._lock:
        if term != node_ctx.current_election_term:
            return
        
        if node_ctx.state != NodeState.CANDIDATE:
            return
        
        if vote_granted and voter_id not in node_ctx.votes_received:
            node_ctx.votes_received.add(voter_id)
            print(f"📥 Voto de {voter_id[:16]}... ({len(node_ctx.votes_received)} total)")

def tornar_se_lider():
    """Transição para LEADER com FastAPI automático"""
    print(f"\n{'='*70}")
    print("👑 ELEITO LÍDER!")
    print(f"{'='*70}")
    
    node_ctx.set_state(NodeState.LEADER)
    
    with node_ctx._lock:
        node_ctx.leader_id = obter_peer_id()
    
    print(f"📍 Peer ID: {node_ctx.leader_id[:40]}...")
    print(f"📊 Term: {node_ctx.current_term}")
    print(f"👥 Peers: {obter_contagem_peers()}")
    
    # ✅ FastAPI inicia automaticamente
    iniciar_servidor_http()
    
    print(f"{'='*70}\n")

# ==============================================
# FASTAPI LIFECYCLE
# ==============================================

def parar_servidor_http():
    """Para o servidor HTTP quando perde liderança"""
    if node_ctx.http_server:
        print("🛑 A parar servidor HTTP...")
        node_ctx.http_server.should_exit = True
        
        if node_ctx.http_server_thread and node_ctx.http_server_thread.is_alive():
            node_ctx.http_server_thread.join(timeout=5)
        
        node_ctx.http_server = None
        node_ctx.http_server_thread = None
        print("✅ Servidor HTTP parado")

def iniciar_servidor_http():
    """✅ Inicia servidor FastAPI automaticamente quando eleito líder"""
    if node_ctx.http_server:
        print("⚠️ Servidor HTTP já está ativo")
        return
    
    print("🚀 A iniciar servidor FastAPI...")
    
    app = criar_aplicacao_fastapi()
    
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="warning"
    )
    
    server = uvicorn.Server(config)
    node_ctx.http_server = server
    
    def run():
        try:
            server.run()
        except Exception as e:
            print(f"⚠️ Servidor HTTP erro: {e}")
    
    node_ctx.http_server_thread = threading.Thread(target=run, daemon=True)
    node_ctx.http_server_thread.start()
    
    print("✅ HTTP ativo em http://0.0.0.0:5000")
    time.sleep(1)

# ==============================================
# FASTAPI APPLICATION
# ==============================================

def criar_aplicacao_fastapi() -> FastAPI:
    """Cria aplicação FastAPI com endpoints"""
    
    app = FastAPI(
        title="IPFS Distributed System",
        description="Sistema distribuído com RAFT clássico e eleição automática",
        version="2.2"
    )
    
    @app.post("/upload")
    async def upload_file(file: UploadFile = File(...)):
        """Endpoint de upload (só disponível no líder)"""
        
        if not node_ctx.is_leader():
            return JSONResponse(
                content={
                    "error": "Este node não é o líder",
                    "leader_id": node_ctx.leader_id
                },
                status_code=403
            )
        
        try:
            filename = os.path.basename(file.filename)
            content = await file.read()
            
            print(f"\n{'='*60}")
            print(f"📤 UPLOAD RECEBIDO: {filename}")
            print(f"{'='*60}")
            
            doc_id = str(uuid.uuid4())
            total_peers = obter_contagem_peers()
            required_votes = (total_peers // 2) + 1
            
            with node_ctx._lock:
                node_ctx.voting_sessions[doc_id] = {
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
            
            temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
            with open(temp_file, 'wb') as f:
                f.write(content)
            
            mensagem = {
                "type": "document_proposal",
                "doc_id": doc_id,
                "filename": filename,
                "total_peers": total_peers,
                "required_votes": required_votes,
                "timestamp": datetime.now().isoformat(),
                "from_peer": obter_peer_id()
            }
            
            publicar_mensagem(mensagem)
            
            print(f"✅ Proposta criada: {doc_id}")
            print(f"👥 Votos necessários: {required_votes}/{total_peers}")
            print(f"{'='*60}\n")
            
            threading.Timer(0.5, lambda: votar_automaticamente(doc_id, "approve")).start()
            
            return {
                "status": "pending_approval",
                "doc_id": doc_id,
                "filename": filename,
                "required_votes": required_votes,
                "total_peers": total_peers
            }
        
        except Exception as e:
            print(f"❌ Erro no upload: {e}")
            import traceback
            traceback.print_exc()
            
            return JSONResponse(
                content={"error": str(e)},
                status_code=500
            )
    
    @app.post("/search", response_model=SearchInitResponse)
    async def start_search(req: SearchRequest):
        print("DEBUG /search chamado com:", req.prompt, req.top_k)

        if not node_ctx.is_leader():
            return JSONResponse(
                content={"error": "Este node não é o líder", "leader_id": node_ctx.leader_id},
                status_code=403,
            )

        search_id = str(uuid.uuid4())
        token = str(uuid.uuid4())
        my_id = obter_peer_id()

        with node_ctx._lock:
            peers = sorted(node_ctx.peers.keys())

            if len(peers) <= 1:
                # Só há este nó → processa localmente
                target_peer = my_id
            else:
                target_peer = peers[node_ctx.last_search_peer_index % len(peers)]
                node_ctx.last_search_peer_index += 1

            node_ctx.search_requests[search_id] = {
                "token": token,
                "peer_id": target_peer,
                "prompt": req.prompt,
                "top_k": req.top_k,
                "created_at": datetime.now().isoformat(),
            }

        print("DEBUG /search criou search_id:", search_id, "target_peer:", target_peer)

        # Modo single-node: processar logo aqui, sem PubSub
        if target_peer == my_id and len(peers) <= 1:
            threading.Thread(
                target=processar_pesquisa_faiss,
                args=(search_id, token, req.prompt, req.top_k, my_id),
                daemon=True,
            ).start()
        else:
            mensagem = {
                "type": "search_request",
                "search_id": search_id,
                "token": token,
                "prompt": req.prompt,
                "top_k": req.top_k,
                "target_peer": target_peer,
                "leader_id": my_id,
                "timestamp": datetime.now().isoformat(),
            }
            publicar_mensagem(mensagem)

        return {"id": search_id, "token": token}


    @app.get("/search/{search_id}") #obter resultados da pesquisa
    def get_search_result(search_id: str, token: str = Query(...)): #
        with node_ctx._lock:
            req = node_ctx.search_requests.get(search_id)
        # verifica se o pedido existe e se o token é válido
        if not req:
            return JSONResponse(content={"error": "ID desconhecido"}, status_code=404)

        if req["token"] != token:
            return JSONResponse(content={"error": "Token inválido"}, status_code=403)
        #
        peer_id = req["peer_id"]

        # se o próprio líder for o peer que processou
        if peer_id == obter_peer_id():
            with node_ctx._lock:
                res = node_ctx.search_results.get(search_id)
            if not res:
                return JSONResponse(content={"status": "processing"}, status_code=202)
            return {"id": search_id, "results": res["results"]}

        # pedir resultado ao peer responsável
        msg = {
            "type": "search_result_request",
            "search_id": search_id,
            "from_leader": obter_peer_id(),
            "target_peer": peer_id,
            "timestamp": datetime.now().isoformat(),
        }
        publicar_mensagem(msg)

        timeout = 5
        interval = 0.2
        waited = 0.0
        while waited < timeout:
            with node_ctx._lock:
                res = node_ctx.search_results.get(search_id)
            if res and res.get("peer_id") == peer_id:
                return {"id": search_id, "results": res["results"]}
            time.sleep(interval)
            waited += interval

        return JSONResponse(content={"status": "processing"}, status_code=202)
    
    @app.get("/status")
    def get_status():
        """Status completo do sistema"""
        
        vector = carregar_vetor_documentos()
        
        with node_ctx._lock:
            return {
                "peer_id": obter_peer_id(),
                "state": node_ctx.state.value,
                "term": node_ctx.current_term,
                "is_leader": node_ctx.is_leader(),
                "leader_id": node_ctx.leader_id,
                "total_peers": len(node_ctx.peers),
                "active_peers": list(node_ctx.peers.keys()),
                "version_confirmed": vector.get('version_confirmed', 0),
                "total_documents": len(vector.get('documents_confirmed', [])),
                "pending_votes": len(node_ctx.voting_sessions)
            }
    
    @app.get("/documents")
    def list_documents():
        """Lista todos os documentos confirmados"""
        
        vector = carregar_vetor_documentos()
        
        confirmed = []
        for doc in vector.get("documents_confirmed", []):
            confirmed.append({
                "cid": doc.get("cid"),
                "filename": doc.get("filename"),
                "added_at": doc.get("added_at"),
                "embedding_cid": doc.get("embedding_cid"),
                "has_embedding": os.path.exists(doc.get("embedding_file", ""))
            })
        
        return {
            "total_confirmed": len(confirmed),
            "documents": confirmed
        }
    
    @app.get("/download/{cid}")
    def download_file(cid: str):
        """Download de ficheiro do IPFS"""
        
        content = obter_do_ipfs(cid)
        
        if not content:
            return JSONResponse(
                content={"error": "Ficheiro não encontrado"},
                status_code=404
            )
        
        vector = carregar_vetor_documentos()
        filename = "file"
        
        for doc in vector.get("documents_confirmed", []):
            if doc.get("cid") == cid:
                filename = doc.get("filename", "file")
                break
        
        return StreamingResponse(
            iter([content]),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    
    return app



# ==============================================
# MESSAGE PROCESSING
# ==============================================

def processar_mensagem_pubsub(mensagem: dict):
    """Processa mensagens recebidas via PubSub"""
    
    msg_type = mensagem.get("type")
    my_id = obter_peer_id()
    
    if msg_type == "peer_heartbeat":
        peer_id = mensagem.get("peer_id")
        if peer_id:
            registar_peer(peer_id)
    
    elif msg_type == "leader_heartbeat":
        leader_id = mensagem.get("leader_id")
        term = mensagem.get("term", 0)
        
        with node_ctx._lock:
            if term >= node_ctx.current_term and not node_ctx.is_leader():
                node_ctx.current_term = term
                node_ctx.leader_id = leader_id
                node_ctx.last_leader_heartbeat = datetime.now()
                
                if node_ctx.state != NodeState.FOLLOWER:
                    node_ctx.set_state(NodeState.FOLLOWER)
    
    elif msg_type == "request_vote":
        candidate_id = mensagem.get("candidate_id")
        term = mensagem.get("term")
        processar_pedido_voto(candidate_id, term)
    
    elif msg_type == "vote_response":
        voter_id = mensagem.get("voter_id")
        candidate_id = mensagem.get("candidate_id")
        term = mensagem.get("term")
        vote_granted = mensagem.get("vote_granted", False)
        processar_resposta_voto(voter_id, candidate_id, term, vote_granted)
    
    elif msg_type == "document_proposal":
        doc_id = mensagem.get("doc_id")
        filename = mensagem.get("filename")
        
        with node_ctx._lock:
            if doc_id not in node_ctx.voting_sessions:
                node_ctx.voting_sessions[doc_id] = {
                    "doc_id": doc_id,
                    "filename": filename,
                    "status": "pending_approval",
                    "total_peers": mensagem.get("total_peers", 1),
                    "required_votes": mensagem.get("required_votes", 1),
                    "votes_approve": set(),
                    "votes_reject": set(),
                    "created_at": mensagem.get("timestamp")
                }
        
        if not node_ctx.is_leader():
            print(f"\n📢 PROPOSTA: {filename}")
            print(f"   Doc ID: {doc_id[:12]}...")
            print(f"   Votos necessários: {mensagem.get('required_votes')}\n")
            
            threading.Timer(0.3, lambda: votar_automaticamente(doc_id, "approve")).start()
    
    elif msg_type == "peer_vote":
        doc_id = mensagem.get("doc_id")
        peer_id = mensagem.get("peer_id")
        vote = mensagem.get("vote")
        
        if peer_id == my_id:
            return
        
        if node_ctx.add_vote(doc_id, peer_id, vote):
            if node_ctx.is_leader():
                verificar_resultado_votacao(doc_id)
    
    elif msg_type == "version_confirmation_request":
        version = mensagem.get("version")
        documents = mensagem.get("documents")
        cid = mensagem.get("cid")
        embedding_cid = mensagem.get("embedding_cid")
        
        if embedding_cid:
            hash_calculado = processar_pedido_confirmacao(version, documents, cid, embedding_cid)
            
            if hash_calculado:
                enviar_confirmacao_ao_lider(version, hash_calculado)
    
    elif msg_type == "version_confirmation":
        if node_ctx.is_leader():
            version = mensagem.get("version")
            hash_peer = mensagem.get("hash")
            peer_id = mensagem.get("peer_id")
            
            with node_ctx._lock:
                if version not in node_ctx.version_confirmations:
                    node_ctx.version_confirmations[version] = (set(), datetime.now())
                
                peers_set, timestamp = node_ctx.version_confirmations[version]
                peers_set.add((peer_id, hash_peer))
                node_ctx.version_confirmations[version] = (peers_set, timestamp)
            
            confirmacoes = len(peers_set)
            total_peers = obter_contagem_peers()
            maioria = (total_peers // 2) + 1
            
            print(f"✅ Confirmação recebida: v{version} ({confirmacoes}/{maioria})")
            
            if confirmacoes >= maioria:
                enviar_commit(version, hash_peer)
    
    elif msg_type == "vector_commit":
        version = mensagem.get("version")
        hash_commit = mensagem.get("hash")
        
        processar_commit_lider(version, hash_commit)
    
    elif msg_type == "document_approved":
        doc_id = mensagem.get("doc_id")
        filename = mensagem.get("filename")
        print(f"\n✅ DOCUMENTO APROVADO: {filename}\n")
    
    elif msg_type == "document_rejected":
        doc_id = mensagem.get("doc_id")
        filename = mensagem.get("filename")
        print(f"\n❌ DOCUMENTO REJEITADO: {filename}\n")
    
    elif msg_type == "search_request":
        print("DEBUG: search_request recebido:", mensagem)
        target_peer = mensagem.get("target_peer")
        myid = obter_peer_id()
        if target_peer and target_peer != myid:
            return  # esta pesquisa é para outro peer

        search_id = mensagem.get("search_id")
        token = mensagem.get("token")
        prompt = mensagem.get("prompt")
        top_k = mensagem.get("top_k", 5)
        leader_id = mensagem.get("leader_id")

        threading.Thread(
            target=processar_pesquisa_faiss,
            args=(search_id, token, prompt, top_k, leader_id),
            daemon=True,
        ).start()

    elif msg_type == "search_result_ready":
        search_id = mensagem.get("search_id")
        peer_id = mensagem.get("peer_id")
        print(f"[SEARCH] Resultado {search_id} pronto no peer {peer_id}")
    
    elif msg_type == "search_result_request":
        target_peer = mensagem.get("target_peer")
        myid = obter_peer_id()
        if target_peer != myid:
            return
        search_id = mensagem.get("search_id")
        with node_ctx._lock:
            res = node_ctx.search_results.get(search_id)
        if not res:
            return
        resposta = {
            "type": "search_result_response",
            "search_id": search_id,
            "peer_id": myid,
            "results": res.get("results", []),
            "timestamp": datetime.now().isoformat(),
        }
        publicar_mensagem(resposta)

    elif msg_type == "search_result_response":
        search_id = mensagem.get("search_id")
        peer_id = mensagem.get("peer_id")
        results = mensagem.get("results", [])
        with node_ctx._lock:
            # mantém token original se existir
            token = None
            if search_id in node_ctx.search_requests:
                token = node_ctx.search_requests[search_id]["token"]
            node_ctx.search_results[search_id] = {
                "token": token,
                "results": results,
                "peer_id": peer_id,
                "created_at": datetime.now().isoformat(),
            }


def processar_pesquisa_faiss(search_id: str, token: str, prompt: str, top_k: int, leader_id: str):
    try:
        import faiss
    except ImportError:
        print("FAISS não disponível neste peer")
        return

    print(f"[SEARCH] A processar pesquisa {search_id}...")

    if not os.path.exists("faiss_index.faiss"):
        print("Índice FAISS não encontrado")
        results = []
    else:
        try:
            index = faiss.read_index("faiss_index.faiss")
        except Exception as e:
            print("Erro ao ler índice FAISS", e)
            results = []
        else:
            # embedding da prompt
            query_emb = embedding_model.encode(prompt, convert_to_numpy=True).astype("float32")
            query_emb = np.expand_dims(query_emb, axis=0)

            distances, indices = index.search(query_emb, top_k)  # k vizinhos mais próximos[web:15]
            distances = distances[0].tolist()
            indices = indices[0].tolist()

            vector = carregar_vetor_documentos()
            docs = vector.get("documents_confirmed", [])

            hits = []
            for rank, idx in enumerate(indices):
                if idx < 0 or idx >= len(docs):
                    continue
                doc = docs[idx]
                hits.append({
                    "rank": rank + 1,
                    "distance": float(distances[rank]),
                    "cid": doc.get("cid"),
                    "filename": doc.get("filename"),
                    "added_at": doc.get("added_at"),
                })
            results = hits

    with node_ctx._lock:
        node_ctx.search_results[search_id] = {
            "token": token,
            "results": results,
            "peer_id": obter_peer_id(),
            "created_at": datetime.now().isoformat(),
        }

    mensagem = {
        "type": "search_result_ready",
        "search_id": search_id,
        "peer_id": obter_peer_id(),
        "timestamp": datetime.now().isoformat(),
    }
    publicar_mensagem(mensagem)


def verificar_resultado_votacao(doc_id: str):
    """Verifica se votação atingiu maioria (só líder)"""
    
    if not node_ctx.is_leader():
        return
    
    with node_ctx._lock:
        if doc_id not in node_ctx.voting_sessions:
            return
        
        session = node_ctx.voting_sessions[doc_id]
        
        if session["status"] != "pending_approval":
            return
        
        approve = len(session["votes_approve"])
        reject = len(session["votes_reject"])
        required = session["required_votes"]
        
        print(f"📊 Votação: A favor={approve} | Contra={reject} | Necessários={required}")
        
        if approve >= required:
            finalizar_documento_aprovado(doc_id)
        elif reject >= required:
            finalizar_documento_rejeitado(doc_id)

def votar_automaticamente(doc_id: str, vote_type: str):
    """Voto automático com processamento local primeiro"""
    my_id = obter_peer_id()
    
    voto_registado = node_ctx.add_vote(doc_id, my_id, vote_type)
    
    if voto_registado:
        print(f"✅ Voto registado: {vote_type.upper()}")
        
        if node_ctx.is_leader():
            verificar_resultado_votacao(doc_id)
    
    mensagem = {
        "type": "peer_vote",
        "doc_id": doc_id,
        "vote": vote_type,
        "peer_id": my_id,
        "timestamp": datetime.now().isoformat()
    }
    
    publicar_mensagem(mensagem)

# ==============================================
# FINALIZATION
# ==============================================

def finalizar_documento_aprovado(doc_id: str):
    """Finaliza documento aprovado"""
    
    with node_ctx._lock:
        if doc_id not in node_ctx.voting_sessions:
            return
        
        session = node_ctx.voting_sessions[doc_id]
        session["status"] = "approved"
        
        filename = session["filename"]
        content = session["content"]
    
    print(f"\n{'='*60}")
    print(f"✅ DOCUMENTO APROVADO: {filename}")
    print(f"{'='*60}")
    
    cid = adicionar_ao_ipfs(content, filename)
    if not cid:
        print("❌ Falha ao adicionar ao IPFS")
        return
    
    print(f"📦 CID: {cid}")
    
    try:
        text = content.decode('utf-8')
    except:
        text = f"Document: {filename}"
    
    embeddings = embedding_model.encode(text, convert_to_numpy=True)
    print(f"🧠 Embeddings: {embeddings.shape}")
    
    emb_bytes = embeddings.tobytes()
    embedding_cid = adicionar_ao_ipfs(emb_bytes, f"{cid}_embeddings.bin")
    
    if not embedding_cid:
        print("❌ Falha ao adicionar embeddings ao IPFS")
        return
    
    print(f"🧠 Embeddings CID: {embedding_cid[:16]}...")
    
    np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings)
    
    vector = carregar_vetor_documentos()
    nova_versao = vector.get("version_confirmed", 0) + 1
    
    doc_entry = {
        "cid": cid,
        "filename": filename,
        "added_at": datetime.now().isoformat(),
        "embedding_cid": embedding_cid,
        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy"
    }
    
    vector["documents_confirmed"].append(doc_entry)
    vector["version_confirmed"] = nova_versao
    vector["last_updated"] = datetime.now().isoformat()
    guardar_vetor_documentos(vector)
    
    print(f"\n📤 A solicitar confirmações (v{nova_versao})...")
    
    mensagem_confirmacao = {
        "type": "version_confirmation_request",
        "version": nova_versao,
        "documents": vector["documents_confirmed"],
        "cid": cid,
        "embedding_cid": embedding_cid,
        "timestamp": datetime.now().isoformat()
    }
    
    publicar_mensagem(mensagem_confirmacao)
    
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    print(f"✅ Processamento completo (v{nova_versao})")
    print(f"{'='*60}\n")
    
    mensagem_aprovacao = {
        "type": "document_approved",
        "doc_id": doc_id,
        "filename": filename,
        "cid": cid,
        "embedding_cid": embedding_cid,
        "version": nova_versao,
        "votes_approve": len(session["votes_approve"]),
        "votes_reject": len(session["votes_reject"]),
        "timestamp": datetime.now().isoformat()
    }
    
    publicar_mensagem(mensagem_aprovacao)

def finalizar_documento_rejeitado(doc_id: str):
    """Finaliza documento rejeitado"""
    
    with node_ctx._lock:
        if doc_id not in node_ctx.voting_sessions:
            return
        
        session = node_ctx.voting_sessions[doc_id]
        session["status"] = "rejected"
        filename = session["filename"]
    
    print(f"\n❌ DOCUMENTO REJEITADO: {filename}\n")
    
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    mensagem = {
        "type": "document_rejected",
        "doc_id": doc_id,
        "filename": filename,
        "votes_approve": len(session["votes_approve"]),
        "votes_reject": len(session["votes_reject"]),
        "timestamp": datetime.now().isoformat()
    }
    
    publicar_mensagem(mensagem)

# ==============================================
# THREADS
# ==============================================

def listener_pubsub():
    """Thread que escuta mensagens do canal PubSub"""
    print("📡 A conectar ao PubSub...")
    
    while node_ctx.running:
        try:
            process = subprocess.Popen(
                ['ipfs', 'pubsub', 'sub', CANAL_PUBSUB],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            print(f"✅ Conectado ao canal '{CANAL_PUBSUB}'")
            
            for line in iter(process.stdout.readline, ''):
                if not node_ctx.running:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    mensagem = json.loads(line)
                    processar_mensagem_pubsub(mensagem)
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"⚠️ Erro ao processar mensagem: {e}")
            
            process.kill()
        
        except Exception as e:
            if node_ctx.running:
                print(f"⚠️ Listener erro: {e}")
                time.sleep(5)

def monitor_lider():
    """
    ✅ Thread que monitora heartbeat do líder E inicia eleição inicial
    CORRIGIDO: Funciona mesmo sem heartbeat prévio
    """
    print("🔍 Monitor do líder iniciado")
    
    while node_ctx.running:
        time.sleep(5)
        
        # Líder não monitora a si próprio
        if node_ctx.is_leader():
            continue
        
        with node_ctx._lock:
            # Só FOLLOWERS monitorizam
            if node_ctx.state != NodeState.FOLLOWER:
                continue
            
            now = datetime.now()
            
            # ✅ FIX: Eleição inicial automática (sem líder após 10-15s)
            if node_ctx.last_leader_heartbeat is None:
                tempo_desde_startup = (now - node_ctx.startup_time).total_seconds()
                
                # Timeout aleatório para evitar split-vote
                timeout_inicial = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
                
                if tempo_desde_startup > timeout_inicial:
                    print(f"\n{'='*60}")
                    print(f"🗳️ TIMEOUT INICIAL ({int(tempo_desde_startup)}s)")
                    print("📢 Nenhum líder detectado, a iniciar eleição...")
                    print(f"{'='*60}\n")
                    
                    iniciar_eleicao()
                
                continue
            
            # ✅ Eleição por crash de líder (com heartbeat prévio)
            tempo_sem_heartbeat = (now - node_ctx.last_leader_heartbeat).total_seconds()
            
            if tempo_sem_heartbeat > LEADER_TIMEOUT:
                print(f"\n{'='*60}")
                print(f"🚨 LÍDER CRASHOU! (timeout: {int(tempo_sem_heartbeat)}s)")
                print(f"{'='*60}\n")
                
                iniciar_eleicao()
                
                # Reset para não retriggerar
                node_ctx.last_leader_heartbeat = None

def loop_heartbeats():
    """Thread que envia heartbeats periódicos"""
    print("💓 Loop de heartbeats iniciado")
    
    while node_ctx.running:
        enviar_heartbeat()
        time.sleep(LEADER_HEARTBEAT_INTERVAL)

# ==============================================
# SIGNAL HANDLER
# ==============================================

def signal_handler(sig, frame):
    """Handler para Ctrl+C"""
    print("\n\n🔌 A encerrar sistema...")
    node_ctx.running = False
    
    if node_ctx.http_server:
        parar_servidor_http()
    
    print("✅ Encerrado")
    sys.exit(0)

# ==============================================
# MAIN
# ==============================================

def main():
    signal.signal(signal.SIGINT, signal_handler)
    
    print("\n" + "="*70)
    print("🚀 SISTEMA DISTRIBUÍDO IPFS + FAISS + RAFT v2.2")
    print("="*70)
    
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=5)
        if response.status_code != 200:
            print("❌ IPFS não está acessível")
            sys.exit(1)
        
        node_ctx.peer_id = response.json()['ID']
        print(f"\n✅ IPFS conectado")
        print(f"📍 Peer ID: {node_ctx.peer_id[:40]}...")
    
    except Exception as e:
        print(f"❌ Erro: {e}")
        print("💡 Certifica-te: ipfs daemon --enable-pubsub-experiment")
        sys.exit(1)
    
    configurar_ipfs_mdns()
    
    print(f"\n🔵 Estado inicial: FOLLOWER")
    print(f"⏳ Eleição automática em {ELECTION_TIMEOUT_MIN}-{ELECTION_TIMEOUT_MAX}s se sem líder...")
    
    threads = [
        threading.Thread(target=listener_pubsub, daemon=True, name="PubSub"),
        threading.Thread(target=monitor_lider, daemon=True, name="Monitor-Líder"),
        threading.Thread(target=loop_heartbeats, daemon=True, name="Heartbeats"),
        threading.Thread(target=garbage_collector, daemon=True, name="GC")
    ]
    
    for t in threads:
        t.start()
    
    print(f"\n{'='*70}")
    print("✅ SISTEMA ATIVO")
    print("="*70)
    print("\nComandos:")
    print("  status  - Ver estado do node")
    print("  peers   - Ver peers ativos")
    print("  docs    - Ver documentos")
    print("  quit    - Encerrar")
    print("="*70 + "\n")
    
    try:
        while node_ctx.running:
            try:
                comando = input(">>> ").strip().lower()
            except EOFError:
                break
            
            if comando == "quit":
                break
            
            elif comando == "status":
                with node_ctx._lock:
                    print(f"\n{'='*60}")
                    print("STATUS DO NODE")
                    print(f"{'='*60}")
                    print(f"Peer ID: {obter_peer_id()[:40]}...")
                    print(f"Estado: {node_ctx.state.value.upper()}")
                    print(f"Term: {node_ctx.current_term}")
                    
                    if node_ctx.is_leader():
                        print(f"Líder: SIM (este node)")
                    else:
                        print(f"Líder: {node_ctx.leader_id[:40] if node_ctx.leader_id else 'Nenhum'}...")
                    
                    print(f"Peers ativos: {len(node_ctx.peers)}")
                    
                    vector = carregar_vetor_documentos()
                    print(f"Documentos: {len(vector.get('documents_confirmed', []))}")
                    print(f"Sessões votação: {len(node_ctx.voting_sessions)}")
                    print(f"{'='*60}\n")
            
            elif comando == "peers":
                with node_ctx._lock:
                    print(f"\n{'='*60}")
                    print(f"PEERS ATIVOS ({len(node_ctx.peers)})")
                    print(f"{'='*60}")
                    for peer_id in list(node_ctx.peers.keys())[:10]:
                        print(f"  🔗 {peer_id[:40]}...")
                    print(f"{'='*60}\n")
            
            elif comando == "docs":
                vector = carregar_vetor_documentos()
                docs = vector.get('documents_confirmed', [])
                
                print(f"\n{'='*60}")
                print(f"DOCUMENTOS CONFIRMADOS ({len(docs)})")
                print(f"{'='*60}")
                
                for i, doc in enumerate(docs[:10], 1):
                    print(f"\n{i}. {doc.get('filename')}")
                    print(f"   CID: {doc.get('cid')}")
                    print(f"   Data: {doc.get('added_at', 'N/A')[:19]}")
                
                print(f"\n{'='*60}\n")
            
            else:
                print("❌ Comando desconhecido\n")
    
    except KeyboardInterrupt:
        pass
    
    print("\n🔌 A encerrar...")
    node_ctx.running = False
    
    if node_ctx.http_server:
        parar_servidor_http()
    
    print("✅ Sistema encerrado")
    sys.exit(0)

if __name__ == "__main__":
    main()
