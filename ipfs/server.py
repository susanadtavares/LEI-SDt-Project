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

# Cria a aplicação FastAPI
app = FastAPI(title="IPFS Upload API")

# URL base para a API do IPFS (gateway local)
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
# Nome do canal PubSub onde os peers comunicam entre si
CANAL_PUBSUB = "canal-ficheiros"

# Caminhos e ficheiros usados para guardar metadados e embeddings
VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"
PENDING_UPLOADS_DIR = "pending_uploads"

# Garante que as pastas existem
Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(PENDING_UPLOADS_DIR).mkdir(exist_ok=True)

print("A carregar modelo SentenceTransformer...")
# Modelo de embeddings usado para transformar texto em vetores numéricos
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("Modelo carregado com sucesso!")

# Dicionário em memória para guardar sessões de votação ativas
voting_sessions: Dict[str, dict] = {}
# Peer ID deste nó (vai ser obtido do IPFS)
my_peer_id = None

# Track dos peers que estão a usar ESTE sistema (não todos os peers do IPFS global)
peers: Dict[str, datetime] = {}
# Tempo máximo de inatividade para remover peers (30 segundos)
PEER_TIMEOUT = timedelta(seconds=30)

LEADER_HEARTBEAT_INTERVAL = 5  # Envia a cada 5 segundos

def get_my_peer_id():
    """
    Obtém e guarda em cache o Peer ID deste nó através da API do IPFS.
    Se já tiver sido obtido, reutiliza o valor.
    """
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
    """
    Regista/atualiza um peer no dicionário local de peers, marcando a hora a que foi visto pela última vez.
    """
    peers[peer_id] = datetime.now()
    cleanup_inactive_peers()

def cleanup_inactive_peers():
    """
    Remove peers que já não são vistos há mais de PEER_TIMEOUT. Isto evita acumulação de peers "fantasma".
    """
    now = datetime.now()
    inactive = [pid for pid, last_seen in peers.items() 
                if now - last_seen > PEER_TIMEOUT]
    for pid in inactive:
        del peers[pid]
        print(f"⚠️  Peer removido por inatividade: {pid[:16]}...")

def get_peers_count():
    """
    Devolve o número de peers ativos no sistema. Garante também que o próprio peer está registado.
    """
    cleanup_inactive_peers()
    
    my_id = get_my_peer_id()
    if my_id not in peers:
        register_peer(my_id)
    
    count = len(peers)
    return count

####### HEARTBEAT
def broadcast_heartbeat():
    """
    Envia um 'heartbeat' via PubSub para informar os outros peers que este nó continua ativo.
    """
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
        # Se falhar, ignoramos silenciosamente
        pass
    
    ##BROADCAST DE 5 EM 5 SEGUNDOS DO LEADER
    
def broadcast_leader_heartbeat():
    """Envia heartbeat do líder com estado agregado do sistema (usando CLI)."""
    try:
        vector = load_document_vector()
        
        # Agrega propostas pendentes
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
        
        # USA CLI em vez de HTTP API!
        import subprocess
        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = process.communicate(input=message_json.encode('utf-8'), timeout=5)
        
        if process.returncode == 0:
            print(f"💓 Heartbeat líder enviado | Pendentes: {len(pending_proposals)}")
        else:
            print(f"⚠️ Erro ao enviar heartbeat líder")
    
    except Exception as e:
        print(f"⚠️ Erro ao enviar heartbeat líder: {e}")


def leader_heartbeat_loop():
    """
    Thread que envia heartbeats do líder periodicamente.
    Agrupa estado do sistema em blocos enviados a cada 5 segundos.
    """
    print("🔄 Leader heartbeat loop iniciado")
    
    while True:
        try:
            broadcast_leader_heartbeat()
            time.sleep(LEADER_HEARTBEAT_INTERVAL)
        except Exception as e:
            print(f"❌ Erro no heartbeat loop: {e}")
            time.sleep(LEADER_HEARTBEAT_INTERVAL)


def load_document_vector():
    """
    Carrega o ficheiro JSON que guarda o vetor de documentos (metadados), ou cria uma estrutura nova se não existir.
    """
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
    """
    Guarda em disco o vetor de documentos (metadados) no ficheiro JSON.
    """
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)

def create_voting_session(doc_id, filename, content):
    """
    Cria uma nova sessão de votação para um documento recém-carregado.
    - Calcula o número de votos necessários (50% + 1 dos peers ativos)
    - Guarda o conteúdo temporariamente numa pasta de 'pending_uploads'
    """
    total_peers = get_peers_count()
    required_votes = (total_peers // 2) + 1  # 50% + 1
    
    voting_sessions[doc_id] = {
        "doc_id": doc_id,
        "filename": filename,
        "content": content,
        "status": "pending_approval",
        "total_peers": total_peers,
        "required_votes": required_votes,
        "votes_approve": set(),   # conjunto de peer_ids que aprovaram
        "votes_reject": set(),    # conjunto de peer_ids que rejeitaram
        "created_at": datetime.now().isoformat(),
        "decided_at": None,
        "final_decision": None
    }
    
    # Guarda o ficheiro no disco até a votação terminar
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    with open(temp_file, 'wb') as f:
        f.write(content)
    
    return voting_sessions[doc_id]

def process_vote(doc_id, peer_id, vote_type):
    """
    Processa um voto de um peer (approve/reject) para um determinado documento.
    - Atualiza a sessão de votação
    - Verifica se já foi atingida a maioria necessária
    - Se sim, finaliza a votação (aprova ou rejeita)
    """
    if doc_id not in voting_sessions:
        return {"status": "error", "message": "Sessão de votação não encontrada"}
    
    session = voting_sessions[doc_id]
    
    if session["status"] != "pending_approval":
        return {"status": "error", "message": "Votação já encerrada"}
    
    # Garante que um peer só tem um voto válido (remove votos anteriores)
    session["votes_approve"].discard(peer_id)
    session["votes_reject"].discard(peer_id)
    
    # Regista o novo voto
    if vote_type == "approve":
        session["votes_approve"].add(peer_id)
    elif vote_type == "reject":
        session["votes_reject"].add(peer_id)
    
    approve_count = len(session["votes_approve"])
    reject_count = len(session["votes_reject"])
    required = session["required_votes"]
    
    decision = None
    
    # Maioria de aprovação alcançada
    if approve_count >= required:
        decision = "approved"
        session["status"] = "approved"
        session["final_decision"] = "approved"
        session["decided_at"] = datetime.now().isoformat()
        
        # Finaliza o documento aprovado (envia para IPFS, cria embedding, etc.)
        result = finalize_approved_document(doc_id)
        return result
    
    # Maioria de rejeição alcançada
    elif reject_count >= required:
        decision = "rejected"
        session["status"] = "rejected"
        session["final_decision"] = "rejected"
        session["decided_at"] = datetime.now().isoformat()
        
        # Remove o ficheiro temporário e regista rejeição
        finalize_rejected_document(doc_id)
        
        # Propaga a decisão de rejeição aos outros peers
        propagate_decision(doc_id, "rejected")
        
        return {
            "status": "rejected",
            "message": "Documento rejeitado pela maioria",
            "votes_approve": approve_count,
            "votes_reject": reject_count,
            "required_votes": required
        }
    
    # Caso ainda não haja maioria, mantemos a votação aberta
    return {
        "status": "voting",
        "message": "Voto registado, a aguardar mais votos",
        "votes_approve": approve_count,
        "votes_reject": reject_count,
        "required_votes": required,
        "votes_remaining": required - max(approve_count, reject_count)
    }

def finalize_approved_document(doc_id):
    """
    Quando um documento é aprovado:
    - Lê o ficheiro temporário
    - Faz upload para o IPFS (pin=True)
    - Extrai texto e cria embedding
    - Atualiza o vetor de documentos
    - Guarda o embedding em ficheiro .npy
    - Remove o ficheiro temporário
    - Propaga a decisão de aprovação aos outros peers
    """
    session = voting_sessions[doc_id]
    filename = session["filename"]
    
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    with open(temp_file, 'rb') as f:
        content = f.read()
    
    try:
        # Envia o ficheiro para o IPFS
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
        
        # Cria embeddings a partir do conteúdo textual do documento
        text_content = extract_text_from_file(content, filename)
        embeddings = generate_embeddings(text_content)
        
        # Atualiza o vetor de documentos com o novo documento confirmado
        new_version, updated_vector = add_document_to_vector(cid, filename, embeddings, confirmed=True)
        
        # Guarda o embedding num ficheiro .npy
        np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings)
        
        # Remove o ficheiro temporário
        os.remove(temp_file)
        
        # Notifica os outros peers da decisão
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
    """
    Quando um documento é rejeitado:
    - Remove o ficheiro temporário (se existir)
    - Regista a rejeição no vetor de documentos
    """
    session = voting_sessions[doc_id]
    filename = session["filename"]
    temp_file = f"{PENDING_UPLOADS_DIR}/{doc_id}_{filename}"
    
    if os.path.exists(temp_file):
        os.remove(temp_file)
    
    # Adiciona informação da rejeição ao vetor
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
    """
    Adiciona um documento ao vetor de metadados.
    - Atualiza versões (confirmed/pending)
    - Regista a informação básica e o caminho do ficheiro de embedding
    """
    vector = load_document_vector()
    
    vector.setdefault("version_confirmed", 0)
    vector.setdefault("version_pending", vector["version_confirmed"])
    vector.setdefault("documents_confirmed", [])
    vector.setdefault("documents_pending", [])
    
    # Nova versão pendente (incrementa)
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
    """
    Usa o modelo SentenceTransformer para gerar um vetor numérico (embedding) a partir do texto fornecido.
    """
    try:
        embedding = embedding_model.encode(text_content, convert_to_numpy=True)
        return embedding
    except Exception as e:
        print(f"Erro ao criar embedding: {e}")
        raise

def extract_text_from_file(content, filename):
    """
    Extrai texto do ficheiro.
    - Tenta decodificar como UTF-8.
    - Se falhar, devolve apenas uma string genérica com o nome do ficheiro.
    """
    try:
        text = content.decode('utf-8')
        return text
    except UnicodeDecodeError:
        # Para binários, imagens, PDFs, etc., ficamos só com um "placeholder"
        return f"Document: {filename}"

def propagate_proposal(doc_id, filename, cid=None):
    """
    Propaga uma nova proposta de documento (novo upload) para todos os peers.
    Envia uma mensagem via PubSub com tipo 'document_proposal'.
    """
    try:
        session = voting_sessions[doc_id]

        # obter total confirmado atual
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

        # inclui cid se disponível (cliente pode ter adicionado ao IPFS)
        if cid:
            message_data["cid"] = cid
        
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
    """
    Propaga a decisão final sobre um documento (approved/rejected) aos outros peers.
    Se aprovado, pode incluir o CID, a versão do vetor e o embedding.
    """
    try:
        session = voting_sessions.get(doc_id)
        if not session:
            return False

        # obter total confirmado atual
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
        
        # Se for approved, envia também os dados necessários para replicar o embedding
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
    """
    Propaga um voto local (approve/reject) para os outros peers via PubSub.
    Isto permite que todos tenham uma visão consistente dos votos.
    """
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
    """
    Endpoint raiz: devolve o estado geral da API, versões, contagem de documentos e peers ativos, bem como a lista de endpoints disponíveis.
    """
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
    """
    Endpoint para upload de ficheiros:
    - Cria uma sessão de votação para o novo documento
    - Guarda temporariamente o ficheiro
    - Propaga a proposta aos outros peers via PubSub
    """
    try:
        filename = file.filename
        content = await file.read()
        
        print(f"\n{'='*60}")
        print(f"NOVO UPLOAD RECEBIDO: {filename}")
        print(f"{'='*60}")
        
        # Gera um ID único para este documento
        doc_id = str(uuid.uuid4())
        
        # Cria a sessão de votação
        session = create_voting_session(doc_id, filename, content)
        
        print(f"📋 Sessão de votação criada")
        print(f"   └─ Doc ID: {doc_id}")
        print(f"   └─ Peers no sistema: {session['total_peers']}")
        print(f"   └─ Votos necessários: {session['required_votes']}")
        
        # Envia a proposta aos outros peers
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



@app.get("/voting-status")
def get_all_voting_status():
    """
    Endpoint que devolve o estado de TODAS as sessões de votação ativas.
    Útil para debugging ou monitorização do sistema.
    """
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
    """
    Endpoint que devolve o estado de votação de um documento específico.
    Se não existir sessão em memória, tenta reconstruir a partir de ficheiro em pending_uploads.
    """
    # Se já existe sessão, devolve normalmente
    if doc_id in voting_sessions:
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

    # Tenta reconstruir sessão a partir de ficheiro pendente (caso o listener tenha gravado o ficheiro mas não criado sessão)
    try:
        matches = [fn for fn in os.listdir(PENDING_UPLOADS_DIR) if fn.startswith(f"{doc_id}_")]
        if matches:
            # pega no primeiro ficheiro que combine com doc_id
            fname = matches[0]
            temp_path = os.path.join(PENDING_UPLOADS_DIR, fname)
            # extrai filename original
            orig_filename = "_".join(fname.split("_")[1:])
            with open(temp_path, 'rb') as rf:
                content = rf.read()
            # cria sessão manual (sem sobrescrever ficheiro)
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
            print(f"🔁 Sessão reconstruída a partir de ficheiro pendente: {temp_path} (doc_id={doc_id})")
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
    except Exception as e:
        print(f"⚠️ Erro ao tentar reconstruir sessão para {doc_id}: {e}")

    return JSONResponse(
        content={"error": "Sessão de votação não encontrada"},
        status_code=404
    )

@app.get("/vector")
def get_document_vector():
    """
    Endpoint para ver um resumo do vetor de documentos:
    - Versão confirmada
    - Totais de confirmados, rejeitados e em votação
    - Lista de documentos confirmados e rejeitados
    """
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
    """
    Endpoint que devolve informação detalhada sobre um ficheiro no IPFS:
    - Estatísticas IPFS (object/stat)
    - Se existe embedding
    - Metadados no vetor de documentos
    - URLs úteis (gateway, download, embedding)
    """
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
        
        # Procura o documento no vetor de confirmados
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
    """
    Endpoint para descarregar um ficheiro a partir do IPFS, usando o CID.
    - Tenta recuperar o nome original do ficheiro a partir do vetor de documentos
    """
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
        
        # Tenta obter o nome do ficheiro a partir do vetor
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
    """
    Endpoint que devolve o embedding completo de um documento:
    - Vetor (lista de floats)
    - Dimensão e shape
    - Estatísticas básicas (mean, std, min, max, norm)
    - Metadados do documento
    """
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
    """
    Endpoint que lista:
    - Documentos confirmados (com URLs úteis)
    - Documentos ainda em votação
    - Documentos rejeitados
    """
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
    """
    Endpoint simples de pesquisa por nome de ficheiro (substring case-insensitive) apenas em documentos confirmados.
    """
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
    """
    Endpoint que remove um documento do vetor de metadados (e o embedding).
    NOTA: Não remove o ficheiro do IPFS, apenas do índice local.
    """
    try:
        vector = load_document_vector()
        
        original_count = len(vector.get('documents_confirmed', []))
        vector['documents_confirmed'] = [
            doc for doc in vector.get('documents_confirmed', [])
            if doc.get('cid') != cid
        ]
        
        if len(vector['documents_confirmed']) < original_count:
            save_document_vector(vector)
            
            # Remove o ficheiro de embedding se existir
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
    """
    Endpoint para verificar o estado de ligação ao IPFS e o estado geral do sistema:
    - Versão do IPFS
    - Peer ID
    - Número de peers
    - Versão do vetor e número de documentos
    """
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
        # Se não conseguir falar com o IPFS, devolve disconnected
        return JSONResponse(
            content={'status': 'disconnected', 'message': 'IPFS não acessível'},
            status_code=503
        )

@app.get("/peers")
def get_peers():
    """
    Endpoint que devolve a lista de peers conhecidos pelo sistema, com a indicação se ainda são considerados ativos.
    """
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
    """
    Endpoint SSE (Server-Sent Events) que faz subscribe ao canal PubSub do IPFS e vai enviando ao cliente eventos em tempo real:
    - Conexão estabelecida
    - Propostas de documentos
    - Votos de peers
    - Decisões de aprovação/rejeição
    - Heartbeats de peers
    """
    async def event_stream():
        try:
            print(f"📡 Cliente conectado ao stream")
            
            # Regista este peer e envia um heartbeat inicial
            my_id = get_my_peer_id()
            register_peer(my_id)
            broadcast_heartbeat()
            
            # Faz subscribe ao canal PubSub
            response = requests.post(
                f"{IPFS_API_URL}/pubsub/sub",
                params={'arg': CANAL_PUBSUB},
                stream=True,
                timeout=None
            )
            
            # Envia um evento inicial ao cliente
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Conectado', 'canal': CANAL_PUBSUB, 'peer_id': my_id})}\n\n"
            
            # Itera sobre as linhas de mensagens recebidas do PubSub
            for line in response.iter_lines():
                if line:
                    try:
                        msg = json.loads(line)
                        data_encoded = msg.get('data', '')
                        
                        # As mensagens do PubSub vêm em base64 -> decodificamos
                        try:
                            data_decoded = base64.b64decode(data_encoded).decode('utf-8')
                        except:
                            data_decoded = data_encoded
                        
                        try:
                            # Tenta interpretar a mensagem como JSON
                            message_obj = json.loads(data_decoded)
                            msg_type = message_obj.get('type')
                            
                            # Heartbeat de peers: apenas atualiza o peer e não envia evento ao cliente
                            if msg_type == 'peer_heartbeat':
                                peer_id = message_obj.get('peer_id')
                                if peer_id:
                                    register_peer(peer_id)
                                continue
                            
                            # Proposta de novo documento
                            elif msg_type == 'document_proposal':
                                doc_id = message_obj['doc_id']
                                filename = message_obj['filename']
                                
                                # Se ainda não temos esta sessão, cria uma sessão "remota"
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
                                # Envia evento SSE ao cliente
                                yield f"data: {json.dumps(notification)}\n\n"
                                print(f"📩 Nova proposta recebida: {filename}")
                            
                            # Voto de um peer noutro nó
                            elif msg_type == 'peer_vote':
                                doc_id = message_obj['doc_id']
                                vote = message_obj['vote']
                                peer_id = message_obj['peer_id']
                                
                                if doc_id in voting_sessions:
                                    # Atualiza a sessão local com o voto remoto
                                    result = process_vote(doc_id, peer_id, vote)
                                    
                                    notification = {
                                        'type': 'peer_vote',
                                        'doc_id': doc_id,
                                        'vote': vote,
                                        'peer_id': peer_id[:20],
                                        'result': result
                                    }
                                    yield f"data: {json.dumps(notification)}\n\n"
                            
                            # Documento aprovado noutro peer
                            elif msg_type == 'document_approved':
                                doc_id = message_obj['doc_id']
                                cid = message_obj.get('cid')
                                
                                # Se recebemos os embeddings, guardamos localmente e atualizamos o vetor
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
                                
                                # Remove a sessão de votação local (se existir)
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
                            
                            # Documento rejeitado noutro peer
                            elif msg_type == 'document_rejected':
                                doc_id = message_obj['doc_id']
                                
                                # Remove a sessão de votação local (se existir)
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
                            # Se não for JSON válido, ignoramos
                            continue
                    
                    except Exception as e:
                        print(f"⚠️  Erro: {e}")
                        continue
        
        except Exception as e:
            print(f"❌ Erro no stream: {e}")
            # Em caso de erro no stream, envia um evento de erro ao cliente
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
    
    # Devolve o StreamingResponse com o tipo 'text/event-stream' (SSE)
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

def pubsub_bg_listener():
    """
    Listener em background que subscreve o canal PubSub do IPFS e processa mensagens
    para criar sessões de votação remotamente (ex.: propostas enviadas por clientes via PubSub).
    """
    try:
        print("📡 [BG] A conectar ao canal PubSub (background)...")
        resp = requests.post(
            f"{IPFS_API_URL}/pubsub/sub",
            params={'arg': CANAL_PUBSUB},
            stream=True,
            timeout=None
        )
        print("✅ [BG] Subscrito ao PubSub (background)")
        for line in resp.iter_lines():
            if not line:
                continue
            try:
                # decode safe
                try:
                    raw = line.decode('utf-8', errors='ignore').strip()
                except Exception:
                    raw = None
                if not raw:
                    continue

                # Log envelope snippet for debugging
                print(f"🔸 [BG] Envelope snippet: {raw[:300]}")

                try:
                    msg = json.loads(raw)
                except Exception:
                    # não é JSON válido no envelope -> ignora
                    print(f"⚠️ [BG] Linha não-JSON recebida (ignorada) snippet: {raw[:200]}")
                    continue

                data_encoded = msg.get('data', '')
                if not data_encoded:
                    continue

                # tenta base64 decode; se falhar, assume texto
                data_decoded = None
                try:
                    if isinstance(data_encoded, (bytes, bytearray)):
                        b = data_encoded
                    else:
                        b = data_encoded.encode('utf-8')
                    b = b.strip()
                    data_decoded = base64.b64decode(b).decode('utf-8')
                except Exception:
                    try:
                        data_decoded = data_encoded if isinstance(data_encoded, str) else data_encoded.decode('utf-8', errors='ignore')
                    except Exception:
                        data_decoded = None

                # Log data snippet
                print(f"🔹 [BG] data snippet: {str(data_decoded)[:300]}")

                if not data_decoded:
                    continue

                try:
                    message_obj = json.loads(data_decoded)
                except Exception:
                    # não é JSON no data -> ignora
                    print(f"⚠️ [BG] Conteúdo 'data' não é JSON (ignorado) snippet: {data_decoded[:200]}")
                    continue

                msg_type = message_obj.get('type')

                # Heartbeat -> regista peer
                if msg_type == 'peer_heartbeat':
                    pid = message_obj.get('peer_id')
                    if pid:
                        register_peer(pid)
                    continue
                
                if msg_type == 'leader_heartbeat':
                    # Servidor ignora, é só para peers
                    continue
                # Proposta de documento via PubSub (cliente enviou cid)
                if msg_type == 'document_proposal':
                    doc_id = message_obj.get('doc_id')
                    filename = message_obj.get('filename')
                    cid = message_obj.get('cid')
                    sender = message_obj.get('from_peer', None)

                    # Evita que o próprio servidor recrie sessão para propostas que ele mesmo enviou
                    if sender == get_my_peer_id():
                        continue

                    if not doc_id or not filename:
                        print(f"⚠️ [BG] Proposta sem doc_id/filename: {message_obj}")
                        continue

                    if doc_id in voting_sessions:
                        print(f"ℹ️ [BG] Sessão já existe para {doc_id}")
                        continue

                    content = b''
                    if cid:
                        # tenta obter ficheiro do IPFS pelo cid
                        try:
                            r = requests.post(f"{IPFS_API_URL}/cat", params={'arg': cid}, timeout=30)
                            if r.status_code == 200:
                                content = r.content
                                print(f"📥 [BG] Ficheiro obtido do IPFS para CID {cid} (doc_id={doc_id[:8]})")
                            else:
                                print(f"⚠️ [BG] Falha ao obter CID {cid} do IPFS (status {r.status_code})")
                        except Exception as e:
                            print(f"⚠️ [BG] Erro ao cat CID {cid}: {e}")

                    # cria sessão local usando o conteúdo (pode ser vazio se falhar o cat)
                    session = create_voting_session(doc_id, filename, content if content is not None else b'')
                    session["received_from_peer"] = True

                    print(f"📩 [BG] Proposta recebida via PubSub -> {filename} (doc_id={doc_id[:8]}). Sessão criada localmente.")
                    # não repropaga se já veio com cid (evita loops)
                    continue

                # Voto vindo de PubSub
                if msg_type == 'peer_vote':
                    doc_id = message_obj.get('doc_id')
                    vote = message_obj.get('vote')
                    peer_id = message_obj.get('peer_id')
                    if doc_id and vote and peer_id:
                        process_vote(doc_id, peer_id, vote)
                        continue

                # Decisões aprovadas/rejeitadas -> sincroniza vetor local
                if msg_type in ('document_approved', 'document_rejected'):
                    doc_id = message_obj.get('doc_id')
                    if doc_id and doc_id in voting_sessions:
                        del voting_sessions[doc_id]
                    if msg_type == 'document_approved' and 'embeddings' in message_obj and message_obj.get('cid'):
                        cid = message_obj.get('cid')
                        embeddings_array = np.array(message_obj['embeddings'])
                        np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings_array)
                        vector = load_document_vector()
                        vector.setdefault("documents_confirmed", [])
                        vector["documents_confirmed"].append({
                            "cid": cid,
                            "filename": message_obj.get("filename"),
                            "added_at": message_obj.get("timestamp"),
                            "embedding_shape": message_obj.get("embedding_shape"),
                            "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy",
                            "confirmed": True
                        })
                        vector["version_confirmed"] = message_obj.get("version", vector.get("version_confirmed", 0) + 1)
                        save_document_vector(vector)
                    continue
                
            except Exception as e:
                print(f"⚠️ [BG] Erro ao processar mensagem PubSub: {e}")
                continue
    except Exception as e:
        print(f"❌ [BG] Falha no listener PubSub: {e}")

if __name__ == "__main__":
    """
    Ponto de entrada quando o ficheiro é executado diretamente.
    - Mostra informação básica
    - Regista o próprio peer
    - Envia um heartbeat inicial
    - Inicia o servidor Uvicorn na porta 5000
    """
    print("\n" + "="*60)
    print("IPFS Upload API")
    print("="*60)
    print(f"Servidor: http://0.0.0.0:5000")
    print(f"Documentação: http://localhost:5000/docs")
    print(f"Canal PubSub: {CANAL_PUBSUB}")
    print(f"Modelo: SentenceTransformer (all-MiniLM-L6-v2)")
    print(f"Dimensão embeddings: 384")
    print("="*60)
    
    # Obtém e regista o Peer ID local
    my_id = get_my_peer_id()
    register_peer(my_id)
    
    # Inicia listener PubSub em background para receber uploads via PubSub
    try:
        t = threading.Thread(target=pubsub_bg_listener, daemon=True)
        t.start()
        print("🔁 Background PubSub listener iniciado")
    except Exception as e:
        print(f"⚠️ Não foi possível iniciar listener PubSub em background: {e}")
    
    print(f"Peer ID: {my_id}")
    print(f"Peers no sistema: {get_peers_count()}")
    
    # Enviar heartbeat inicial para os outros peers
    broadcast_heartbeat()
    
    try:
        leader_thread = threading.Thread(target=leader_heartbeat_loop, daemon=True)
        leader_thread.start()
        print("💓 Leader heartbeat thread iniciado (envia a cada 5s)")
    except Exception as e:
        print(f"⚠️ Erro ao iniciar leader heartbeat: {e}")
    
    # Inicia o servidor FastAPI com Uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info"
    )
