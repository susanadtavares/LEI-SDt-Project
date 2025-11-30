import requests
import json
import time
import threading
import subprocess
import numpy as np
import faiss
import os
import random
from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path
import hashlib

# Configuração
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
HEARTBEAT_INTERVAL = 5
LEADER_TIMEOUT = 15  # Tempo para considerar líder desconectado
ELECTION_TIMEOUT_MIN = 10
ELECTION_TIMEOUT_MAX = 20
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
FAISS_INDEX_FILE = "faiss_index.bin"
VECTOR_FILE = "document_vector.json"

Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(TEMP_EMBEDDINGS_DIR).mkdir(exist_ok=True)

# Estados
class NodeState:
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

# Estado do peer
current_state = NodeState.FOLLOWER
current_term = 0
voted_for = None
votes_received = set()
voting_sessions: Dict[str, dict] = {}
my_peer_id = None
last_leader_heartbeat: datetime = None
leader_id = None
running = False

# FAISS
faiss_index = faiss.IndexFlatL2(384)
faiss_cid_map = []


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


def publish_to_pubsub(message_data: dict):
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
        return False


def send_heartbeat():
    """Envia heartbeat de peer ou líder"""
    if current_state == NodeState.LEADER:
        # Heartbeat de líder
        message_data = {
            "type": "leader_heartbeat",
            "leader_id": get_my_peer_id(),
            "term": current_term,
            "timestamp": datetime.now().isoformat()
        }
    else:
        # Heartbeat normal de peer
        message_data = {
            "type": "peer_heartbeat",
            "peer_id": get_my_peer_id(),
            "timestamp": datetime.now().isoformat()
        }
    
    return publish_to_pubsub(message_data)


def heartbeat_loop():
    count = 0
    while running:
        send_heartbeat()
        count += 1
        if count % 6 == 0:
            if current_state == NodeState.LEADER:
                print(f"💓 Heartbeat de LÍDER #{count} enviado")
            else:
                print(f"💓 Heartbeat #{count} enviado")
        time.sleep(HEARTBEAT_INTERVAL)


def check_leader_status():
    global last_leader_heartbeat
    
    if current_state == NodeState.LEADER:
        return True
    
    if last_leader_heartbeat is None:
        return False
    
    time_since_last = (datetime.now() - last_leader_heartbeat).total_seconds()
    
    return time_since_last <= LEADER_TIMEOUT


def start_election():
    """Inicia processo de eleição"""
    global current_state, current_term, voted_for, votes_received
    
    current_state = NodeState.CANDIDATE
    current_term += 1
    voted_for = get_my_peer_id()
    votes_received = {get_my_peer_id()}  # Vota em si mesmo
    
    print(f"\n{'='*60}")
    print(f"🗳️  INICIANDO ELEIÇÃO")
    print(f"{'='*60}")
    print(f"Term: {current_term}")
    print(f"Candidato: {voted_for[:20]}...")
    print(f"{'='*60}\n")
    
    # Envia pedido de voto
    message_data = {
        "type": "election_request_vote",
        "candidate_id": get_my_peer_id(),
        "term": current_term,
        "timestamp": datetime.now().isoformat()
    }
    
    publish_to_pubsub(message_data)
    
    # Aguarda votos
    time.sleep(5)
    
    # Verifica se ganhou (maioria simples)
    if len(votes_received) >= 1:  # Pelo menos o próprio voto
        become_leader()


def become_leader():
    """Promove este peer a líder"""
    global current_state, leader_id
    
    current_state = NodeState.LEADER
    leader_id = get_my_peer_id()
    
    print(f"\n{'='*60}")
    print(f"👑 ELEITO LÍDER!")
    print(f"{'='*60}")
    print(f"Peer ID: {leader_id[:40]}...")
    print(f"Term: {current_term}")
    print(f"{'='*60}\n")
    
    # Envia heartbeat imediatamente
    send_heartbeat()


def become_follower(new_term: int, new_leader: str):
    """Volta ao estado de follower"""
    global current_state, current_term, voted_for, leader_id
    
    if new_term > current_term:
        current_state = NodeState.FOLLOWER
        current_term = new_term
        voted_for = None
        leader_id = new_leader
        print(f"📍 Track do novo líder (term {new_term}): {new_leader[:20]}...")


def election_timer_loop():
    """Thread que monitora líder e inicia eleição se necessário"""
    while running:
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        time.sleep(timeout)
        
        if current_state == NodeState.FOLLOWER and not check_leader_status():
            print(f"\n🚨 LÍDER DESCONECTADO! Iniciando eleição...")
            start_election()


def auto_vote_on_proposal(doc_id: str, filename: str):
    """
    ✅ VOTAÇÃO AUTOMÁTICA
    Peer vota automaticamente "approve" em todas as propostas
    (Pode ser modificado para lógica mais complexa)
    """
    vote_type = "approve"
    
    print(f"\n🗳️  VOTAÇÃO AUTOMÁTICA: {vote_type.upper()} em '{filename}'")
    
    message_data = {
        "type": "peer_vote",
        "doc_id": doc_id,
        "vote": vote_type,
        "peer_id": get_my_peer_id(),
        "timestamp": datetime.now().isoformat()
    }
    
    success = publish_to_pubsub(message_data)
    
    if success:
        if doc_id in voting_sessions:
            session = voting_sessions[doc_id]
            if vote_type == "approve":
                if my_peer_id not in session.get("votes_approve", []):
                    session.setdefault("votes_approve", []).append(my_peer_id)
            elif vote_type == "reject":
                if my_peer_id not in session.get("votes_reject", []):
                    session.setdefault("votes_reject", []).append(my_peer_id)
        
        print(f"✅ Voto '{vote_type}' enviado")
    else:
        print(f"❌ Falha ao enviar voto")
    
    return success


def load_faiss_index():
    global faiss_index, faiss_cid_map
    
    if os.path.exists(FAISS_INDEX_FILE):
        try:
            faiss_index = faiss.read_index(FAISS_INDEX_FILE)
            if os.path.exists(VECTOR_FILE):
                with open(VECTOR_FILE, 'r') as f:
                    content = f.read().strip()
                    if content:
                        vector = json.loads(content)
                        faiss_cid_map = [doc['cid'] for doc in vector.get('documents_confirmed', [])]
            print(f"✅ FAISS carregado: {faiss_index.ntotal} vetores")
        except Exception as e:
            print(f"⚠️  Erro ao carregar FAISS: {e}")
            faiss_index = faiss.IndexFlatL2(384)
            faiss_cid_map = []
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


def pubsub_listener():
    global running, last_leader_heartbeat, leader_id, current_state, current_term, voted_for, votes_received
    
    try:
        print("📡 A conectar ao canal PubSub...")
        
        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'sub', CANAL_PUBSUB],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )
        
        print(f"✅ Conectado ao canal '{CANAL_PUBSUB}'")
        print("🔍 A aguardar mensagens...\n")
        
        buffer = ""
        
        while running:
            try:
                byte = process.stdout.read(1)
                if not byte:
                    break
                
                char = byte.decode('utf-8', errors='ignore')
                buffer += char
                
                if char == '}':
                    try:
                        message_obj = json.loads(buffer)
                        buffer = ""
                        
                        sender_peer = message_obj.get('peer_id', message_obj.get('leader_id', message_obj.get('candidate_id')))
                        msg_type = message_obj.get('type')
                        
                        # Ignora próprias mensagens (exceto broadcasts)
                        if sender_peer == my_peer_id and msg_type not in ['document_proposal', 'version_update_request', 'version_commit']:
                            continue
                        
                        # Heartbeat do líder
                        if msg_type == 'leader_heartbeat':
                            leader_id_msg = message_obj.get('leader_id')
                            term = message_obj.get('term', 0)
                            last_leader_heartbeat = datetime.now()
                            
                            if term >= current_term:
                                become_follower(term, leader_id_msg)
                        
                        # Pedido de voto (eleição)
                        elif msg_type == 'election_request_vote':
                            candidate_id = message_obj.get('candidate_id')
                            term = message_obj.get('term', 0)
                            
                            # Vota se term é maior e ainda não votou neste term
                            if term > current_term and (voted_for is None or voted_for == candidate_id):
                                voted_for = candidate_id
                                current_term = term
                                current_state = NodeState.FOLLOWER
                                
                                vote_msg = {
                                    "type": "election_vote",
                                    "voter_id": my_peer_id,
                                    "candidate_id": candidate_id,
                                    "term": term,
                                    "timestamp": datetime.now().isoformat()
                                }
                                
                                publish_to_pubsub(vote_msg)
                                print(f"🗳️  Votei em {candidate_id[:20]}... (term {term})")
                        
                        # Voto recebido (se este peer é candidato)
                        elif msg_type == 'election_vote':
                            if current_state == NodeState.CANDIDATE:
                                voter_id = message_obj.get('voter_id')
                                candidate_id_msg = message_obj.get('candidate_id')
                                
                                if candidate_id_msg == my_peer_id:
                                    votes_received.add(voter_id)
                                    print(f"✅ Voto recebido de {voter_id[:20]}... (total: {len(votes_received)})")
                        
                        # Proposta de documento
                        elif msg_type == 'document_proposal':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename')
                            
                            if not doc_id or not filename:
                                continue
                            
                            if doc_id not in voting_sessions:
                                voting_sessions[doc_id] = {
                                    "doc_id": doc_id,
                                    "filename": filename,
                                    "status": "pending_approval",
                                    "total_peers": message_obj.get('total_peers', 1),
                                    "required_votes": message_obj.get('required_votes', 1),
                                    "votes_approve": [],
                                    "votes_reject": [],
                                    "created_at": message_obj.get('timestamp', datetime.now().isoformat())
                                }
                                
                                print(f"\n📢 NOVA PROPOSTA: {filename}")
                                print(f"   Votos necessários: {message_obj.get('required_votes', 1)}/{message_obj.get('total_peers', 1)}")
                                
                                time.sleep(1)
                                auto_vote_on_proposal(doc_id, filename)
                        
                        # Voto de peer
                        elif msg_type == 'peer_vote':
                            doc_id = message_obj.get('doc_id')
                            vote = message_obj.get('vote')
                            peer_id_vote = message_obj.get('peer_id')
                            
                            if doc_id in voting_sessions:
                                session = voting_sessions[doc_id]
                                
                                # Remove votos anteriores deste peer
                                votes_approve = session.get("votes_approve", [])
                                votes_reject = session.get("votes_reject", [])
                                
                                if peer_id_vote in votes_approve:
                                    votes_approve.remove(peer_id_vote)
                                if peer_id_vote in votes_reject:
                                    votes_reject.remove(peer_id_vote)
                                
                                # Adiciona novo voto
                                if vote == "approve":
                                    if peer_id_vote not in votes_approve:
                                        votes_approve.append(peer_id_vote)
                                elif vote == "reject":
                                    if peer_id_vote not in votes_reject:
                                        votes_reject.append(peer_id_vote)
                                
                                session["votes_approve"] = votes_approve
                                session["votes_reject"] = votes_reject
                                
                                # Mostra progresso
                                approve_count = len(votes_approve)
                                reject_count = len(votes_reject)
                                required = session.get("required_votes", 1)
                                
                                print(f"📊 Voto de {peer_id_vote[:12]}...: {vote.upper()}")
                                print(f"   Status: ✅ A FAVOR: {approve_count}/{required} | ❌ CONTRA: {reject_count}/{required}")
                                
                                # Verifica se atingiu consenso
                                if approve_count >= required:
                                    print(f"🎉 CONSENSO ATINGIDO! Documento será aprovado")
                                elif reject_count >= required:
                                    print(f"🚫 CONSENSO ATINGIDO! Documento será rejeitado")
                            else:
                                print(f"📊 Voto de {peer_id_vote[:12]}...: {vote.upper()}")
                        
                        # Documento aprovado
                        elif msg_type == 'document_approved':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename', 'unknown')
                            
                            if doc_id in voting_sessions:
                                del voting_sessions[doc_id]
                            
                            print(f"\n✅ DOCUMENTO APROVADO: {filename}")
                        
                        # Documento rejeitado
                        elif msg_type == 'document_rejected':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename', 'unknown')
                            
                            if doc_id in voting_sessions:
                                del voting_sessions[doc_id]
                            
                            print(f"\n❌ DOCUMENTO REJEITADO: {filename}")
                        
                        # Pedido de atualização de versão
                        elif msg_type == 'version_update_request':
                            version = message_obj.get('version')
                            cid = message_obj.get('cid')
                            embeddings_list = message_obj.get('embeddings')
                            
                            print(f"\n📥 Pedido de atualização: versão {version}, CID: {cid[:16]}...")
                            
                            if embeddings_list:
                                embeddings_array = np.array(embeddings_list)
                                np.save(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", embeddings_array)
                            
                            if os.path.exists(VECTOR_FILE):
                                with open(VECTOR_FILE, 'r') as f:
                                    content = f.read().strip()
                                    if content:
                                        vector = json.loads(content)
                                    else:
                                        vector = {
                                            "version_confirmed": 0,
                                            "documents_confirmed": [],
                                            "documents_temp": []
                                        }
                            else:
                                vector = {
                                    "version_confirmed": 0,
                                    "documents_confirmed": [],
                                    "documents_temp": []
                                }
                            
                            vector["version_pending"] = version
                            vector.setdefault("documents_temp", [])
                            vector["documents_temp"].append({
                                "cid": cid,
                                "filename": message_obj.get('filename'),
                                "added_at": datetime.now().isoformat()
                            })
                            
                            with open(VECTOR_FILE, 'w') as f:
                                json.dump(vector, f, indent=2)
                            
                            cids = [doc['cid'] for doc in vector.get('documents_confirmed', [])]
                            vector_str = json.dumps(cids, sort_keys=True)
                            local_hash = hashlib.sha256(vector_str.encode()).hexdigest()
                            
                            confirm_msg = {
                                "type": "version_confirmation",
                                "peer_id": my_peer_id,
                                "version": version,
                                "hash": local_hash,
                                "timestamp": datetime.now().isoformat()
                            }
                            
                            publish_to_pubsub(confirm_msg)
                            print(f"✅ Hash enviada ao líder: {local_hash[:16]}...")
                        
                        # Commit de versão
                        elif msg_type == 'version_commit':
                            version = message_obj.get('version')
                            cid = message_obj.get('cid')
                            
                            print(f"\n✅ COMMIT RECEBIDO: versão {version}, CID: {cid[:16]}...")
                            
                            temp_path = f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy"
                            final_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
                            
                            if os.path.exists(temp_path):
                                os.rename(temp_path, final_path)
                            
                            if os.path.exists(VECTOR_FILE):
                                with open(VECTOR_FILE, 'r') as f:
                                    content = f.read().strip()
                                    if content:
                                        vector = json.loads(content)
                                    else:
                                        vector = {"version_confirmed": 0, "documents_confirmed": [], "documents_temp": []}
                            else:
                                vector = {"version_confirmed": 0, "documents_confirmed": [], "documents_temp": []}
                            
                            if vector.get("documents_temp"):
                                for temp_doc in vector["documents_temp"]:
                                    if temp_doc["cid"] == cid:
                                        temp_doc["confirmed"] = True
                                        vector["documents_confirmed"].append(temp_doc)
                                
                                vector["version_confirmed"] = version
                                vector["documents_temp"] = []
                                
                                with open(VECTOR_FILE, 'w') as f:
                                    json.dump(vector, f, indent=2)
                            
                            # ✅ ATUALIZA ÍNDICE FAISS
                            update_faiss_index_after_commit(cid)
                            
                            print(f"✅ Versão {version} confirmada e FAISS atualizado")
                        
                        # Peer heartbeat
                        elif msg_type == 'peer_heartbeat':
                            pass  # Apenas para manter peers ativos
                    
                    except json.JSONDecodeError:
                        buffer = ""
                    except Exception as e:
                        buffer = ""
                
                if len(buffer) > 100000:
                    buffer = ""
            
            except:
                continue
        
        process.kill()
    
    except FileNotFoundError:
        print("❌ Comando 'ipfs' não encontrado!")
        running = False
    except Exception as e:
        print(f"❌ Erro: {e}")
        running = False


def start_peer():
    global running
    
    print("\n" + "="*60)
    print("PEER COM VOTAÇÃO AUTOMÁTICA E ELEIÇÃO")
    print("="*60)
    
    my_id = get_my_peer_id()
    print(f"Peer ID: {my_id}")
    
    # Carrega índice FAISS
    load_faiss_index()
    print(f"FAISS: {faiss_index.ntotal} vetores")
    
    running = True
    
    # Inicia threads
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=election_timer_loop, daemon=True).start()
    threading.Thread(target=pubsub_listener, daemon=True).start()
    
    # Mantém programa ativo
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🔌 A desconectar...")
        running = False


if __name__ == "__main__":
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=2)
        
        if response.status_code == 200:
            start_peer()
        else:
            print("❌ IPFS não responde")
    
    except requests.exceptions.ConnectionError:
        print("❌ Erro ao conectar ao IPFS")
        print("💡 Execute: ipfs daemon")
    except Exception as e:
        print(f"❌ Erro: {e}")
