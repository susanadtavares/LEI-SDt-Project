"""
Peer com votação automática e sincronização via PubSub
"""

import requests
import json
import time
import threading
import subprocess
import numpy as np
import faiss
import os
from datetime import datetime, timedelta
from typing import Dict
from pathlib import Path

# Configuração
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
HEARTBEAT_INTERVAL = 5
LEADER_TIMEOUT = 20
EMBEDDINGS_DIR = "embeddings"
TEMP_EMBEDDINGS_DIR = "temp_embeddings"
FAISS_INDEX_FILE = "faiss_index.bin"
VECTOR_FILE = "document_vector.json"

Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)
Path(TEMP_EMBEDDINGS_DIR).mkdir(exist_ok=True)

# Estado do peer
voting_sessions: Dict[str, dict] = {}
my_peer_id = None
last_leader_heartbeat: datetime = None
leader_alive = True
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
        print(f"❌ Erro ao publicar: {e}")
        return False


def send_heartbeat():
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
            print(f"💓 Heartbeat #{count} enviado")
        time.sleep(HEARTBEAT_INTERVAL)


def check_leader_status():
    global leader_alive, last_leader_heartbeat
    
    if last_leader_heartbeat is None:
        return
    
    time_since_last = (datetime.now() - last_leader_heartbeat).total_seconds()
    
    if time_since_last > LEADER_TIMEOUT:
        if leader_alive:
            leader_alive = False
            print(f"\n🚨 LÍDER DOWN! (último HB há {int(time_since_last)}s)")
    else:
        if not leader_alive:
            leader_alive = True
            print(f"\n✅ LÍDER RECUPERADO!")


def leader_monitor_loop():
    while running:
        check_leader_status()
        time.sleep(5)


def auto_vote_on_proposal(doc_id: str, filename: str):
    """
    ✅ VOTAÇÃO AUTOMÁTICA
    Peer vota automaticamente "approve" em todas as propostas
    (Pode ser modificado para lógica mais complexa)
    """
    # Simples: aprova tudo
    vote_type = "approve"
    
    # Lógica alternativa (exemplo):
    # if "test" in filename.lower():
    #     vote_type = "reject"
    # else:
    #     vote_type = "approve"
    
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
                    vector = json.load(f)
                faiss_cid_map = [doc['cid'] for doc in vector.get('documents_confirmed', [])]
            print(f"✅ FAISS carregado: {faiss_index.ntotal} vetores")
        except Exception as e:
            print(f"⚠️  Erro ao carregar FAISS: {e}")
            faiss_index = faiss.IndexFlatL2(384)
            faiss_cid_map = []


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
    global running, last_leader_heartbeat, leader_alive, leader_id
    
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
                        
                        sender_peer = message_obj.get('peer_id', message_obj.get('leader_id'))
                        msg_type = message_obj.get('type')
                        
                        # Ignora próprias mensagens
                        if sender_peer == my_peer_id and msg_type not in ['document_proposal', 'version_update_request', 'version_commit']:
                            continue
                        
                        # Heartbeat do líder
                        if msg_type == 'leader_heartbeat':
                            leader_id = message_obj.get('leader_id')
                            last_leader_heartbeat = datetime.now()
                            
                            if not leader_alive:
                                leader_alive = True
                                print(f"\n✅ LÍDER RECUPERADO!")
                        
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
                                print(f"   Doc ID: {doc_id[:12]}...")
                                print(f"   Votos necessários: {message_obj.get('required_votes', 1)}")
                                
                                # ✅ VOTAÇÃO AUTOMÁTICA
                                time.sleep(1)  # Pequeno delay para simular processamento
                                auto_vote_on_proposal(doc_id, filename)
                        
                        # Voto de peer
                        elif msg_type == 'peer_vote':
                            doc_id = message_obj.get('doc_id')
                            vote = message_obj.get('vote')
                            peer_id_vote = message_obj.get('peer_id')
                            
                            if doc_id in voting_sessions:
                                session = voting_sessions[doc_id]
                                
                                if vote == "approve":
                                    if peer_id_vote not in session.get("votes_approve", []):
                                        session.setdefault("votes_approve", []).append(peer_id_vote)
                                elif vote == "reject":
                                    if peer_id_vote not in session.get("votes_reject", []):
                                        session.setdefault("votes_reject", []).append(peer_id_vote)
                                
                                approve_count = len(session.get("votes_approve", []))
                                reject_count = len(session.get("votes_reject", []))
                                required = session["required_votes"]
                                
                                print(f"📊 Voto de {peer_id_vote[:12]}...: {vote.upper()}")
                                print(f"   A FAVOR: {approve_count} | CONTRA: {reject_count} | Necessários: {required}")
                        
                        # Documento aprovado
                        elif msg_type == 'document_approved':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename', 'unknown')
                            
                            print(f"\n✅ DOCUMENTO APROVADO: {filename}")
                            
                            if doc_id in voting_sessions:
                                del voting_sessions[doc_id]
                        
                        # Documento rejeitado
                        elif msg_type == 'document_rejected':
                            doc_id = message_obj.get('doc_id')
                            filename = message_obj.get('filename', 'unknown')
                            
                            print(f"\n❌ DOCUMENTO REJEITADO: {filename}")
                            
                            if doc_id in voting_sessions:
                                del voting_sessions[doc_id]
                        
                        # Pedido de atualização de versão
                        elif msg_type == 'version_update_request':
                            version = message_obj.get('version')
                            cid = message_obj.get('cid')
                            embeddings_list = message_obj.get('embeddings')
                            
                            print(f"\n📥 Pedido de atualização: versão {version}, CID: {cid[:16]}...")
                            
                            # Guarda embedding temporariamente
                            if embeddings_list:
                                embeddings_array = np.array(embeddings_list)
                                np.save(f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy", embeddings_array)
                            
                            # Atualiza vetor local
                            if os.path.exists(VECTOR_FILE):
                                with open(VECTOR_FILE, 'r') as f:
                                    vector = json.load(f)
                            else:
                                vector = {
                                    "version_confirmed": 0,
                                    "version_pending": 0,
                                    "documents_confirmed": [],
                                    "documents_temp": []
                                }
                            
                            vector["version_pending"] = version
                            vector.setdefault("documents_temp", [])
                            vector["documents_temp"].append({
                                "cid": cid,
                                "filename": message_obj.get('filename'),
                                "added_at": datetime.now().isoformat(),
                                "embedding_shape": message_obj.get('embedding_shape')
                            })
                            
                            with open(VECTOR_FILE, 'w') as f:
                                json.dump(vector, f, indent=2)
                            
                            # Calcula hash do vetor
                            cids = [doc['cid'] for doc in vector.get('documents_confirmed', [])]
                            vector_str = json.dumps(cids, sort_keys=True)
                            import hashlib
                            local_hash = hashlib.sha256(vector_str.encode()).hexdigest()
                            
                            # Envia confirmação ao líder
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
                            
                            # Move embedding temp → definitivo
                            temp_path = f"{TEMP_EMBEDDINGS_DIR}/{cid}.npy"
                            final_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
                            
                            if os.path.exists(temp_path):
                                os.rename(temp_path, final_path)
                            
                            # Atualiza vetor
                            if os.path.exists(VECTOR_FILE):
                                with open(VECTOR_FILE, 'r') as f:
                                    vector = json.load(f)
                                
                                if vector.get("documents_temp"):
                                    for temp_doc in vector["documents_temp"]:
                                        if temp_doc["cid"] == cid:
                                            temp_doc["confirmed"] = True
                                            vector["documents_confirmed"].append(temp_doc)
                                    
                                    vector["version_confirmed"] = version
                                    vector["documents_temp"] = []
                                    vector["version_pending"] = 0
                                    
                                    with open(VECTOR_FILE, 'w') as f:
                                        json.dump(vector, f, indent=2)
                            
                            # ✅ ATUALIZA ÍNDICE FAISS
                            update_faiss_index_after_commit(cid)
                            
                            print(f"✅ Versão {version} confirmada e FAISS atualizado")
                    
                    except json.JSONDecodeError:
                        buffer = ""
                        continue
                    except Exception as e:
                        print(f"⚠️  Erro ao processar mensagem: {e}")
                        buffer = ""
                        continue
                
                if len(buffer) > 100000:
                    buffer = ""
            
            except Exception as e:
                continue
        
        process.kill()
    
    except FileNotFoundError:
        print("❌ Comando 'ipfs' não encontrado!")
        running = False
    except Exception as e:
        print(f"❌ Erro no listener PubSub: {e}")
        running = False


def start_peer():
    global running
    
    print("\n" + "="*60)
    print("PEER COM VOTAÇÃO AUTOMÁTICA")
    print("="*60)
    
    my_id = get_my_peer_id()
    print(f"Peer ID: {my_id}")
    
    # Carrega índice FAISS
    load_faiss_index()
    print(f"FAISS: {faiss_index.ntotal} vetores")
    
    print("="*60)
    print("\nFUNCIONALIDADES:")
    print("  ✅ Votação automática (approve)")
    print("  ✅ Recebe atualizações de versão")
    print("  ✅ Envia confirmações ao líder")
    print("  ✅ Atualiza FAISS após commit")
    print("  ✅ Monitoriza líder (heartbeat)")
    print("="*60 + "\n")
    
    running = True
    
    # Inicia threads
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    
    monitor_thread = threading.Thread(target=leader_monitor_loop, daemon=True)
    monitor_thread.start()
    
    pubsub_thread = threading.Thread(target=pubsub_listener, daemon=True)
    pubsub_thread.start()
    
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
        print("💡 Certifica-te: ipfs daemon")
    except Exception as e:
        print(f"❌ Erro: {e}")
