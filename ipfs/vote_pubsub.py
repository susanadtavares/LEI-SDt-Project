"""
VERSÃO FINAL - Sistema de votação via PubSub.

CORREÇÕES IMPLEMENTADAS:
- Leitura byte-a-byte em vez de readline() para capturar mensagens sem \n
- Parser robusto para JSONs concatenados
- Debug completo
- voting_sessions como global partilhado entre threads

Pressupostos:
- IPFS daemon rodando com --enable-pubsub-experiment
- Canal PubSub: 'canal-ficheiros'
"""

import requests
import json
import time
import threading
import subprocess
from datetime import datetime
from typing import Dict, List

# Configuração
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"
HEARTBEAT_INTERVAL = 5

# Estado local sincronizado via PubSub
voting_sessions: Dict[str, dict] = {}
my_peer_id = None
pubsub_thread = None
heartbeat_thread = None
running = False
debug_mode = True


def get_my_peer_id():
    """Obtém o Peer ID deste nó."""
    global my_peer_id
    if my_peer_id:
        return my_peer_id
    
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=5)
        if response.status_code == 200:
            my_peer_id = response.json()['ID']
            return my_peer_id
    except Exception as e:
        if debug_mode:
            print(f"⚠️ Erro ao obter peer ID: {e}")
    return "unknown"


def publish_to_pubsub(message_data: dict):
    """Publica uma mensagem no canal PubSub usando CLI."""
    try:
        message_json = json.dumps(message_data)
        
        if debug_mode:
            msg_type = message_data.get('type', 'unknown')
            if msg_type != 'peer_heartbeat':
                print(f"📤 Enviando: {msg_type}")
        
        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        stdout, stderr = process.communicate(input=message_json.encode('utf-8'), timeout=5)
        return process.returncode == 0
        
    except Exception as e:
        if debug_mode:
            print(f"❌ Erro ao publicar mensagem: {e}")
        return False


def send_heartbeat():
    """Envia heartbeat via PubSub."""
    message_data = {
        "type": "peer_heartbeat",
        "peer_id": get_my_peer_id(),
        "timestamp": datetime.now().isoformat()
    }
    return publish_to_pubsub(message_data)


def heartbeat_loop():
    """Thread que envia heartbeats periódicos."""
    count = 0
    while running:
        send_heartbeat()
        count += 1
        
        if count % 6 == 0 and debug_mode:
            print(f"💓 Heartbeat #{count} enviado - Peer ativo")
        
        time.sleep(HEARTBEAT_INTERVAL)


def send_vote(doc_id: str, vote_type: str):
    """Envia um voto via PubSub."""
    message_data = {
        "type": "peer_vote",
        "doc_id": doc_id,
        "vote": vote_type,
        "peer_id": get_my_peer_id(),
        "timestamp": datetime.now().isoformat()
    }
    
    success = publish_to_pubsub(message_data)
    
    if success:
        print(f"✅ Voto '{vote_type}' enviado via PubSub")
        if doc_id in voting_sessions:
            session = voting_sessions[doc_id]
            if vote_type == "approve":
                if my_peer_id not in session["votes_approve"]:
                    session["votes_approve"].append(my_peer_id)
                if my_peer_id in session["votes_reject"]:
                    session["votes_reject"].remove(my_peer_id)
            elif vote_type == "reject":
                if my_peer_id not in session["votes_reject"]:
                    session["votes_reject"].append(my_peer_id)
                if my_peer_id in session["votes_approve"]:
                    session["votes_approve"].remove(my_peer_id)
    else:
        print("❌ Falha ao enviar voto")
    
    return success


def process_pubsub_message(message_obj: dict):
    """Processa mensagens recebidas do canal PubSub."""
    global voting_sessions
    
    msg_type = message_obj.get('type')
    
    if debug_mode and msg_type != 'peer_heartbeat':
        print(f"📥 Mensagem recebida: {msg_type}")
    
    if msg_type == 'peer_heartbeat':
        pass
    
    elif msg_type == 'document_proposal':
        doc_id = message_obj.get('doc_id')
        filename = message_obj.get('filename')
        cid = message_obj.get('cid')
        
        if not doc_id or not filename:
            if debug_mode:
                print(f"⚠️ Proposta incompleta: {message_obj}")
            return
        
        if doc_id not in voting_sessions:
            voting_sessions[doc_id] = {
                "doc_id": doc_id,
                "filename": filename,
                "status": "pending_approval",
                "total_peers": message_obj.get('total_peers', 1),
                "required_votes": message_obj.get('required_votes', 1),
                "votes_approve": [],
                "votes_reject": [],
                "created_at": message_obj.get('timestamp', datetime.now().isoformat()),
                "from_peer": message_obj.get('from_peer', 'unknown'),
                "cid": cid
            }
            
            print(f"\n📢 NOVA PROPOSTA: {filename}")
            print(f"   Doc ID: {doc_id[:12]}...")
            if cid:
                print(f"   CID: {cid}")
            print(f"   Votos necessários: {message_obj.get('required_votes', 1)}")
            print(f"   Total peers: {message_obj.get('total_peers', 1)}")
            print(">>> ", end='', flush=True)
        else:
            if debug_mode:
                print(f"ℹ️ Proposta duplicada ignorada: {doc_id[:12]}...")
    
    elif msg_type == 'peer_vote':
        doc_id = message_obj.get('doc_id')
        vote = message_obj.get('vote')
        peer_id = message_obj.get('peer_id')
        
        if not doc_id or not vote or not peer_id:
            if debug_mode:
                print(f"⚠️ Voto incompleto: {message_obj}")
            return
        
        if doc_id in voting_sessions:
            session = voting_sessions[doc_id]
            
            if peer_id in session["votes_approve"]:
                session["votes_approve"].remove(peer_id)
            if peer_id in session["votes_reject"]:
                session["votes_reject"].remove(peer_id)
            
            if vote == "approve":
                session["votes_approve"].append(peer_id)
            elif vote == "reject":
                session["votes_reject"].append(peer_id)
            
            approve_count = len(session["votes_approve"])
            reject_count = len(session["votes_reject"])
            required = session["required_votes"]
            
            if approve_count >= required and session["status"] == "pending_approval":
                session["status"] = "approved"
                print(f"\n✅ Documento APROVADO: {session['filename']}")
                print(f"   Votos A FAVOR: {approve_count} | CONTRA: {reject_count}")
                print(">>> ", end='', flush=True)
            elif reject_count >= required and session["status"] == "pending_approval":
                session["status"] = "rejected"
                print(f"\n❌ Documento REJEITADO: {session['filename']}")
                print(f"   Votos A FAVOR: {approve_count} | CONTRA: {reject_count}")
                print(">>> ", end='', flush=True)
            else:
                print(f"\n📊 Voto registado para '{session['filename']}'")
                print(f"   A FAVOR: {approve_count} | CONTRA: {reject_count} | Necessários: {required}")
                print(">>> ", end='', flush=True)
        else:
            if debug_mode:
                print(f"⚠️ Voto para documento desconhecido: {doc_id[:12]}...")


def parse_json_objects_from_buffer(buffer: str) -> tuple:
    """
    Extrai múltiplos objetos JSON completos de um buffer.
    Retorna (list_of_jsons, remaining_buffer).
    """
    results = []
    i = 0
    
    while i < len(buffer):
        start_idx = buffer.find('{', i)
        if start_idx == -1:
            break
        
        depth = 0
        end_idx = -1
        
        for j in range(start_idx, len(buffer)):
            if buffer[j] == '{':
                depth += 1
            elif buffer[j] == '}':
                depth -= 1
                if depth == 0:
                    end_idx = j
                    break
        
        if end_idx != -1:
            json_str = buffer[start_idx:end_idx + 1]
            try:
                obj = json.loads(json_str)
                results.append(obj)
            except json.JSONDecodeError as e:
                if debug_mode:
                    print(f"⚠️ Erro ao parse JSON: {e}")
            
            i = end_idx + 1
        else:
            # JSON incompleto, guarda no buffer
            return results, buffer[start_idx:]
    
    return results, ""


def pubsub_listener():
    """Thread que escuta mensagens do canal PubSub byte-a-byte."""
    global running, voting_sessions
    
    try:
        print("📡 A conectar ao canal PubSub...")
        
        process = subprocess.Popen(
            ['ipfs', 'pubsub', 'sub', CANAL_PUBSUB],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0  # Sem buffering
        )
        
        print(f"✅ Conectado ao canal '{CANAL_PUBSUB}'")
        print("🔍 A aguardar mensagens (leitura byte-a-byte)...\n")
        
        buffer = ""
        byte_count = 0
        
        while running:
            try:
                # Lê 1 byte de cada vez
                byte = process.stdout.read(1)
                if not byte:
                    break
                
                char = byte.decode('utf-8', errors='ignore')
                buffer += char
                byte_count += 1
                
                # Quando encontrar }, tenta processar buffer
                if char == '}':
                    json_objects, buffer = parse_json_objects_from_buffer(buffer)
                    
                    for message_obj in json_objects:
                        sender_peer = message_obj.get('peer_id')
                        msg_type = message_obj.get('type')
                        
                        if sender_peer == my_peer_id and msg_type not in ['document_proposal', 'document_approved', 'document_rejected']:
                            continue
                        
                        process_pubsub_message(message_obj)
                
                # Limita buffer para evitar overflow
                if len(buffer) > 100000:
                    if debug_mode:
                        print("⚠️ Buffer grande, limpando...")
                    buffer = ""
            
            except Exception as e:
                if debug_mode:
                    print(f"⚠️ Erro no loop: {e}")
                continue
        
        process.kill()
    
    except FileNotFoundError:
        print("❌ Comando 'ipfs' não encontrado!")
        running = False
    except Exception as e:
        print(f"❌ Erro no listener PubSub: {e}")
        running = False


def start_pubsub_listener():
    """Inicia threads de escuta do PubSub e envio de heartbeats."""
    global pubsub_thread, heartbeat_thread, running
    
    running = True
    
    pubsub_thread = threading.Thread(target=pubsub_listener, daemon=True)
    pubsub_thread.start()
    
    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    
    time.sleep(2)
    send_heartbeat()
    
    if debug_mode:
        print(f"🔄 Heartbeats periódicos ativados (cada {HEARTBEAT_INTERVAL}s)")


def stop_pubsub_listener():
    """Para threads de escuta do PubSub e heartbeats."""
    global running
    running = False
    
    if pubsub_thread:
        pubsub_thread.join(timeout=2)
    if heartbeat_thread:
        heartbeat_thread.join(timeout=2)


def get_pending_documents() -> List[dict]:
    """Retorna lista de sessões pendentes do estado local."""
    return [s for s in voting_sessions.values() if s['status'] == 'pending_approval']


def get_all_documents() -> List[dict]:
    """Retorna todos os documentos."""
    return list(voting_sessions.values())


def display_pending_documents(documents: List[dict]) -> bool:
    """Imprime listagem formatada dos documentos pendentes."""
    if not documents:
        print("\n❌ Nenhum documento pendente de votação\n")
        return False
    
    print("\n" + "="*70)
    print("DOCUMENTOS PENDENTES DE VOTAÇÃO")
    print("="*70)
    
    for idx, doc in enumerate(documents, 1):
        print(f"\n[{idx}] {doc['filename']}")
        print(f"    Doc ID: {doc['doc_id'][:16]}...")
        print(f"    Votos A FAVOR: {len(doc['votes_approve'])} | CONTRA: {len(doc['votes_reject'])}")
        print(f"    Necessários: {doc['required_votes']}")
    
    print("\n" + "="*70 + "\n")
    return True


def vote_interactive():
    """Loop interativo."""
    global debug_mode
    
    print("\n" + "="*70)
    print("SISTEMA DE VOTAÇÃO (PubSub)")
    print("="*70)
    print("Comandos: list | all | vote <num> approve|reject | debug | quit")
    print("="*70 + "\n")
    
    while running:
        try:
            command = input(">>> ").strip().lower()
            
            if command == 'quit':
                break
            
            elif command == 'list':
                if debug_mode:
                    print(f"\n🔍 DEBUG: Total sessões: {len(voting_sessions)}")
                    for doc_id, s in voting_sessions.items():
                        print(f"   - {doc_id[:12]}... | status={s['status']} | file={s['filename']}")
                
                documents = get_pending_documents()
                display_pending_documents(documents)
            
            elif command == 'all':
                documents = get_all_documents()
                if not documents:
                    print("\n❌ Nenhum documento registado\n")
                else:
                    print("\n" + "="*70)
                    print("TODOS OS DOCUMENTOS")
                    print("="*70)
                    for idx, doc in enumerate(documents, 1):
                        icon = "✅" if doc['status'] == 'approved' else "❌" if doc['status'] == 'rejected' else "⏳"
                        print(f"\n[{idx}] {icon} {doc['filename']} | {doc['status']}")
                    print("\n" + "="*70 + "\n")
            
            elif command == 'debug':
                debug_mode = not debug_mode
                print(f"🔧 Modo debug: {'ATIVADO' if debug_mode else 'DESATIVADO'}")
            
            elif command.startswith('vote '):
                parts = command.split()
                if len(parts) != 3:
                    print("❌ Uso: vote <num> approve|reject")
                    continue
                
                try:
                    doc_num = int(parts[1])
                    vote_type = parts[2]
                    
                    if vote_type not in ['approve', 'reject']:
                        print("❌ Voto deve ser 'approve' ou 'reject'")
                        continue
                    
                    documents = get_pending_documents()
                    
                    if doc_num < 1 or doc_num > len(documents):
                        print(f"❌ Documento {doc_num} não encontrado")
                        continue
                    
                    doc_id = documents[doc_num - 1]['doc_id']
                    filename = documents[doc_num - 1]['filename']
                    
                    print(f"\n🗳️  Votando: {vote_type.upper()} em '{filename}'...")
                    send_vote(doc_id, vote_type)
                
                except ValueError:
                    print("❌ Número inválido")
                except Exception as e:
                    print(f"❌ Erro: {e}")
            
            else:
                print("❌ Comando desconhecido")
        
        except (KeyboardInterrupt, EOFError):
            break
        except Exception as e:
            print(f"❌ Erro: {e}")


if __name__ == "__main__":
    try:
        print("🔍 A verificar conexão IPFS...")
        response = requests.post(f"{IPFS_API_URL}/id", timeout=2)
        
        if response.status_code == 200:
            peer_data = response.json()
            my_peer_id = peer_data['ID']
            
            print(f"\n✅ Conectado ao IPFS")
            print(f"📍 Peer ID: {my_peer_id[:20]}...")
            
            start_pubsub_listener()
            vote_interactive()
            
            print("\n🔌 A desconectar...")
            stop_pubsub_listener()
            print("✅ Desconectado")
        else:
            print("❌ IPFS não responde")
    
    except requests.exceptions.ConnectionError:
        print("❌ Erro ao conectar ao IPFS")
        print("💡 Certifica-te: ipfs daemon --enable-pubsub-experiment")
    except Exception as e:
        print(f"❌ Erro: {e}")