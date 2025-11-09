import requests
import json
import sys
import time
from datetime import datetime

pending_proposals = {}

def display_voting_menu():
    if not pending_proposals:
        return
    
    print("\n" + "="*60)
    print("DOCUMENTOS PENDENTES DE VOTAÇÃO")
    print("="*60)
    for idx, (doc_id, info) in enumerate(pending_proposals.items(), 1):
        print(f"\n{idx}. {info['filename']}")
        print(f"   Doc ID: {doc_id[:16]}...")
        print(f"   Votos necessários: {info['required_votes']}")
        print(f"   Recebido: {info.get('received_at', 'agora')}")
    print("\n" + "="*60)
    print("Digite 'vote <número> approve' ou 'vote <número> reject'")
    print("Exemplo: vote 1 approve")
    print("="*60 + "\n")

def vote_on_document(doc_number, vote_type):
    try:
        doc_id = list(pending_proposals.keys())[doc_number - 1]
        response = requests.post(f'http://localhost:5000/vote/{doc_id}/{vote_type}')
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ VOTO REGISTADO: {vote_type.upper()}")
            print(f"Status: {result.get('status')}")
            print(f"Votos a favor: {result.get('votes_approve', 0)}")
            print(f"Votos contra: {result.get('votes_reject', 0)}")
            print(f"Necessários: {result.get('required_votes', 0)}")
            
            if result.get('status') in ['approved', 'rejected']:
                print(f"\n🎯 DECISÃO FINAL: {result['status'].upper()}")
                if doc_id in pending_proposals:
                    del pending_proposals[doc_id]
        else:
            print(f"\n❌ Erro ao votar: {response.text}")
    
    except IndexError:
        print("\n❌ Número de documento inválido")
    except Exception as e:
        print(f"\n❌ Erro: {e}")

def connect_to_notifications():
    print("="*60)
    print("A aguardar notificações e propostas de documentos...")
    print("="*60 + "\n")
    
    while True:
        try:
            response = requests.get(
                'http://localhost:5000/notifications',
                stream=True,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Erro na conexão: {response.status_code}. A tentar reconectar em 5 segundos...")
                time.sleep(5)
                continue
            
            print("✅ Conectado ao servidor\n")
            
            for line in response.iter_lines():
                if line:
                    try:
                        if line.startswith(b'data: '):
                            data = line[6:]
                            msg = json.loads(data)
                            msg_type = msg.get('type')
                            
                            if msg_type == 'connected':
                                print(f"📡 {msg['message']} (Canal: {msg['canal']})\n")
                            
                            elif msg_type == 'document_proposal':
                                doc_id = msg['doc_id']
                                filename = msg['filename']
                                required_votes = msg['required_votes']
                                
                                pending_proposals[doc_id] = {
                                    'filename': filename,
                                    'required_votes': required_votes,
                                    'received_at': datetime.now().strftime("%H:%M:%S")
                                }
                                
                                print("\n" + "="*60)
                                print("🗳️  NOVA PROPOSTA DE DOCUMENTO")
                                print("="*60)
                                print(f"Ficheiro: {filename}")
                                print(f"Doc ID: {doc_id}")
                                print(f"Votos necessários: {required_votes}")
                                print(f"De: {msg.get('from_peer', 'unknown')}")
                                print("="*60)
                                print("\nVotar: /vote <doc_id> approve|reject")
                                print(f"Exemplo: /vote {doc_id} approve")
                                print("="*60 + "\n")
                                
                                display_voting_menu()
                            
                            elif msg_type == 'peer_vote':
                                doc_id = msg['doc_id']
                                vote = msg['vote']
                                peer_id = msg.get('peer_id', 'unknown')
                                result = msg.get('result', {})
                                
                                print(f"\n📊 Voto recebido de {peer_id[:16]}...")
                                print(f"   Voto: {vote.upper()}")
                                if result:
                                    print(f"   A favor: {result.get('votes_approve', 0)} | Contra: {result.get('votes_reject', 0)}")
                                    print(f"   Necessários: {result.get('required_votes', 0)}")
                            
                            elif msg_type == 'document_approved':
                                doc_id = msg['doc_id']
                                if doc_id in pending_proposals:
                                    del pending_proposals[doc_id]
                                
                                print("\n" + "="*60)
                                print("✅ DOCUMENTO APROVADO")
                                print("="*60)
                                print(f"Ficheiro: {msg['filename']}")
                                print(f"CID: {msg.get('cid', 'N/A')}")
                                print(f"Votos a favor: {msg.get('votes_approve', 0)}")
                                print(f"Votos contra: {msg.get('votes_reject', 0)}")
                                print("="*60 + "\n")
                            
                            elif msg_type == 'document_rejected':
                                doc_id = msg['doc_id']
                                if doc_id in pending_proposals:
                                    del pending_proposals[doc_id]
                                
                                print("\n" + "="*60)
                                print("❌ DOCUMENTO REJEITADO")
                                print("="*60)
                                print(f"Ficheiro: {msg['filename']}")
                                print(f"Votos a favor: {msg.get('votes_approve', 0)}")
                                print(f"Votos contra: {msg.get('votes_reject', 0)}")
                                print("="*60 + "\n")
                            
                            elif msg_type == 'error':
                                print(f"Erro: {msg['message']}")
                    
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        print(f"⚠️  Erro ao processar: {e}")
                        continue
        
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {str(e)}")
            print("A tentar reconectar em 5 segundos...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n\nA encerrar o listener...")
            sys.exit(0)
        except Exception as e:
            print(f"Erro inesperado: {e}")
            time.sleep(5)

if __name__ == "__main__":
    connect_to_notifications()
