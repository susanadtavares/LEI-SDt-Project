import requests
import json
import time

def get_pending_documents():
    try:
        response = requests.get('http://localhost:5000/voting-status')
        if response.status_code == 200:
            data = response.json()
            pending = [s for s in data['sessions'] if s['status'] == 'pending_approval']
            return pending
        return []
    except:
        return []

def display_pending_documents(documents):
    if not documents:
        print("\n❌ Nenhum documento pendente de votação\n")
        return False
    
    print("\n" + "="*70)
    print("DOCUMENTOS PENDENTES DE VOTAÇÃO")
    print("="*70)
    
    for idx, doc in enumerate(documents, 1):
        print(f"\n[{idx}] {doc['filename']}")
        print(f"    Doc ID: {doc['doc_id']}")
        print(f"    Status: {doc['status']}")
        print(f"    Votos A FAVOR: {doc['votes_approve']} | CONTRA: {doc['votes_reject']}")
        print(f"    Necessários para decisão: {doc['required_votes']}")
        print(f"    Total de peers: {doc['total_peers']}")
        print(f"    Criado em: {doc['created_at']}")
    
    print("\n" + "="*70 + "\n")
    return True

def vote_interactive():
    print("\n" + "="*70)
    print("SISTEMA DE VOTAÇÃO")
    print("="*70)
    print("Comandos:")
    print("  • list - Listar documentos pendentes")
    print("  • vote <número> approve - Aprovar documento")
    print("  • vote <número> reject - Rejeitar documento")
    print("  • status <número> - Ver status detalhado")
    print("  • quit - Sair")
    print("="*70 + "\n")
    
    while True:
        try:
            command = input(">>> ").strip().lower()
            
            if command == 'quit':
                break
            
            elif command == 'list':
                documents = get_pending_documents()
                display_pending_documents(documents)
            
            elif command.startswith('vote '):
                parts = command.split()
                if len(parts) != 3:
                    print("❌ Uso: vote <número> approve|reject")
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
                    
                    print(f"\n🗳️  A enviar voto: {vote_type.upper()} para documento #{doc_num}...")
                    
                    response = requests.post(f'http://localhost:5000/vote/{doc_id}/{vote_type}')
                    
                    if response.status_code == 200:
                        result = response.json()
                        status = result.get('status')
                        
                        print("\n" + "="*70)
                        if status == 'approved':
                            print("✅ DOCUMENTO APROVADO!")
                            print(f"CID: {result.get('cid', 'N/A')}")
                        elif status == 'rejected':
                            print("❌ DOCUMENTO REJEITADO!")
                        else:
                            print("📊 VOTO REGISTADO")
                        
                        print("="*70)
                        print(f"Votos A FAVOR: {result.get('votes_approve', 0)}")
                        print(f"Votos CONTRA: {result.get('votes_reject', 0)}")
                        print(f"Necessários: {result.get('required_votes', 0)}")
                        if status == 'voting':
                            print(f"Faltam: {result.get('votes_remaining', 0)} votos")
                        print("="*70 + "\n")
                    else:
                        print(f"❌ Erro: {response.text}\n")
                
                except ValueError:
                    print("❌ Número de documento inválido")
                except Exception as e:
                    print(f"❌ Erro: {e}")
            
            elif command.startswith('status '):
                try:
                    doc_num = int(command.split()[1])
                    documents = get_pending_documents()
                    
                    if doc_num < 1 or doc_num > len(documents):
                        print(f"❌ Documento {doc_num} não encontrado")
                        continue
                    
                    doc_id = documents[doc_num - 1]['doc_id']
                    response = requests.get(f'http://localhost:5000/voting-status/{doc_id}')
                    
                    if response.status_code == 200:
                        data = response.json()
                        print("\n" + "="*70)
                        print("STATUS DETALHADO")
                        print("="*70)
                        print(f"Ficheiro: {data['filename']}")
                        print(f"Doc ID: {data['doc_id']}")
                        print(f"Status: {data['status']}")
                        print(f"Votos A FAVOR: {data['votes_approve']}")
                        print(f"Votos CONTRA: {data['votes_reject']}")
                        print(f"Necessários: {data['required_votes']}")
                        print(f"Faltam: {data.get('votes_remaining', 0)}")
                        print(f"Total peers: {data['total_peers']}")
                        print("="*70 + "\n")
                
                except (ValueError, IndexError):
                    print("❌ Uso: status <número>")
            
            else:
                print("❌ Comando desconhecido. Digite 'list', 'vote', 'status' ou 'quit'")
        
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Erro: {e}")

if __name__ == "__main__":
    # Verificar conexão
    try:
        response = requests.get('http://localhost:5000/status', timeout=2)
        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Conectado ao servidor")
            print(f"Peer ID: {data.get('peer_id', 'unknown')[:20]}...")
            print(f"Peers conectados: {data.get('connected_peers', 1)}")
            vote_interactive()
        else:
            print("❌ Servidor não responde")
    except:
        print("❌ Não foi possível conectar ao servidor")
        print("Certifica-te que o servidor está a correr")
