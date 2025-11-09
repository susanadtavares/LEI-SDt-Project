import requests
import json
import time

print("="*60)
print("Upload com Embeddings")
print("="*60)

print("\n📤 A enviar ficheiro para votação...\n")

with open('teste_sprint3.txt', 'rb') as f:
    files = {'file': f}
    response = requests.post('http://localhost:5000/upload', files=files)
    
    if response.status_code == 200:
        result = response.json()
        doc_id = result['doc_id']
        
        print("✅ FICHEIRO ENVIADO PARA VOTAÇÃO!")
        print(f"   └─ Doc ID: {doc_id}")
        print(f"   └─ Ficheiro: {result['filename']}")
        print(f"   └─ Status: {result['status']}")
        print(f"   └─ Total peers: {result['total_peers']}")
        print(f"   └─ Votos necessários: {result['required_votes']}")
        print(f"   └─ Propagado: {result['propagated']}")
        
        print("\n" + "="*60)
        print("⏳ A aguardar votação dos peers...")
        print("="*60)
        
        # Monitorizar status da votação
        for i in range(30):
            time.sleep(1)
            
            try:
                status_response = requests.get(f'http://localhost:5000/voting-status/{doc_id}')
                if status_response.status_code == 200:
                    status = status_response.json()
                    
                    if status['status'] in ['approved', 'rejected']:
                        print("\n" + "="*60)
                        if status['status'] == 'approved':
                            print("✅ DOCUMENTO APROVADO!")
                        else:
                            print("❌ DOCUMENTO REJEITADO!")
                        print("="*60)
                        print(f"Votos a favor: {status['votes_approve']}")
                        print(f"Votos contra: {status['votes_reject']}")
                        print(f"Necessários: {status['required_votes']}")
                        if status.get('final_decision'):
                            print(f"Decisão final: {status['final_decision'].upper()}")
                        print("="*60 + "\n")
                        break
                    
                    else:
                        print(f"\r🗳️  Votação a decorrer... A favor: {status['votes_approve']} | Contra: {status['votes_reject']} | Necessários: {status['required_votes']}", end='', flush=True)
            except:
                pass
        
        else:
            print("\n\n⏱️  Tempo de espera excedido. Verifica o status manualmente.")
    
    else:
        print(f"\n❌ ERRO NO UPLOAD: {response.text}")

# Verificar vetor
print("\n" + "="*60)
print("A verificar o vetor de documentos...")
print("="*60)

try:
    response = requests.get('http://localhost:5000/vector')
    if response.status_code == 200:
        vector = response.json()
        print(f"\nVersão confirmada: {vector.get('version_confirmed', 0)}")
        print(f"Total confirmados: {vector.get('total_confirmed', 0)}")
        print(f"Total rejeitados: {vector.get('total_rejected', 0)}")
        print(f"Pendentes de aprovação: {vector.get('total_pending_approval', 0)}")
        
        if vector.get('documents_confirmed'):
            print("\n✅ Documentos confirmados:")
            for doc in vector['documents_confirmed'][-3:]:  # U3
                print(f"   • {doc.get('filename')} → {doc.get('cid')}")
        
        if vector.get('documents_rejected'):
            print("\n❌ Documentos rejeitados:")
            for doc in vector['documents_rejected'][-3:]:
                print(f"   • {doc.get('filename')} (rejeitado)")
except Exception as e:
    print(f"Erro: {e}")

print("\n" + "="*60 + "\n")
