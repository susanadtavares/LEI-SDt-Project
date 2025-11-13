"""
Exemplo de upload de um ficheiro para votação e verificação de estado.

O script faz o seguinte:
- Envia o ficheiro `teste_sprint3.txt` para o endpoint `/upload` do servidor local.
- Se o upload for aceite, monitora o estado da votação consultando repetidamente `/voting-status/<doc_id>` até a votação terminar (aprovação ou rejeição) ou até esgotar o tempo de espera.
- No final, consulta o vetor global (`/vector`) para apresentar um resumo dos documentos confirmados/rejeitados.

Pressupostos:
- O servidor de backend está a correr em `http://localhost:5000` e expõe os endpoints usados (`/upload`, `/voting-status/<id>`, `/vector`).
"""

import requests
import json
import time

print("="*60)
print("Upload com Embeddings")
print("="*60)

print("\n📤 A enviar ficheiro para votação...\n")

# Abre o ficheiro local que será enviado. Usa modo 'rb' para enviar como multipart/form-data (files=...). O nome do ficheiro é `teste_sprint3.txt`.
with open('teste_sprint3.txt', 'rb') as f:
    files = {'file': f}
    # Envia o ficheiro para o endpoint de upload do servidor
    response = requests.post('http://localhost:5000/upload', files=files)
    
    if response.status_code == 200:
        # Caso sucesso, o servidor devolve informações sobre a sessão de votação
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
        
        # Monitorizar o estado da votação durante um período (30s por defeito)
        for i in range(30):
            time.sleep(1)
            
            try:
                # Consulta o estado atual da votação para o doc_id
                status_response = requests.get(f'http://localhost:5000/voting-status/{doc_id}')
                if status_response.status_code == 200:
                    status = status_response.json()
                    
                    # Se a votação terminou, imprime o resultado e sai do loop
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
                        # Mostra um resumo em linha (sem quebrar o terminal)
                        print(f"\r🗳️  Votação a decorrer... A favor: {status['votes_approve']} | Contra: {status['votes_reject']} | Necessários: {status['required_votes']}", end='', flush=True)
            except:
                # Ignora erros temporários (ex.: timeout)
                pass
        
        else:
            # Se o loop terminar sem decisão final, informa o utilizador
            print("\n\n⏱️  Tempo de espera excedido. Verifica o status manualmente.")
    
    else:
        # Em caso de falha no upload, imprime o texto de erro retornado
        print(f"\n❌ ERRO NO UPLOAD: {response.text}")


# Após o fluxo de upload/votação, consultamos o vetor global para obter um resumo do estado dos documentos no sistema.
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
        
        # Mostra até 3 documentos confirmados/rejeitados como exemplo
        if vector.get('documents_confirmed'):
            print("\n✅ Documentos confirmados:")
            for doc in vector['documents_confirmed'][-3:]:  # últimos 3
                print(f"   • {doc.get('filename')} → {doc.get('cid')}")
        
        if vector.get('documents_rejected'):
            print("\n❌ Documentos rejeitados:")
            for doc in vector['documents_rejected'][-3:]:
                print(f"   • {doc.get('filename')} (rejeitado)")
except Exception as e:
    # Em caso de erro ao consultar o vetor, imprime a exceção
    print(f"Erro: {e}")

print("\n" + "="*60 + "\n")
