import requests
import json

print("="*60)
print("Upload com Embeddings")
print("="*60)

with open('teste_sprint2.txt', 'rb') as f:
    files = {'file': f}
    response = requests.post('http://25.42.152.214:5000/upload', files=files)
    
    if response.status_code == 200:
        result = response.json()
        print("\n✅ UPLOAD REALIZADO COM SUCESSO!")
        print(f"   └─ CID: {result['cid']}")
        print(f"   └─ Versão vetor: {result['vector_version']}")
        print(f"   └─ Embedding shape: {result['embedding_shape']}")
        print(f"   └─ Propagado: {result['propagated']}")
        print(f"   └─ Gateway: {result['gateway_url']}")
    else:
        print(f"\n❌ ERRO NO UPLOAD: {response.text}")

# Verificar vetor
print("\n" + "="*60)
print("A verificar o vetor de documentos...")
print("="*60)

response = requests.get('http://25.42.152.214:5000/vector')
if response.status_code == 200:
    vector = response.json()
    print(f"\nVersão confirmada: {vector.get('version_confirmed', 0)}")
    print(f"Versão pendente: {vector.get('version_pending', 0)}")
    print(f"Total confirmados: {vector.get('total_confirmed', 0)}")
    print(f"Total pendentes: {vector.get('total_pending', 0)}")
    print(f"Última atualização: {vector.get('last_updated')}")
    
    if vector.get('documents_confirmed'):
        print("\nDocumentos confirmados:")
        for doc in vector['documents_confirmed']:
            print(f"   • {doc.get('filename', 'sem nome')} → {doc.get('cid', 'sem cid')}")
    
    if vector.get('documents_pending'):
        print("\nDocumentos pendentes:")
        for doc in vector['documents_pending']:
            print(f"   • {doc.get('filename', 'sem nome')} → {doc.get('cid', 'sem cid')} (pendente)")
else:
    print(f"\n❌ ERRO AO VERIFICAR O VETOR: {response.text}")
