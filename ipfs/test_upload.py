import requests
import json

print("="*60)
print("Upload com Embeddings")
print("="*60)

with open('teste_sprint2.txt', 'rb') as f:
    files = {'file': f}
    response = requests.post('http://localhost:5000/upload', files=files)
    
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

response = requests.get('http://localhost:5000/vector')
if response.status_code == 200:
    vector = response.json()
    print(f"\nVersão: {vector['version']}")
    print(f"Total de documentos: {vector['total_documents']}")
    print(f"Última atualização: {vector['last_updated']}")
    print("\nDocumentos:")
    for doc in vector['documents']:
        print(f"   • {doc['filename']} → {doc['cid']}")
else:
    print(f"\n❌ ERRO AO VERIFICAR O VETOR: {response.text}")
