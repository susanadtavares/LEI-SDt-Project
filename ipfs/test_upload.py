import requests
import time

def test_upload():
    print("="*60)
    print("TESTE DE UPLOAD")
    print("="*60)
    
    # Cria ficheiro de teste
    test_content = f"Documento de teste criado em {time.strftime('%Y-%m-%d %H:%M:%S')}"
    filename = f"teste_{int(time.time())}.txt"
    
    with open(filename, 'w') as f:
        f.write(test_content)
    
    print(f"\n📤 A enviar '{filename}' para o líder...")
    
    try:
        # Upload via HTTP
        with open(filename, 'rb') as f:
            files = {'file': (filename, f)}
            response = requests.post('http://localhost:5000/upload', files=files, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            print("\n✅ UPLOAD REALIZADO!")
            print(f"   └─ Doc ID: {result['doc_id']}")
            print(f"   └─ Ficheiro: {result['filename']}")
            print(f"   └─ Status: {result['status']}")
            print(f"   └─ Votos necessários: {result['required_votes']}")
            
            print("\n⏳ Aguardando votação e processamento...")
            print("   (Os peers vão votar automaticamente)")
            
            # Aguarda processamento
            for i in range(30):
                time.sleep(1)
                try:
                    status_response = requests.get('http://localhost:5000/status')
                    if status_response.status_code == 200:
                        status = status_response.json()
                        print(f"\r   Versão confirmada: {status.get('version_confirmed', 0)} | FAISS: {status.get('faiss_vectors', 0)} vetores", end='', flush=True)
                except:
                    pass
            
            print("\n\n✅ Processamento concluído!")
            print("="*60 + "\n")
        
        elif response.status_code == 403:
            error = response.json()
            print(f"\n❌ ERRO: {error.get('error')}")
            print(f"   Líder atual: {error.get('leader_id', 'unknown')}")
        
        else:
            print(f"\n❌ ERRO: {response.text}")
    
    except requests.exceptions.ConnectionError:
        print("\n❌ Não foi possível conectar ao servidor")
        print("   Certifica-te que o servidor está a correr: python ipfs/server.py")
    except Exception as e:
        print(f"\n❌ Erro: {e}")


if __name__ == "__main__":
    test_upload()
