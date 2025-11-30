import requests
import time
import sys

URL = "http://localhost:5000/upload"

def test_upload():
    print("\n" + "="*60)
    print("📤 TESTE DE UPLOAD ")
    print("="*60)
    
    filename = f"documento_{int(time.time())}.txt"
    content = f"Documento de teste criado em {time.strftime('%Y-%m-%d %H:%M:%S')}."
    
    with open(filename, 'w') as f:
        f.write(content)
        
    print(f"📄 Ficheiro criado: {filename}")
    print(f"📡 A tentar conectar a: {URL}")
    
    try:
        with open(filename, 'rb') as f:
            files = {'file': (filename, f)}
            # Timeout curto para falhar rápido se servidor não existir
            response = requests.post(URL, files=files, timeout=5)
            
        if response.status_code == 200:
            print("\n✅ UPLOAD REALIZADO!")
            print(f"   └─ Doc ID: {result['doc_id']}")
            print(f"   └─ Ficheiro: {result['filename']}")
            print(f"   └─ Status: {result['status']}")
            print(f"   └─ Votos necessários: {result['required_votes']}")

            print("\n⏳ Aguardando votação e processamento...")

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

            print(f"   {response.json()}")
        elif response.status_code == 403:
            error = response.json()
            print(f"\n❌ ERRO: {error.get('error')}")
            print(f"   Líder atual: {error.get('leader_id', 'desconhecido')}")
        else:
            print(f"\n❌ FALHA: {response.status_code}")
            print(f"   {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("\n❌ Não foi possível conectar ao servidor")
    except Exception as e:
        print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    test_upload()
