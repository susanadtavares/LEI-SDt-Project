import requests
import sys

URL = "http://localhost:5000"

def upload_file(filename: str):
    try:
        print(f"\n{'='*60}")
        print(f"📤 A enviar: {filename}")
        print(f"{'='*60}\n")
        
        with open(filename, 'rb') as f:
            files = {'file': (filename, f)}
            response = requests.post(
                f"{URL}/upload",
                files=files,
                timeout=5
            )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Upload bem-sucedido!")
            print(f"\n📋 Detalhes:")
            print(f"   ID do documento: {result['doc_id']}")
            print(f"   Ficheiro: {result['filename']}")
            print(f"   Votos necessários: {result['required_votes']}")
            print(f"   Total de peers: {result['total_peers']}")
            print(f"\n⏳ A aguardar votação...\n")
        else:
            print(f"\n❌ Erro {response.status_code}: {response.text}\n")
    
    except FileNotFoundError:
        print(f"\n❌ Ficheiro '{filename}' não encontrado!\n")
    except requests.exceptions.ConnectionError:
        print(f"\n❌ Não foi possível conectar ao líder em {URL}")
    except Exception as e:
        print(f"\n❌ Erro: {e}\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("\nUso: python test_upload.py <ficheiro>")
        sys.exit(1)
    
    filename = sys.argv[1]
    upload_file(filename)
