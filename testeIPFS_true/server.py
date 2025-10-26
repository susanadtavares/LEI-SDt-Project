from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import requests
import uvicorn

app = FastAPI(title="IPFS Upload API")

# URL da API HTTP do IPFS Desktop
IPFS_API_URL = "http://127.0.0.1:5001/api/v0"

@app.get("/")
def root():
    """Verifica se API está ativa"""
    return {
        "message": "API IPFS ativa",
        "status": "running",
        "endpoints": {
            "upload": "/upload (POST)",
            "info": "/info/{cid} (GET)"
        }
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Adiciona ficheiro ao IPFS via HTTP API"""
    try:
        filename = file.filename
        content = await file.read()
        
        print(f"📁 Ficheiro recebido: {filename}")
        
        # Adicionar ao IPFS via HTTP API
        files = {'file': (filename, content)}
        response = requests.post(
            f"{IPFS_API_URL}/add",
            files=files,
            params={'pin': 'true'}
        )
        
        if response.status_code != 200:
            print(f"❌ Erro IPFS: {response.text}")
            return JSONResponse(
                content={
                    'error': 'Falha ao adicionar ao IPFS',
                    'details': response.text
                },
                status_code=500
            )
        
        result = response.json()
        cid = result['Hash']
        
        print(f"✅ CID gerado: {cid}")
        
        # Opcional: Publicar via PubSub
        try:
            requests.post(
                f"{IPFS_API_URL}/pubsub/pub",
                params={
                    'arg': 'canal-ficheiros',
                    'arg': f'Novo ficheiro: {filename} - CID: {cid}'
                }
            )
            print(f"📢 Mensagem publicada no PubSub")
        except Exception as e:
            print(f"⚠️  PubSub falhou (normal se não houver peers): {e}")
        
        return {
            'status': 'success',
            'filename': filename,
            'cid': cid,
            'size': result.get('Size', 'unknown'),
            'gateway_url': f'http://localhost:8080/ipfs/{cid}'
        }
    
    except requests.exceptions.ConnectionError:
        print("❌ Não foi possível conectar ao IPFS Desktop")
        return JSONResponse(
            content={
                'error': 'IPFS Desktop não está a correr',
                'solution': 'Abre o IPFS Desktop e tenta novamente'
            },
            status_code=503
        )
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/info/{cid}")
def file_info(cid: str):
    """Obtém informações sobre um CID"""
    try:
        response = requests.post(
            f"{IPFS_API_URL}/object/stat",
            params={'arg': cid}
        )
        
        if response.status_code != 200:
            return JSONResponse(
                content={'error': 'CID não encontrado'},
                status_code=404
            )
        
        return {
            'cid': cid,
            'info': response.json(),
            'gateway_url': f'http://localhost:8080/ipfs/{cid}'
        }
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/status")
def ipfs_status():
    """Verifica se IPFS Desktop está acessível"""
    try:
        response = requests.post(f"{IPFS_API_URL}/version")
        if response.status_code == 200:
            version_info = response.json()
            return {
                'status': 'connected',
                'ipfs_version': version_info.get('Version'),
                'message': 'IPFS Desktop está ativo'
            }
    except:
        return JSONResponse(
            content={
                'status': 'disconnected',
                'message': 'IPFS Desktop não está acessível. Abre a aplicação.'
            },
            status_code=503
        )

if __name__ == "__main__":
    print("🚀 A iniciar API IPFS...")
    print("📡 Servidor: http://localhost:5000")
    print("📚 Documentação: http://localhost:5000/docs")
    print("\n⚠️  Certifica-te que o IPFS Desktop está a correr!")
    print("   (Verifica o ícone na barra de tarefas)\n")
    uvicorn.run(app, host="0.0.0.0", port=5000)
