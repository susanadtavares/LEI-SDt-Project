from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
import requests
import uvicorn
import json
import base64

app = FastAPI(title="IPFS Upload API")

IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"

@app.get("/")
def root():
    """Verifica se API está ativa"""
    return {
        "message": "API IPFS ativa",
        "status": "running",
        "version": "2.0",
        "endpoints": {
            "upload": "/upload (POST)",
            "info": "/info/{cid} (GET)",
            "status": "/status (GET)",
            "peers": "/peers (GET)",
            "my-peer-info": "/my-peer-info (GET)",
            "connect": "/connect (POST)",
            "notifications": "/notifications (GET - SSE Stream)",
            "docs": "/docs"
        }
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Adiciona ficheiro ao IPFS via HTTP API e notifica via PubSub"""
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
        
        # Publicar notificação no PubSub
        try:
            mensagem = f'Novo ficheiro: {filename} - CID: {cid}'
            
            requests.post(
                f"{IPFS_API_URL}/pubsub/pub",
                params={
                    'arg': CANAL_PUBSUB,
                    'arg': mensagem
                }
            )
            print(f"📢 Mensagem publicada no PubSub: {mensagem}")
        except Exception as e:
            print(f"⚠️  PubSub falhou (normal se não houver peers): {e}")
        
        return {
            'status': 'success',
            'filename': filename,
            'cid': cid,
            'size': result.get('Size', 'unknown'),
            'gateway_url': f'http://localhost:8080/ipfs/{cid}',
            'download_url': f'http://localhost:8080/ipfs/{cid}?download=true'
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
            'gateway_url': f'http://localhost:8080/ipfs/{cid}',
            'download_url': f'http://localhost:8080/ipfs/{cid}?download=true'
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
        response = requests.post(f"{IPFS_API_URL}/version", timeout=5)
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

@app.get("/peers")
def list_connected_peers():
    """Lista peers conectados"""
    try:
        response = requests.post(f"{IPFS_API_URL}/swarm/peers", timeout=10)
        if response.status_code == 200:
            peers = response.json().get('Peers', [])
            
            return {
                'peer_count': len(peers),
                'peers': [
                    {
                        'id': p['Peer'],
                        'addr': p.get('Addr', 'unknown')
                    }
                    for p in peers[:20]  # Mostrar max 20
                ],
                'total_available': len(peers)
            }
    except Exception as e:
        return {'error': str(e)}, 500

@app.get("/my-peer-info")
def get_my_peer_info():
    """
    Retorna informações do teu peer IPFS
    Os colegas usam isto para saber como se conectar a ti
    """
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            # Filtrar endereços úteis
            useful_addrs = [
                addr for addr in data['Addresses']
                if '/ip4/' in addr and '/127.0.0.1' not in addr
            ]
            
            return {
                'peer_id': data['ID'],
                'addresses': useful_addrs[:5] if useful_addrs else data['Addresses'][:5],
                'all_addresses': data['Addresses'],
                'message': 'Os teus colegas devem usar um destes endereços'
            }
    except Exception as e:
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.post("/connect")
def connect_peer(peer_multiaddr: str):
    """
    Permite que um peer se conecte ao teu nó IPFS
    Os teus colegas chamam este endpoint com o multiaddr deles
    """
    try:
        print(f"🔗 Tentando conectar ao peer: {peer_multiaddr[:50]}...")
        
        response = requests.post(
            f"{IPFS_API_URL}/swarm/connect",
            params={'arg': peer_multiaddr},
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"✅ Peer conectado!")
            return {
                'status': 'connected',
                'message': 'Peer conectado com sucesso!',
                'peer': peer_multiaddr[:50] + '...'
            }
        else:
            print(f"❌ Falha: {response.text}")
            return JSONResponse(
                content={'error': 'Falha na conexão', 'details': response.text},
                status_code=500
            )
    
    except Exception as e:
        print(f"❌ Erro: {e}")
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/notifications")
async def get_notifications():
    """
    Stream de notificações em tempo real (Server-Sent Events)
    Os colegas conectam-se aqui para receber avisos de novos ficheiros
    """
    async def event_stream():
        """Gera eventos Server-Sent Events (SSE)"""
        
        try:
            print(f"📡 Novo cliente conectado ao stream de notificações")
            
            # Conectar ao PubSub do IPFS
            response = requests.post(
                f"{IPFS_API_URL}/pubsub/sub",
                params={'arg': CANAL_PUBSUB},
                stream=True,
                timeout=None
            )
            
            # Enviar mensagem inicial
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Conectado ao canal de notificações', 'canal': CANAL_PUBSUB})}\n\n"
            
            # Ler mensagens do PubSub
            for line in response.iter_lines():
                if line:
                    try:
                        # Parsear mensagem do IPFS
                        msg = json.loads(line)
                        data_encoded = msg.get('data', '')
                        
                        # Decodificar base64
                        try:
                            data_decoded = base64.b64decode(data_encoded).decode('utf-8')
                        except:
                            data_decoded = data_encoded
                        
                        # Construir notificação
                        notification = {
                            'type': 'new_file',
                            'message': data_decoded,
                            'from': msg.get('from', 'unknown')[:20] + '...',
                            'timestamp': str(msg.get('seqno', ''))
                        }
                        
                        # Enviar ao cliente via SSE
                        yield f"data: {json.dumps(notification)}\n\n"
                        print(f"📢 Notificação enviada: {data_decoded[:50]}...")
                        
                    except Exception as e:
                        print(f"⚠️  Erro ao processar mensagem: {e}")
                        continue
        
        except Exception as e:
            print(f"❌ Erro no stream: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': f'Erro: {str(e)}'})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

# ============= INIT =============

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 IPFS Upload API v2.0")
    print("="*60)
    print(f"📡 Servidor: http://0.0.0.0:5000")
    print(f"📚 Documentação: http://localhost:5000/docs")
    print(f"📢 Canal PubSub: {CANAL_PUBSUB}")
    print("="*60)
    print("\n⚠️  IMPORTANTE:")
    print("   • Certifica-te que o IPFS Desktop está a correr")
    print("   • Verifica o ícone na barra de tarefas")
    print("   • API IPFS deve estar em: http://127.0.0.1:5001")
    print("="*60 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info"
    )
