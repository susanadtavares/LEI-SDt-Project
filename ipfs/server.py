from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from sentence_transformers import SentenceTransformer
import requests
import uvicorn
import json
import base64
import numpy as np
import os
from pathlib import Path
from datetime import datetime
import hashlib

app = FastAPI(title="IPFS Upload API")

IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"

VECTOR_FILE = "document_vector.json"
EMBEDDINGS_DIR = "embeddings"

Path(EMBEDDINGS_DIR).mkdir(exist_ok=True)

print("A carregar modelo SentenceTransformer...")
embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
print("Modelo carregado com sucesso!")

def load_document_vector():
    """Carrega vetor de documentos do ficheiro JSON"""
    if os.path.exists(VECTOR_FILE):
        with open(VECTOR_FILE, 'r') as f:
            return json.load(f)
    else:
        return {
            "version_confirmed": 0,
            "version_pending": 0,
            "documents_confirmed": [],
            "documents_pending": [],
            "last_updated": None
        }

def save_document_vector(vector_data):
    """Guarda vetor de documentos no ficheiro JSON"""
    with open(VECTOR_FILE, 'w') as f:
        json.dump(vector_data, f, indent=2)
    print(f"Vetor guardado - Versão confirmada: {vector_data.get('version_confirmed', 0)}")

def add_document_to_vector(cid, filename, embeddings, confirmed=False):
    """
    Cria nova versão do vetor de documentos.
    Se confirmed=False → cria versão temporária (pendente).
    """
    vector = load_document_vector()

    # Se for a primeira execução, garantir chaves
    vector.setdefault("version_confirmed", 0)
    vector.setdefault("version_pending", vector["version_confirmed"])
    vector.setdefault("documents_confirmed", [])
    vector.setdefault("documents_pending", [])

    # Incrementa versão pendente
    new_version = vector["version_pending"] + 1

    doc_entry = {
        "cid": cid,
        "filename": filename,
        "added_at": datetime.now().isoformat(),
        "embedding_shape": embeddings.shape,
        "embedding_file": f"{EMBEDDINGS_DIR}/{cid}.npy",
        "confirmed": confirmed
    }

    if confirmed:
        vector["documents_confirmed"].append(doc_entry)
        vector["version_confirmed"] = new_version
        print(f"✅ Documento confirmado — versão {new_version}")
    else:
        vector["documents_pending"].append(doc_entry)
        vector["version_pending"] = new_version
        print(f"🕒 Documento pendente — versão {new_version}")

    vector["last_updated"] = datetime.now().isoformat()

    # Guarda embedding
    np.save(f"{EMBEDDINGS_DIR}/{cid}.npy", embeddings)
    save_document_vector(vector)

    return new_version, vector

def generate_embeddings(text_content):
    """
    Cria embeddings utilizando SentenceTransformer
    Retorna: numpy array com shape (384,)
    """
    try:
        # Cria o embedding
        embedding = embedding_model.encode(text_content, convert_to_numpy=True)
        print(f"Embedding criado - Shape: {embedding.shape}")
        return embedding
    except Exception as e:
        print(f"Erro ao criar embedding: {e}")
        raise

def extract_text_from_file(content, filename):
    """
    Extrai texto do ficheiro para criar embeddings
    Por enquanto trata apenas ficheiros de texto
    """
    try:
        # Tentar decodificar como UTF-8
        text = content.decode('utf-8')
        return text
    except UnicodeDecodeError:
        # Se não for texto, usa filename e metadata
        print(f"Ficheiro binário, utiliza filename e metadata")
        return f"Document: {filename}"

def propagate_to_peers(version, cid, filename, embeddings):
    """
    Propaga [versão, CID, embeddings] para os peers via PubSub
    """
    try:
        # Prepara a mensagem
        message_data = {
            "type": "new_document",
            "version": version,
            "cid": cid,
            "filename": filename,
            "embedding_shape": list(embeddings.shape),
            "embedding_hash": hashlib.sha256(embeddings.tobytes()).hexdigest()[:16],
            "timestamp": datetime.now().isoformat()
        }
        
        # Converte embeddings para lista para JSON
        message_data["embeddings"] = embeddings.tolist()
        
        message_json = json.dumps(message_data)
        
        # Publica via PubSub
        response = requests.post(
            f"{IPFS_API_URL}/pubsub/pub",
            params={'arg': CANAL_PUBSUB},
            data=message_json.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=5
        )
        
        if response.status_code == 200:
            print(f"Transmissão para os peers:")
            print(f"   └─ Versão: {version}")
            print(f"   └─ CID: {cid}")
            print(f"   └─ Embeddings: {embeddings.shape}")
            return True
        else:
            print(f"Falha na transmissão: {response.text}")
            return False
            
    except Exception as e:
        print(f"Erro na transmissão: {e}")
        return False

@app.get("/")
def root():
    """Verifica se API está ativa"""
    vector = load_document_vector()
    return {
        "message": "API IPFS ativa",
        "status": "running",
        "version": "2.0",
        "document_vector_version": vector.get("version_confirmed", 0),
        "total_documents": len(vector.get("documents_confirmed", [])),
        "endpoints": {
            "upload": "/upload (POST)",
            "vector": "/vector (GET)",
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
    """
    1. Recebe documento
    2. Adiciona ao IPFS
    3. Cria embeddings
    4. Atualiza o vetor de CIDs
    5. Transmite para os peers
    """
    try:
        filename = file.filename
        content = await file.read()
        
        print(f"\n{'='*60}")
        print(f"NOVO UPLOAD: {filename}")
        print(f"{'='*60}")
        
        # Adicionar ao IPFS via HTTP API
        files = {'file': (filename, content)}
        response = requests.post(
            f"{IPFS_API_URL}/add",
            files=files,
            params={'pin': 'true'}
        )
        
        if response.status_code != 200:
            return JSONResponse(
                content={'error': 'Falha ao adicionar ao IPFS', 'details': response.text},
                status_code=500
            )
        
        result = response.json()
        cid = result['Hash']
        print(f"✅ [1/4] CID criado: {cid}")
        
        # Criar embeddings
        try:
            text_content = extract_text_from_file(content, filename)
            embeddings = generate_embeddings(text_content)
            print(f"✅ [2/4] Embeddings criados: shape {embeddings.shape}")
        except Exception as e:
            print(f"Erro ao criar embeddings: {e}")
            return JSONResponse(
                content={'error': f'Falha ao criar embeddings: {str(e)}'},
                status_code=500
            )
        
        # Atualizar o vetor de documentos
        try:
            new_version, updated_vector = add_document_to_vector(cid, filename, embeddings)
            print(f"✅ [3/4] Vetor atualizado para versão {new_version}")
        except Exception as e:
            print(f"Erro ao atualizar vetor: {e}")
            return JSONResponse(
                content={'error': f'Falha ao atualizar vetor: {str(e)}'},
                status_code=500
            )
        
        # Transmissão para os peers
        propagation_success = propagate_to_peers(new_version, cid, filename, embeddings)
        print(f"✅ [4/4] Transmissão para os peers: {'Sucesso' if propagation_success else 'Ocorreu uma falha'}")
        
        print(f"{'='*60}\n")
        
        return {
            'status': 'success',
            'filename': filename,
            'cid': cid,
            'size': result.get('Size', 'unknown'),
            'vector_version': new_version,
            'embedding_shape': list(embeddings.shape),
            'propagated': propagation_success,
            'gateway_url': f'http://localhost:8080/ipfs/{cid}',
            'download_url': f'http://localhost:8080/ipfs/{cid}?download=true'
        }
    
    except requests.exceptions.ConnectionError:
        return JSONResponse(
            content={
                'error': 'IPFS Desktop não está a correr',
                'solution': 'Abre o IPFS Desktop e tenta novamente'
            },
            status_code=503
        )
    except Exception as e:
        print(f"Erro geral: {str(e)}")
        return JSONResponse(
            content={'error': str(e)},
            status_code=500
        )

@app.get("/vector")
def get_document_vector():
    """Retorna o vetor atual de documentos (confirmados e pendentes)"""
    vector = load_document_vector()
    return {
        'version_confirmed': vector.get('version_confirmed', 0),
        'version_pending': vector.get('version_pending', 0),
        'total_confirmed': len(vector.get('documents_confirmed', [])),
        'total_pending': len(vector.get('documents_pending', [])),
        'last_updated': vector.get('last_updated'),
        'documents_confirmed': vector.get('documents_confirmed', []),
        'documents_pending': vector.get('documents_pending', [])
    }

@app.get("/vector/{cid}/embedding")
def get_embedding(cid: str):
    """Retorna o embedding de um documento específico"""
    try:
        embedding_path = f"{EMBEDDINGS_DIR}/{cid}.npy"
        if not os.path.exists(embedding_path):
            return JSONResponse(
                content={'error': 'Embedding não encontrado'},
                status_code=404
            )
        
        embedding = np.load(embedding_path)
        return {
            'cid': cid,
            'shape': list(embedding.shape),
            'embedding': embedding.tolist()
        }
    except Exception as e:
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
        
        # Verificar se temos embedding
        has_embedding = os.path.exists(f"{EMBEDDINGS_DIR}/{cid}.npy")
        
        return {
            'cid': cid,
            'info': response.json(),
            'has_embedding': has_embedding,
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
            vector = load_document_vector()
            return {
                'status': 'connected',
                'ipfs_version': version_info.get('Version'),
                'vector_version': vector.get('version_confirmed', 0),
                'total_documents': len(vector.get('documents_confirmed', [])),
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
    """Retorna informações do peer IPFS local"""
    try:
        response = requests.post(f"{IPFS_API_URL}/id", timeout=10)
        if response.status_code == 200:
            data = response.json()
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
    Permite conectar a um peer IPFS
    Outros peers devem usar este endpoint para conectar com o multiaddr
    """
    try:
        print(f"A tentar conectar ao peer: {peer_multiaddr[:50]}...")
        
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
    Recebe vetores de CIDs e embeddings
    """
    async def event_stream():
        try:
            print(f"📡 Novo cliente conectado ao stream de notificações")
            
            # Conectar ao PubSub do IPFS
            response = requests.post(
                f"{IPFS_API_URL}/pubsub/sub",
                params={'arg': CANAL_PUBSUB},
                stream=True,
                timeout=None
            )
            
            yield f"data: {json.dumps({'type': 'connected', 'message': 'Conectado ao canal de notificações', 'canal': CANAL_PUBSUB})}\n\n"
            
            for line in response.iter_lines():
                if line:
                    try:
                        msg = json.loads(line)
                        data_encoded = msg.get('data', '')

                        try:
                            data_decoded = base64.b64decode(data_encoded).decode('utf-8')
                        except:
                            data_decoded = data_encoded
                        
                        # Tentar parsear como JSON
                        try:
                            message_obj = json.loads(data_decoded)
                            
                            if message_obj.get('type') == 'new_document':
                                # Mensagem com vetor e embeddings
                                notification = {
                                    'type': 'new_document',
                                    'version': message_obj['version'],
                                    'cid': message_obj['cid'],
                                    'filename': message_obj['filename'],
                                    'embedding_shape': message_obj['embedding_shape'],
                                    'embedding_hash': message_obj['embedding_hash'],
                                    'timestamp': message_obj['timestamp'],
                                    'from': msg.get('from', 'unknown')[:20] + '...'
                                }
                                
                                # Guarda embeddings recebidos
                                try:
                                    embeddings_array = np.array(message_obj['embeddings'])
                                    np.save(f"{EMBEDDINGS_DIR}/{message_obj['cid']}.npy", embeddings_array)
                                    print(f"Embedding recebido e guardado: {message_obj['cid']}")

                                    # 🔹 Atualiza o vetor local (pendente)
                                    vector = load_document_vector()
                                    vector.setdefault("version_confirmed", 0)
                                    vector.setdefault("version_pending", vector["version_confirmed"])
                                    vector.setdefault("documents_confirmed", [])
                                    vector.setdefault("documents_pending", [])

                                    vector["version_pending"] = message_obj["version"]
                                    vector["documents_pending"].append({
                                        "cid": message_obj["cid"],
                                        "filename": message_obj["filename"],
                                        "added_at": message_obj["timestamp"],
                                        "embedding_shape": message_obj["embedding_shape"],
                                        "embedding_file": f"{EMBEDDINGS_DIR}/{message_obj['cid']}.npy",
                                        "received_from_peer": True
                                    })
                                    save_document_vector(vector)
                                    print(f"🕒 Documento pendente adicionado (versão {message_obj['version']})")

                                except Exception as e:
                                    print(f"⚠️  Erro ao guardar embedding: {e}")
                                
                                yield f"data: {json.dumps(notification)}\n\n"
                                print(f"Novo documento recebido: {message_obj['filename']} (v{message_obj['version']})")

                            else:
                                # Mensagem genérica
                                notification = {
                                    'type': 'message',
                                    'message': data_decoded,
                                    'from': msg.get('from', 'unknown')[:20] + '...'
                                }
                                yield f"data: {json.dumps(notification)}\n\n"

                        except json.JSONDecodeError:
                            # Mensagem de texto simples
                            notification = {
                                'type': 'new_file',
                                'message': data_decoded,
                                'from': msg.get('from', 'unknown')[:20] + '...'
                            }
                            yield f"data: {json.dumps(notification)}\n\n"
                        
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

@app.post("/confirm-version")
def confirm_version(version: int, hash_received: str):
    """
    Recebe confirmação de peers e valida se hash corresponde à versão pendente.
    (Simulação do comportamento esperado no Sprint 3)
    """
    vector = load_document_vector()

    if vector.get("version_pending") != version:
        return {"status": "rejected", "reason": "Versão pendente diferente"}

    # Calcula hash local
    import hashlib, json
    local_hash = hashlib.sha256(
        json.dumps(vector["documents_pending"], sort_keys=True).encode()
    ).hexdigest()

    if local_hash != hash_received:
        return {"status": "rejected", "reason": "Hash inválida"}

    # Se hash for válida, confirmar versão
    vector["documents_confirmed"].extend(vector["documents_pending"])
    vector["version_confirmed"] = version
    vector["documents_pending"] = []
    save_document_vector(vector)

    print(f"✅ Versão {version} confirmada e promovida a estável")
    return {"status": "confirmed", "version": version}

@app.post("/ack-version")
def ack_version():
    """
    Peer devolve a hash da versão pendente ao líder.
    """
    vector = load_document_vector()

    if "documents_pending" not in vector or not vector["documents_pending"]:
        return {"status": "error", "message": "Sem versão pendente para confirmar"}

    version = vector.get("version_pending", 0)

    # Calcula hash da versão pendente
    vector_hash = hashlib.sha256(
        json.dumps(vector["documents_pending"], sort_keys=True).encode()
    ).hexdigest()

    print(f"📩 Peer envia hash da versão pendente ({version}): {vector_hash[:12]}")

    return {
        "status": "ok",
        "version": version,
        "hash": vector_hash
    }

@app.post("/commit-version")
def commit_version():
    """
    LÍDER envia commit aos peers para confirmar versão pendente.
    """
    vector = load_document_vector()

    if not vector.get("documents_pending"):
        return {"status": "error", "message": "Sem versão pendente para confirmar"}

    version = vector.get("version_pending")

    # Promove versão pendente → confirmada
    vector["documents_confirmed"].extend(vector["documents_pending"])
    vector["documents_pending"] = []
    vector["version_confirmed"] = version
    save_document_vector(vector)

    print(f"✅ Commit aplicado. Versão {version} confirmada.")
    return {"status": "committed", "version": version}

# ============= INIT =============

if __name__ == "__main__":
    print("\n" + "="*60)
    print("IPFS Upload API")
    print("="*60)
    print(f"Servidor: http://0.0.0.0:5000")
    print(f"Documentação: http://localhost:5000/docs")
    print(f"Canal PubSub: {CANAL_PUBSUB}")
    print(f"Modelo: SentenceTransformer (all-MiniLM-L6-v2)")
    print(f"Dimensão embeddings: 384")
    print("="*60)
    
    vector = load_document_vector()
    print(f"\nVetor de Documentos:")
    print(f"  └─ Versão confirmada: {vector.get('version_confirmed', 0)}")
    print(f"  └─ Versão pendente: {vector.get('version_pending', 0)}")
    print(f"  └─ Total confirmados: {len(vector.get('documents_confirmed', []))}")
    print(f"  └─ Total pendentes: {len(vector.get('documents_pending', []))}")
    print("="*60 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info"
    )
