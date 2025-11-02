import requests
import json
import sys

print("="*60)
print("LISTENER - A aguardar transmissões...")
print("="*60)
print("A ligar ao stream de notificações...\n")

try:
    response = requests.get(
        'http://localhost:5000/notifications',
        stream=True,
        timeout=None
    )
    
    for line in response.iter_lines():
        if line:
            try:
                if line.startswith(b'data: '):
                    data = line[6:]
                    msg = json.loads(data)
                    
                    if msg['type'] == 'connected':
                        print(f"{msg['message']}")
                        print(f"Canal: {msg['canal']}\n")
                    
                    elif msg['type'] == 'new_document':
                        print("="*60)
                        print("NOVO DOCUMENTO RECEBIDO COM SUCESSO!")
                        print("="*60)
                        print(f"Ficheiro: {msg['filename']}")
                        print(f"CID: {msg['cid']}")
                        print(f"Versão vetor: {msg['version']}")
                        print(f"Embedding shape: {msg['embedding_shape']}")
                        print(f"Hash: {msg['embedding_hash']}")
                        print(f"De: {msg['from']}")
                        print(f"Timestamp: {msg['timestamp']}")
                        print("="*60 + "\n")
                    
                    elif msg['type'] == 'new_file':
                        print(f"{msg['message']}")
                    
                    elif msg['type'] == 'error':
                        print(f"Erro: {msg['message']}")
            
            except Exception as e:
                print(f"⚠️  Erro ao processar notificação: {e}")
                continue

except KeyboardInterrupt:
    print("\n\n Desconectado")
    sys.exit(0)
except Exception as e:
    print(f"Erro: {e}")
    sys.exit(1)
