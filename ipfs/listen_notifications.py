import requests
import json
import sys
import time

def connect_to_notifications():
    print("="*60)
    print("LISTENER - A aguardar transmissões...")
    print("="*60)
    
    while True:
        print("\nA ligar ao stream de notificações...")
        try:
            response = requests.get(
                'http://25.42.152.214:5000/notifications',
                stream=True,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"Erro na conexão: {response.status_code}. A tentar reconectar em 5 segundos...")
                time.sleep(5)
                continue
                
            print("Conectado ao servidor de notificações\n")
            
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
                    
                    except json.JSONDecodeError:
                        print(f"⚠️  Erro ao decodificar mensagem: {line}")
                        continue
                    except Exception as e:
                        print(f"⚠️  Erro ao processar notificação: {e}")
                        continue
                        
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {str(e)}")
            print("A tentar reconectar em 5 segundos...")
            time.sleep(5)
            continue
        except KeyboardInterrupt:
            print("\n\nA encerrar o listener...")
            sys.exit(0)
        except Exception as e:
            print(f"Erro inesperado: {e}")
            print("A tentar reconectar em 5 segundos...")
            time.sleep(5)
            continue

if __name__ == "__main__":
    connect_to_notifications()
