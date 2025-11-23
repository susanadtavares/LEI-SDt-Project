"""
Exemplo SIMPLIFICADO de upload de ficheiro via PubSub.

Este script:
1. Faz upload do ficheiro para o IPFS
2. Publica proposta via PubSub usando CLI do IPFS (mais confiável)
3. Mostra mensagem de sucesso (votação acontece nos peers)

Para ver o progresso da votação, use vote-pubsub-v3.py
"""

import requests
import json
import uuid
import sys
import subprocess
from datetime import datetime

IPFS_API_URL = "http://127.0.0.1:5001/api/v0"
CANAL_PUBSUB = "canal-ficheiros"

print("="*60)
print("Upload Simples via PubSub")
print("="*60)

filename = 'teste_sprintAHAHA.txt'
doc_id = str(uuid.uuid4())

print(f"\n📤 A enviar '{filename}' para o IPFS...")

# 1) Upload para IPFS
try:
    with open(filename, 'rb') as f:
        files = {'file': (filename, f)}
        r = requests.post(f"{IPFS_API_URL}/add", files=files, timeout=30)
    
    if r.status_code == 200:
        cid = r.json().get('Hash')
        print(f"✅ Ficheiro adicionado ao IPFS")
        print(f"   CID: {cid}")
    else:
        print(f"❌ Falha ao adicionar ao IPFS: {r.status_code}")
        sys.exit(1)
except FileNotFoundError:
    print(f"❌ Ficheiro '{filename}' não encontrado!")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro ao adicionar ao IPFS: {e}")
    sys.exit(1)

# 2) Publica proposta via PubSub usando CLI
print(f"\n📡 A publicar proposta via PubSub...")

message = {
    "type": "document_proposal",
    "doc_id": doc_id,
    "filename": filename,
    "cid": cid,
    "from_peer": "test_upload",
    "timestamp": datetime.now().isoformat(),
    "total_peers": 2,  # Ajusta conforme necessário
    "required_votes": 2  # Maioria simples
}

message_json = json.dumps(message)

# Usa CLI do IPFS (mais confiável que HTTP API para PubSub)
try:
    process = subprocess.Popen(
        ['ipfs', 'pubsub', 'pub', CANAL_PUBSUB],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    stdout, stderr = process.communicate(input=message_json.encode('utf-8'), timeout=10)
    
    if process.returncode == 0:
        print(f"✅ Proposta publicada com sucesso!")
        print(f"\n" + "="*60)
        print("📋 DETALHES DA PROPOSTA")
        print("="*60)
        print(f"Doc ID: {doc_id}")
        print(f"Ficheiro: {filename}")
        print(f"CID: {cid}")
        print(f"Votos necessários: {message['required_votes']}")
        print("="*60)
        print("\n💡 Use 'vote-pubsub-v3.py' para votar!")
        print("   Comando: vote 1 approve")
    else:
        print(f"❌ Falha ao publicar via CLI")
        print(f"   Stderr: {stderr.decode('utf-8', errors='ignore')[:200]}")
        sys.exit(1)

except FileNotFoundError:
    print("❌ Comando 'ipfs' não encontrado!")
    print("   Certifica-te que o IPFS está instalado e no PATH")
    sys.exit(1)
except subprocess.TimeoutExpired:
    process.kill()
    print("❌ Timeout ao publicar via PubSub")
    sys.exit(1)
except Exception as e:
    print(f"❌ Erro ao publicar via PubSub: {e}")
    sys.exit(1)

print("\n" + "="*60 + "\n")
