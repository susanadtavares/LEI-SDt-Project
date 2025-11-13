"""
Lista os peers conectados ao nó IPFS local e imprime-os.

Descrição:
- Faz um POST para a API local do IPFS no endpoint `/api/v0/swarm/peers` para obter a lista de peers com os quais o nó está conectado.
- Extrai a lista do campo `Peers` do JSON retornado e apresenta um resumo simples no terminal.

Pressupostos:
- O daemon IPFS está a correr localmente e expõe a API na porta `5001` (URL: `http://localhost:5001`).
"""

import requests
import json

# Mensagem inicial a indicar que a listar peers está a começar.
print("👥 A listar peers conectados...\n")

# Faz um POST para o endpoint do API do IPFS que devolve os peers conectados ao swarm. Usamos o endpoint local padrão `5001`.
response = requests.post("http://localhost:5001/api/v0/swarm/peers")

# Converte a resposta para JSON. Se o servidor não devolver JSON isto levantará uma exceção;
peers = response.json()

# O JSON retornado contém tipicamente uma chave `Peers` com uma lista de peers; usamos `.get()` com lista vazia como fallback.
peer_list = peers.get('Peers', [])

if peer_list:
        # Imprime o número total de peers conectados.
        print(f"✅ {len(peer_list)} peers conectados:\n")

        # Para evitar saída demasiado longa, mostramos apenas os primeiros20 peers.
        for peer in peer_list[:20]:
                # Cada `peer` é tipicamente um dicionário com campos como 'Peer' (identificador) e 'Addr' (endereço multiaddr).
                print(f"🔗 {peer['Peer']}")
                print(f"   Endereço: {peer['Addr']}\n")
else:
        # Caso a lista esteja vazia, mostramos uma mensagem informativa.
        print("⚠️  Nenhum peer conectado")
