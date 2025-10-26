import requests
import json

print("👥 A listar peers conectados...\n")

response = requests.post("http://127.0.0.1:5001/api/v0/swarm/peers")
peers = response.json()

peer_list = peers.get('Peers', [])

if peer_list:
    print(f"✅ {len(peer_list)} peers conectados:\n")
    for peer in peer_list[:10]:  # Mostrar apenas 10
        print(f"🔗 {peer['Peer']}")
        print(f"   Endereço: {peer['Addr']}\n")
else:
    print("⚠️  Nenhum peer conectado")
