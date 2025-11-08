import requests

print("🔁 A enviar confirmação de versão para o líder...\n")
response = requests.post("http://25.42.152.214:5000/ack-version")

if response.status_code == 200:
    result = response.json()
    print(f"✅ Hash enviada ao líder (versão {result['version']}): {result['hash'][:16]}")
else:
    print("❌ Erro ao enviar confirmação:", response.text)
