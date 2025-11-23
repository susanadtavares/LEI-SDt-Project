"""
DEBUG SIMPLES - Testa se o listener consegue receber mensagens.
"""

import subprocess
import json

print("🔍 A ouvir o canal 'canal-ficheiros'...")
print("(Abre outro terminal e corre: python test-upload-cli.py)\n")

try:
    process = subprocess.Popen(
        ['ipfs', 'pubsub', 'sub', 'canal-ficheiros'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
        errors='ignore'
    )
    
    line_count = 0
    for line in iter(process.stdout.readline, ''):
        line_count += 1
        line = line.strip()
        
        if not line:
            print(f"[{line_count}] LINHA VAZIA")
            continue
        
        print(f"\n[{line_count}] RAW: {line[:200]}")
        
        # Tenta parse
        try:
            obj = json.loads(line)
            print(f"     ✅ JSON válido! type={obj.get('type')}")
        except Exception as e:
            print(f"     ❌ Erro JSON: {e}")

except KeyboardInterrupt:
    print("\n\n✅ Interrompido")
except Exception as e:
    print(f"❌ Erro: {e}")