"""
Verifica se todas as dependências estão instaladas
"""

import sys

def check_setup():
    print("\n" + "="*60)
    print("VERIFICAÇÃO DE DEPENDÊNCIAS")
    print("="*60 + "\n")
    
    all_ok = True
    
    # Python
    print("🐍 Python:")
    print(f"   Versão: {sys.version}")
    if sys.version_info >= (3, 12):
        print("   ✅ OK")
    else:
        print("   ❌ Requer Python 3.12+")
        all_ok = False
    
    # Módulos Python
    modules = [
        "fastapi",
        "uvicorn",
        "requests",
        "sentence_transformers",
        "faiss",
        "numpy",
        "torch"
    ]
    
    print("\n📦 Módulos Python:")
    for module in modules:
        try:
            __import__(module)
            print(f"   ✅ {module}")
        except ImportError:
            print(f"   ❌ {module} não instalado")
            all_ok = False
    
    # IPFS
    print("\n🌐 IPFS:")
    import subprocess
    try:
        result = subprocess.run(['ipfs', 'version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"   ✅ {version}")
        else:
            print("   ❌ IPFS não responde")
            all_ok = False
    except FileNotFoundError:
        print("   ❌ IPFS não instalado")
        all_ok = False
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        all_ok = False
    
    # IPFS Daemon
    print("\n🔌 IPFS Daemon:")
    import requests
    try:
        response = requests.post("http://127.0.0.1:5001/api/v0/version", timeout=2)
        if response.status_code == 200:
            print("   ✅ Daemon ativo")
        else:
            print("   ❌ Daemon não responde")
            all_ok = False
    except:
        print("   ❌ Daemon não está a correr")
        print("   💡 Execute: ipfs daemon")
        all_ok = False
    
    print("\n" + "="*60)
    if all_ok:
        print("✅ TUDO PRONTO!")
    else:
        print("❌ CORRIGE OS PROBLEMAS ACIMA")
    print("="*60 + "\n")


if __name__ == "__main__":
    check_setup()
