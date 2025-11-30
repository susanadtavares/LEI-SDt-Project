"""
Script para limpar todos os ficheiros gerados
"""

import os
import shutil
from pathlib import Path

def cleanup():
    print("\n" + "="*60)
    print("LIMPEZA DO SISTEMA")
    print("="*60)
    
    items_to_remove = [
        "embeddings/",
        "temp_embeddings/",
        "pending_uploads/",
        "document_vector.json",
        "faiss_index.bin",
        "teste_*.txt"
    ]
    
    for item in items_to_remove:
        if '*' in item:
            # Ficheiros com wildcard
            import glob
            for file in glob.glob(item):
                try:
                    os.remove(file)
                    print(f"✅ Removido: {file}")
                except Exception as e:
                    print(f"⚠️  Erro ao remover {file}: {e}")
        else:
            # Diretórios ou ficheiros específicos
            if os.path.isdir(item):
                try:
                    shutil.rmtree(item)
                    print(f"✅ Removido: {item}")
                except Exception as e:
                    print(f"⚠️  Erro ao remover {item}: {e}")
            elif os.path.isfile(item):
                try:
                    os.remove(item)
                    print(f"✅ Removido: {item}")
                except Exception as e:
                    print(f"⚠️  Erro ao remover {item}: {e}")
    
    print("="*60)
    print("✅ Limpeza concluída!")
    print("="*60 + "\n")


if __name__ == "__main__":
    confirm = input("⚠️  Tem a certeza que quer limpar tudo? (s/n): ")
    if confirm.lower() == 's':
        cleanup()
    else:
        print("Cancelado.")
