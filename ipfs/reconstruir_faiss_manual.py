# reconstruir_faiss_manual.py
import json, os
import numpy as np
from pathlib import Path

from node import carregar_vetor_documentos, reconstruir_faiss

if __name__ == "__main__":
    vector = carregar_vetor_documentos()
    print("version_confirmed:", vector.get("version_confirmed"))
    print("docs:", len(vector.get("documents_confirmed", [])))
    reconstruir_faiss()
