"""
Script simples para enviar uma confirmação de versão ao nó líder.

Descrição:
- Faz um pedido HTTP POST para o endpoint `/ack-version` do líder na máquina local (assume que o serviço está em `http://localhost:5000`).
- Espera receber uma resposta JSON contendo pelo menos os campos `version` e `hash` quando o pedido for bem-sucedido (status 200).

Uso / pressupostos:
- O serviço do líder deve estar a correr em `localhost:5000` e expor o endpoint `/ack-version` que aceita POSTs.
- A resposta, em caso de sucesso, tem formato JSON, por exemplo: {"version": 3, "hash": "Qm..."}

Este ficheiro imprime mensagens simples no terminal indicando o sucesso ou falha do envio.
"""

import requests

# Mensagem de início para o utilizador — indica que o script vai tentar enviar a confirmação de versão ao nó líder.
print("🔁 A enviar confirmação de versão para o líder...\n")

# Envia um POST simples para o endpoint do líder. 
response = requests.post("http://localhost:5000/ack-version")

# Verifica o código de estado HTTP retornado pelo servidor.
if response.status_code == 200:
        # Interpreta a resposta como JSON e extrai os campos esperados.
        # Aqui assumimos que `version` é um número (versão) e `hash` é uma
        # string (por exemplo, um IPFS hash). Nós cortamos a hash para os
        # primeiros 16 caracteres na impressão para manter a saída compacta.
        result = response.json()
        print(f"✅ Hash enviada ao líder (versão {result['version']}): {result['hash'][:16]}")
else:
        # Em caso de erro, imprime o corpo da resposta para ajudar no
        # diagnóstico (pode conter mensagens de erro do servidor).
        print("❌ Erro ao enviar confirmação:", response.text)
