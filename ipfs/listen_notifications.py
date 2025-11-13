"""
Listener de notificações e sistema de votação simples.

Este script estabelece uma ligação HTTP em streaming ao endpoint `/notifications` do servidor local para receber eventos em tempo
real (SSE-like ou streaming JSON). Quando chegam notificações sobre propostas de documentos, votos de peers ou decisões finais, o script
imprime informações no terminal e mantém um dicionário `pending_proposals` com as propostas ainda por decidir.

Pressupostos:
- O servidor de notificações está disponível em `http://localhost:5000`.
- O endpoint `/notifications` suporta streaming (eventos por linha), com linhas que começam por `data: ` seguidas de JSON.

Funcionalidades principais:
- `connect_to_notifications()` — conecta e processa eventos em loop.
- `vote_on_document()` — envia um POST para registar o voto do utilizador.
- `display_voting_menu()` — mostra um resumo das propostas pendentes.
"""

import requests
import json
import sys
import time
from datetime import datetime

# Estrutura em memória para guardar propostas de documentos que estão pendentes de decisão. A chave é o `doc_id` e o valor é um dicionário
# com metadados como nome do ficheiro e votos necessários.
pending_proposals = {}

def display_voting_menu():
    """Mostra no terminal um menu/resumo das propostas pendentes.

    Se não existirem propostas pendentes, retorna imediatamente. Caso contrário, imprime uma lista numerada com informações úteis
    para que o utilizador possa indicar qual documento pretende votar.
    """
    if not pending_proposals:
        return
    
    print("\n" + "="*60)
    print("DOCUMENTOS PENDENTES DE VOTAÇÃO")
    print("="*60)
    for idx, (doc_id, info) in enumerate(pending_proposals.items(), 1):
        # Mostramos um resumo: nome do ficheiro, parte do doc_id e votos necessários
        print(f"\n{idx}. {info['filename']}")
        print(f"   Doc ID: {doc_id[:16]}...")
        print(f"   Votos necessários: {info['required_votes']}")
        print(f"   Recebido: {info.get('received_at', 'agora')}")
    print("\n" + "="*60)
    print("Digite 'vote <número> approve' ou 'vote <número> reject'")
    print("Exemplo: vote 1 approve")
    print("="*60 + "\n")


def vote_on_document(doc_number, vote_type):
    """Envia um POST para o servidor a registar um voto para um documento.

    `doc_number` é a posição (1-based) na listagem retornada por `pending_proposals`. `vote_type` espera 'approve' ou 'reject'.
    """
    try:
        # Converte o índice do utilizador para o doc_id correspondente
        doc_id = list(pending_proposals.keys())[doc_number - 1]

        # Envia o pedido ao servidor para registar o voto
        response = requests.post(f'http://localhost:5000/vote/{doc_id}/{vote_type}')
        
        if response.status_code == 200:
            # Em caso de sucesso, imprime detalhes do estado atual da votação
            result = response.json()
            print(f"\n✅ VOTO REGISTADO: {vote_type.upper()}")
            print(f"Status: {result.get('status')}")
            print(f"Votos a favor: {result.get('votes_approve', 0)}")
            print(f"Votos contra: {result.get('votes_reject', 0)}")
            print(f"Necessários: {result.get('required_votes', 0)}")
            
            # Se a votação terminou (approved/rejected), removemos a proposta
            if result.get('status') in ['approved', 'rejected']:
                print(f"\n🎯 DECISÃO FINAL: {result['status'].upper()}")
                if doc_id in pending_proposals:
                    del pending_proposals[doc_id]
        else:
            # Mostra o texto de erro recebido do servidor para diagnóstico
            print(f"\n❌ Erro ao votar: {response.text}")
    
    except IndexError:
        # Índice inválido fornecido pelo utilizador
        print("\n❌ Número de documento inválido")
    except Exception as e:
        # Captura genérica para evitar crash do programa
        print(f"\n❌ Erro: {e}")


def connect_to_notifications():
    """Conecta ao servidor de notificações e processa eventos em streaming.

    Mantém uma conexão HTTP com `stream=True` e itera sobre as linhas recebidas. Para cada linha que contenha um evento (`data: <json>`),
    decodifica o JSON e executa ações dependendo do tipo de evento.
    """
    print("="*60)
    print("A aguardar notificações e propostas de documentos...")
    print("="*60 + "\n")
    
    while True:
        try:
            # Abre uma conexão GET em streaming para receber notificações
            response = requests.get(
                'http://localhost:5000/notifications',
                stream=True,
                timeout=30
            )
            
            if response.status_code != 200:
                # Se não for 200, aguarda e tenta reconectar
                print(f"Erro na conexão: {response.status_code}. A tentar reconectar em 5 segundos...")
                time.sleep(5)
                continue
            
            print("✅ Conectado ao servidor\n")
            
            # Itera sobre as linhas do stream; cada linha representa um evento (por exemplo em formato SSE: 'data: {...}').
            for line in response.iter_lines():
                if line:
                    try:
                        # Verifica se a linha começa com o prefixo 'data: '
                        if line.startswith(b'data: '):
                            data = line[6:]
                            msg = json.loads(data)
                            msg_type = msg.get('type')
                            
                            # Evento de ligação inicial
                            if msg_type == 'connected':
                                print(f"📡 {msg['message']} (Canal: {msg['canal']})\n")
                            
                            # Nova proposta de documento — adiciona ao dicionário
                            elif msg_type == 'document_proposal':
                                doc_id = msg['doc_id']
                                filename = msg['filename']
                                required_votes = msg['required_votes']
                                
                                pending_proposals[doc_id] = {
                                    'filename': filename,
                                    'required_votes': required_votes,
                                    'received_at': datetime.now().strftime("%H:%M:%S")
                                }
                                
                                # Mostra um resumo da nova proposta ao utilizador
                                print("\n" + "="*60)
                                print("🗳️  NOVA PROPOSTA DE DOCUMENTO")
                                print("="*60)
                                print(f"Ficheiro: {filename}")
                                print(f"Doc ID: {doc_id}")
                                print(f"Votos necessários: {required_votes}")
                                print(f"De: {msg.get('from_peer', 'unknown')}")
                                print("="*60)
                                print("\nVotar: /vote <doc_id> approve|reject")
                                print(f"Exemplo: /vote {doc_id} approve")
                                print("="*60 + "\n")
                                
                                display_voting_menu()
                            
                            # Notificação de voto de um peer — imprime resumo
                            elif msg_type == 'peer_vote':
                                doc_id = msg['doc_id']
                                vote = msg['vote']
                                peer_id = msg.get('peer_id', 'unknown')
                                result = msg.get('result', {})
                                
                                print(f"\n📊 Voto recebido de {peer_id[:16]}...")
                                print(f"   Voto: {vote.upper()}")
                                if result:
                                    print(f"   A favor: {result.get('votes_approve', 0)} | Contra: {result.get('votes_reject', 0)}")
                                    print(f"   Necessários: {result.get('required_votes', 0)}")
                            
                            # Documento aprovado — remove da lista de pendentes
                            elif msg_type == 'document_approved':
                                doc_id = msg['doc_id']
                                if doc_id in pending_proposals:
                                    del pending_proposals[doc_id]
                                
                                print("\n" + "="*60)
                                print("✅ DOCUMENTO APROVADO")
                                print("="*60)
                                print(f"Ficheiro: {msg['filename']}")
                                print(f"CID: {msg.get('cid', 'N/A')}")
                                print(f"Votos a favor: {msg.get('votes_approve', 0)}")
                                print(f"Votos contra: {msg.get('votes_reject', 0)}")
                                print("="*60 + "\n")
                            
                            # Documento rejeitado — remove da lista e informa o utilizador
                            elif msg_type == 'document_rejected':
                                doc_id = msg['doc_id']
                                if doc_id in pending_proposals:
                                    del pending_proposals[doc_id]
                                
                                print("\n" + "="*60)
                                print("❌ DOCUMENTO REJEITADO")
                                print("="*60)
                                print(f"Ficheiro: {msg['filename']}")
                                print(f"Votos a favor: {msg.get('votes_approve', 0)}")
                                print(f"Votos contra: {msg.get('votes_reject', 0)}")
                                print("="*60 + "\n")
                            
                            # Evento de erro enviado pelo servidor
                            elif msg_type == 'error':
                                print(f"Erro: {msg['message']}")
                    
                    except json.JSONDecodeError:
                        # Ignora linhas que não sejam JSON válido
                        continue
                    except Exception as e:
                        # Evita que um erro de processamento derrube o loop
                        print(f"⚠️  Erro ao processar: {e}")
                        continue
        
        except requests.exceptions.RequestException as e:
            # Erros de rede (timeout, reset, etc.) — tenta reconectar
            print(f"Erro de conexão: {str(e)}")
            print("A tentar reconectar em 5 segundos...")
            time.sleep(5)
        except KeyboardInterrupt:
            # Permite ao utilizador encerrar o listener com Ctrl+C
            print("\n\nA encerrar o listener...")
            sys.exit(0)
        except Exception as e:
            # Captura qualquer erro inesperado e tenta continuar
            print(f"Erro inesperado: {e}")
            time.sleep(5)


if __name__ == "__main__":
    connect_to_notifications()
