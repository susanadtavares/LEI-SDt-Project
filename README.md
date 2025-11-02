# Trabalho Prático

Este projeto surge no âmbito da disciplina de Sistemas Distribuídos, do Curso de Engenharia Informática.

**Realizado por:**

- David Borges - 27431
- Patrícia Oliveira - 22525
- Susana Tavares - 27467
- José Arrais - 23747 

## Pré-requisitos

- Python 3.12 ou superior
- IPFS instalado e em execução

## Instalação

1. Criar e ativar o ambiente virtual:
```bash
python -m venv venv
venv\Scripts\activate # Windows
source venv/bin/activate # Linux/MacOS
```

2. Instalar as dependências:
```bash
pip install -r requirements.txt
```

## Procedimento de Testes

1. Iniciar o servidor principal:
```bash
python ipfs/server.py
```

2. Iniciar o recetor de mensagens:
```bash
python ipfs/listen_notifications.py
```

3. Iniciar o cliente:
```bash
python ipfs/test_upload.py
```

## Documentação

A documentação da API está disponível em:
http://localhost:5000/docs
