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

3. Iniciar o IPFS daemon:
```bash
ipfs daemon
```

## Verificação da Instalação

1. Verificar se todas as dependências estão instaladas:
```bash
python ipfs/check_setup.py
```

## Procedimento de Testes

1. Iniciar o servidor principal:
```bash
python node.py --initial-leader
```

2. Iniciar o sistema de votação:
```bash
python node.py
```

3. Iniciar o cliente:
```bash
python ipfs/test_upload.py
```

## Documentação

A documentação da API está disponível em:
http://localhost:5000/docs
