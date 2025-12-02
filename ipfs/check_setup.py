"""
Script para verificar se todas as dependências estão instaladas

Uso:
    python check_setup.py           # Verificação completa
    python check_setup.py --fix     # Tenta corrigir problemas
    python check_setup.py --verbose # Output detalhado
"""

import sys
import os
import subprocess
import socket
import argparse
from pathlib import Path


class Colors:
    """Cores ANSI para output colorido"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_header(text):
    """Imprime header colorido"""
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.HEADER}{text}{Colors.RESET}")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")


def print_success(text, indent=0):
    """Imprime mensagem de sucesso"""
    prefix = "   " * indent
    print(f"{prefix}{Colors.GREEN}✅ {text}{Colors.RESET}")


def print_warning(text, indent=0):
    """Imprime mensagem de aviso"""
    prefix = "   " * indent
    print(f"{prefix}{Colors.YELLOW}⚠️  {text}{Colors.RESET}")


def print_error(text, indent=0):
    """Imprime mensagem de erro"""
    prefix = "   " * indent
    print(f"{prefix}{Colors.RED}❌ {text}{Colors.RESET}")


def print_info(text, indent=0):
    """Imprime mensagem informativa"""
    prefix = "   " * indent
    print(f"{prefix}{Colors.BLUE}ℹ️  {text}{Colors.RESET}")


def check_python():
    """Verifica versão do Python"""
    print(f"{Colors.BOLD}🐍 Python{Colors.RESET}")
    
    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"
    
    print(f"   Versão: {version_str}")
    
    if version >= (3, 12):
        return True
    else:
        print_error(f"Requer Python 3.12+ (atual: {version_str})", indent=1)
        print_info("Instalar: https://www.python.org/downloads/", indent=1)
        return False


def check_modules(verbose=False):
    """Verifica módulos Python"""
    print(f"\n{Colors.BOLD}📦 Módulos Python{Colors.RESET}")
    
    modules = {
        "fastapi": "FastAPI",
        "uvicorn": "Uvicorn",
        "requests": "Requests",
        "sentence_transformers": "Sentence Transformers",
        "faiss": "FAISS",
        "numpy": "NumPy",
        "torch": "PyTorch",
    }
    
    all_ok = True
    missing = []
    
    for module, name in modules.items():
        try:
            mod = __import__(module)
            version = getattr(mod, '__version__', 'unknown')
            
            if verbose:
                print_success(f"{name} ({version})", indent=1)
            else:
                print_success(name, indent=1)
        
        except ImportError:
            print_error(f"{name} não instalado", indent=1)
            missing.append(module)
            all_ok = False
    
    if missing:
        print_info("Instalar com:", indent=1)
        print(f"   pip install {' '.join(missing)}")
    
    return all_ok


def check_ipfs():
    """Verifica IPFS instalado"""
    print(f"\n{Colors.BOLD}🌐 IPFS{Colors.RESET}")
    
    try:
        result = subprocess.run(
            ['ipfs', 'version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            version = result.stdout.strip()
            print_success(f"{version}", indent=1)
            return True
        else:
            print_error("IPFS não responde", indent=1)
            return False
    
    except FileNotFoundError:
        print_error("IPFS não instalado", indent=1)
        print_info("Instalar: https://docs.ipfs.tech/install/", indent=1)
        return False
    
    except subprocess.TimeoutExpired:
        print_error("IPFS timeout", indent=1)
        return False
    
    except Exception as e:
        print_error(f"Erro: {e}", indent=1)
        return False


def check_ipfs_daemon():
    """Verifica IPFS Daemon ativo"""
    print(f"\n{Colors.BOLD}🔌 IPFS Daemon{Colors.RESET}")
    
    import requests
    
    try:
        response = requests.post(
            "http://127.0.0.1:5001/api/v0/version",
            timeout=2
        )
        
        if response.status_code == 200:
            data = response.json()
            version = data.get('Version', 'unknown')
            print_success(f"Daemon ativo (v{version})", indent=1)
            return True
        else:
            print_error("Daemon não responde", indent=1)
            return False
    
    except requests.exceptions.ConnectionError:
        print_error("Daemon não está ativo", indent=1)
        print_info("Iniciar com: ipfs daemon --enable-pubsub-experiment", indent=1)
        return False
    
    except Exception as e:
        print_error(f"Erro: {e}", indent=1)
        return False


def check_ipfs_pubsub():
    """Verifica PubSub habilitado"""
    print(f"\n{Colors.BOLD}📡 IPFS PubSub{Colors.RESET}")
    
    try:
        result = subprocess.run(
            ['ipfs', 'config', 'Pubsub.Enabled'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            enabled = result.stdout.strip().lower()
            
            if enabled == 'true':
                print_success("PubSub habilitado", indent=1)
                return True
            else:
                print_warning("PubSub desabilitado", indent=1)
                print_info("Habilitar: ipfs config --json Pubsub.Enabled true", indent=1)
                return False
        else:
            print_warning("Não foi possível verificar PubSub", indent=1)
            return False
    
    except Exception as e:
        print_error(f"Erro: {e}", indent=1)
        return False


def check_ipfs_mdns():
    """Verifica mDNS configurado"""
    print(f"\n{Colors.BOLD}🔍 IPFS mDNS (Auto-Discovery){Colors.RESET}")
    
    try:
        # Verificar se está habilitado
        result = subprocess.run(
            ['ipfs', 'config', 'Discovery.MDNS.Enabled'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            enabled = result.stdout.strip().lower()
            
            if enabled == 'true':
                print_success("mDNS habilitado", indent=1)
                
                # Verificar intervalo
                result = subprocess.run(
                    ['ipfs', 'config', 'Discovery.MDNS.Interval'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                if result.returncode == 0:
                    interval = result.stdout.strip()
                    print_info(f"Intervalo: {interval}s", indent=1)
                
                return True
            else:
                print_warning("mDNS desabilitado", indent=1)
                print_info("Habilitar: ipfs config --json Discovery.MDNS.Enabled true", indent=1)
                return False
        else:
            print_warning("Não foi possível verificar mDNS", indent=1)
            return False
    
    except Exception as e:
        print_error(f"Erro: {e}", indent=1)
        return False


def check_ports():
    """Verifica se portas estão disponíveis"""
    print(f"\n{Colors.BOLD}🔌 Portas{Colors.RESET}")
    
    ports = {
        5000: "FastAPI (HTTP)",
        4001: "IPFS Swarm",
        5001: "IPFS API",
        8080: "IPFS Gateway"
    }
    
    all_ok = True
    
    for port, name in ports.items():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        
        if result == 0:
            # Porta em uso
            if port == 5000:
                print_warning(f"Porta {port} ({name}) já em uso", indent=1)
                print_info("Normal se já houver um líder ativo", indent=1)
            else:
                print_success(f"Porta {port} ({name}) em uso (OK)", indent=1)
        else:
            # Porta livre
            if port == 5000:
                print_success(f"Porta {port} ({name}) disponível", indent=1)
            else:
                print_warning(f"Porta {port} ({name}) não está em uso", indent=1)
                if port in [4001, 5001, 8080]:
                    print_info("Daemon IPFS pode não estar ativo", indent=1)
                    all_ok = False
    
    return all_ok


def check_directories():
    """Verifica estrutura de diretórios"""
    print(f"\n{Colors.BOLD}📁 Estrutura de Diretórios{Colors.RESET}")
    
    dirs = [
        "embeddings/",
        "temp_embeddings/",
        "pending_uploads/"
    ]
    
    for dir_path in dirs:
        if os.path.exists(dir_path):
            num_files = sum(1 for _ in Path(dir_path).rglob('*') if _.is_file())
            print_success(f"{dir_path} existe ({num_files} ficheiros)", indent=1)
        else:
            print_info(f"{dir_path} será criado automaticamente", indent=1)
    
    return True


def check_network():
    """Verifica conectividade de rede"""
    print(f"\n{Colors.BOLD}🌐 Conectividade de Rede{Colors.RESET}")
    
    try:
        # Verificar se consegue resolver DNS
        socket.gethostbyname('www.google.com')
        print_success("DNS funcional", indent=1)
        
        # Verificar localhost
        socket.gethostbyname('localhost')
        print_success("Localhost OK", indent=1)
        
        return True
    
    except socket.error:
        print_error("Problema de conectividade", indent=1)
        return False


def fix_common_issues():
    """Tenta corrigir problemas comuns"""
    print_header("🔧 CORRIGINDO PROBLEMAS COMUNS")
    
    # Criar pastas
    print("📁 A criar pastas...")
    for dir_path in ["embeddings", "temp_embeddings", "pending_uploads"]:
        Path(dir_path).mkdir(exist_ok=True)
        print_success(f"{dir_path}/ criado", indent=1)
    
    # Tentar ativar PubSub
    print("\n📡 A ativar PubSub...")
    try:
        subprocess.run(
            ['ipfs', 'config', '--json', 'Pubsub.Enabled', 'true'],
            check=True,
            capture_output=True
        )
        print_success("PubSub habilitado", indent=1)
    except:
        print_warning("Não foi possível habilitar PubSub", indent=1)
    
    # Tentar ativar mDNS
    print("\n🔍 A ativar mDNS...")
    try:
        subprocess.run(
            ['ipfs', 'config', '--json', 'Discovery.MDNS.Enabled', 'true'],
            check=True,
            capture_output=True
        )
        print_success("mDNS habilitado", indent=1)
    except:
        print_warning("Não foi possível habilitar mDNS", indent=1)
    
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Verificação de dependências do sistema"
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Output detalhado (mostra versões)'
    )
    
    parser.add_argument(
        '--fix',
        action='store_true',
        help='Tenta corrigir problemas comuns'
    )
    
    args = parser.parse_args()
    
    print_header("🔍 VERIFICAÇÃO DE DEPENDÊNCIAS")
    
    # Executar verificações
    results = {
        "Python": check_python(),
        "Módulos": check_modules(args.verbose),
        "IPFS": check_ipfs(),
        "Daemon": check_ipfs_daemon(),
        "PubSub": check_ipfs_pubsub(),
        "mDNS": check_ipfs_mdns(),
        "Portas": check_ports(),
        "Diretórios": check_directories(),
        "Rede": check_network()
    }
    
    # Resumo
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}📊 RESUMO{Colors.RESET}\n")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, ok in results.items():
        if ok:
            print_success(f"{name}")
        else:
            print_error(f"{name}")
    
    print(f"\n{Colors.BOLD}Total: {passed}/{total} verificações passaram{Colors.RESET}")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")
    
    # Resultado final
    if passed == total:
        print(f"{Colors.GREEN}{Colors.BOLD}✅ SISTEMA PRONTO PARA USAR!{Colors.RESET}\n")
        print_info("Execute: python ipfs/node.py")
        return 0
    else:
        print(f"{Colors.RED}{Colors.BOLD}❌ CORRIGE OS PROBLEMAS ACIMA{Colors.RESET}\n")
        
        if not args.fix:
            print_info("Tenta: python ipfs/check_setup.py --fix")
        else:
            fix_common_issues()
        
        return 1


if __name__ == "__main__":
    sys.exit(main())
