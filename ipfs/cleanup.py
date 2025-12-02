"""
Script para limpar ficheiros gerados pelo sistema

Uso:
    python cleanup.py           # Limpeza interativa
    python cleanup.py --force   # Limpeza sem confirmação
    python cleanup.py --soft    # Remove apenas temporários
"""

import os
import shutil
import glob
import sys
import argparse
from pathlib import Path
from datetime import datetime


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


def print_success(text):
    """Imprime mensagem de sucesso"""
    print(f"{Colors.GREEN}✅ {text}{Colors.RESET}")


def print_warning(text):
    """Imprime mensagem de aviso"""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.RESET}")


def print_error(text):
    """Imprime mensagem de erro"""
    print(f"{Colors.RED}❌ {text}{Colors.RESET}")


def print_info(text):
    """Imprime mensagem informativa"""
    print(f"{Colors.BLUE}ℹ️  {text}{Colors.RESET}")


def get_size(path):
    """Calcula tamanho de ficheiro ou diretório"""
    if os.path.isfile(path):
        return os.path.getsize(path)
    
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total += os.path.getsize(filepath)
    return total


def format_size(bytes):
    """Formata tamanho em bytes para humano"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"


def backup_file(filepath):
    """Cria backup de ficheiro importante"""
    if not os.path.exists(filepath):
        return None
    
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = Path(filepath).name
    backup_path = backup_dir / f"{filename}.{timestamp}.bak"
    
    try:
        shutil.copy2(filepath, backup_path)
        return backup_path
    except Exception as e:
        print_error(f"Erro ao criar backup de {filepath}: {e}")
        return None


def remove_item(item, create_backup=False):
    """Remove ficheiro ou diretório"""
    try:
        # Verificar se existe
        if '*' in item:
            # Wildcard
            files = glob.glob(item)
            if not files:
                print_info(f"Nenhum ficheiro encontrado: {item}")
                return True
            
            removed = 0
            total_size = 0
            for file in files:
                size = get_size(file)
                total_size += size
                os.remove(file)
                removed += 1
            
            print_success(f"Removidos {removed} ficheiros ({format_size(total_size)}): {item}")
            return True
        
        elif not os.path.exists(item):
            print_info(f"Não existe: {item}")
            return True
        
        # Calcular tamanho antes de remover
        size = get_size(item)
        
        # Backup se necessário
        if create_backup:
            backup_path = backup_file(item)
            if backup_path:
                print_info(f"Backup criado: {backup_path}")
        
        # Remover
        if os.path.isdir(item):
            shutil.rmtree(item)
            print_success(f"Removido diretório ({format_size(size)}): {item}")
        elif os.path.isfile(item):
            os.remove(item)
            print_success(f"Removido ficheiro ({format_size(size)}): {item}")
        
        return True
    
    except PermissionError:
        print_error(f"Sem permissão para remover: {item}")
        return False
    except Exception as e:
        print_error(f"Erro ao remover {item}: {e}")
        return False


def cleanup_full():
    """Limpeza completa do sistema"""
    print_header("🧹 LIMPEZA COMPLETA DO SISTEMA")
    
    items = [
        # Diretórios
        ("embeddings/", False),
        ("temp_embeddings/", False),
        ("pending_uploads/", False),
        ("backups/", False),
        ("__pycache__/", False),
        
        # Ficheiros principais
        ("document_vector.json", True),  # Criar backup
        ("faiss_index.faiss", False),
        
        # Ficheiros temporários
        ("*.pyc", False),
        ("*.log", False),
        ("test_*.txt", False),
        ("teste_*.txt", False),
        ("*.tmp", False),
    ]
    
    total_removed = 0
    total_failed = 0
    
    for item, create_backup in items:
        if remove_item(item, create_backup):
            total_removed += 1
        else:
            total_failed += 1
    
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print_success(f"Limpeza concluída: {total_removed} itens removidos")
    if total_failed > 0:
        print_warning(f"{total_failed} itens falharam")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")


def cleanup_soft():
    """Limpa apenas ficheiros temporários"""
    print_header("🧹 LIMPAR APENAS FICHEIROS TEMPORARIOS")
    
    items = [
        ("temp_embeddings/", False),
        ("pending_uploads/", False),
        ("__pycache__/", False),
        ("*.pyc", False),
        ("*.log", False),
        ("test_*.txt", False),
        ("teste_*.txt", False),
        ("*.tmp", False),
    ]
    
    total_removed = 0
    
    for item, _ in items:
        if remove_item(item, False):
            total_removed += 1
    
    print(f"\n{Colors.CYAN}{'='*60}{Colors.RESET}")
    print_success(f"Limpeza concluída: {total_removed} itens removidos")
    print_info("Dados permanentes preservados: embeddings/, document_vector.json")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")


def show_status():
    """Mostra status do sistema (tamanhos)"""
    print_header("📊 STATUS DO SISTEMA")
    
    items = {
        "Embeddings permanentes": "embeddings/",
        "Embeddings temporários": "temp_embeddings/",
        "Uploads pendentes": "pending_uploads/",
        "Vetor de documentos": "document_vector.json",
        "Índice FAISS": "faiss_index.faiss",
        "Backups": "backups/",
    }
    
    total_size = 0
    
    for name, path in items.items():
        if os.path.exists(path):
            size = get_size(path)
            total_size += size
            
            if os.path.isdir(path):
                num_files = sum(1 for _ in Path(path).rglob('*') if _.is_file())
                print(f"  📁 {name}: {format_size(size)} ({num_files} ficheiros)")
            else:
                print(f"  📄 {name}: {format_size(size)}")
        else:
            print(f"  ➖ {name}: não existe")
    
    print(f"\n{Colors.BOLD}  💾 Total: {format_size(total_size)}{Colors.RESET}\n")
    print(f"{Colors.CYAN}{'='*60}{Colors.RESET}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Script para limpar ficheiros gerados pelo sistema",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Limpeza sem confirmação'
    )
    
    parser.add_argument(
        '--soft', '-s',
        action='store_true',
        help='Limpa apenas ficheiros temporários'
    )
    
    parser.add_argument(
        '--status',
        action='store_true',
        help='Mostrar status sem limpar'
    )
    
    args = parser.parse_args()
    
    # Mostrar status
    if args.status:
        show_status()
        return
    
    # Mostrar status antes da limpeza
    show_status()
    
    # Confirmação
    if not args.force:
        if args.soft:
            msg = "⚠️  Limpar ficheiros temporários? (s/n): "
        else:
            msg = f"{Colors.RED}⚠️  AVISO: Isto vai remover TODOS os dados! (s/n): {Colors.RESET}"
        
        confirm = input(msg)
        if confirm.lower() not in ['s', 'sim', 'y', 'yes']:
            print_info("Cancelado.")
            return
    
    # Executar limpeza
    if args.soft:
        cleanup_soft()
    else:
        cleanup_full()
    
    # Mostrar status após limpeza
    print_info("Status após limpeza:")
    show_status()


if __name__ == "__main__":
    main()
