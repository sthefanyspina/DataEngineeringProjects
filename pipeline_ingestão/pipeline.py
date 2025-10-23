import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime

# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================

# Caminhos das pastas
INPUT_PATH = "input"
REJECT_PATH = "rejected"
LOG_PATH = "logs"

# Cria pastas se não existirem
os.makedirs(INPUT_PATH, exist_ok=True)
os.makedirs(REJECT_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

# Configuração de log
log_file = os.path.join(LOG_PATH, f"ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ==========================================
# CONEXÃO COM O BANCO DE DADOS MYSQL
# ==========================================

# 🔧 Ajuste conforme seu ambiente MySQL
# mysql+pymysql://usuario:senha@host:porta/banco
DB_URL = "mysql+pymysql://root:password@localhost:3306/ingest_db"

try:
    engine = create_engine(DB_URL)
    logging.info("Conexão com o banco MySQL estabelecida com sucesso.")
except Exception as e:
    logging.error(f"Erro ao conectar ao banco MySQL: {str(e)}")
    raise SystemExit("❌ Falha na conexão com o banco. Verifique as credenciais e a disponibilidade.")

# ==========================================
# FUNÇÕES AUXILIARES
# ==========================================

def validar_dados(df):
    """Validações básicas de estrutura e tipos"""
    erros = []

    # Exemplo: verificar se coluna obrigatória existe
    colunas_obrigatorias = ["id", "nome", "idade", "renda"]
    for col in colunas_obrigatorias:
        if col not in df.columns:
            erros.append(f"Coluna obrigatória ausente: {col}")

    # Tipos de dados
    if "idade" in df.columns:
        try:
            df["idade"] = df["idade"].astype(int)
        except Exception:
            erros.append("Erro de tipo na coluna 'idade'")

    if "renda" in df.columns:
        try:
            df["renda"] = df["renda"].astype(float)
        except Exception:
            erros.append("Erro de tipo na coluna 'renda'")

    if df.isnull().sum().any():
        erros.append("Ha valores nulos no dataset")

    return erros


def salvar_rejeitados(df, file_name):
    """Salva registros rejeitados"""
    reject_file = os.path.join(
        REJECT_PATH,
        f"rejected_{os.path.splitext(file_name)[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    )
    df.to_csv(reject_file, index=False)
    logging.warning(f"Registros rejeitados salvos em {reject_file}")


def processar_arquivo(file_path, file_name):
    """Processa e carrega um arquivo CSV/JSON"""
    try:
        # Leitura do arquivo
        if file_name.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_name.endswith(".json"):
            df = pd.read_json(file_path, lines=True)
        else:
            logging.warning(f"Formato não suportado: {file_name}")
            return

        logging.info(f"Arquivo lido com sucesso: {file_name} ({len(df)} registros)")

        # Validação
        erros = validar_dados(df)
        if erros:
            logging.error(f"Validação falhou para {file_name}: {erros}")
            salvar_rejeitados(df, file_name)
            return

        # Carga no banco MySQL
        df.to_sql("dados_ingestao", engine, if_exists="append", index=False)
        logging.info(f"Arquivo {file_name} carregado com sucesso no banco MySQL.")

    except Exception as e:
        logging.error(f"Erro ao processar {file_name}: {str(e)}")
        try:
            salvar_rejeitados(df, file_name)
        except Exception as ex:
            logging.error(f"Falha ao salvar rejeitados de {file_name}: {str(ex)}")

# ==========================================
# EXECUÇÃO PRINCIPAL
# ==========================================

if __name__ == "__main__":
    logging.info("==== Início do pipeline de ingestão ====")

    arquivos = os.listdir(INPUT_PATH)
    if not arquivos:
        logging.info("Nenhum arquivo encontrado na pasta input/")
    else:
        for file_name in arquivos:
            file_path = os.path.join(INPUT_PATH, file_name)
            processar_arquivo(file_path, file_name)

    logging.info("==== Pipeline finalizado ====")
