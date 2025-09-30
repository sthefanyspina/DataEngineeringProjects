from sqlalchemy import create_engine
from config import DB_CONFIG
from logger import logger

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def load_data(df, table_name):
    try:
        logger.info(f"Carregando dados na tabela {table_name}...")
        engine = get_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Dados carregados com sucesso em {table_name}")
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {e}")
        raise
