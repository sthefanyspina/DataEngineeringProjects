import pandas as pd
from load import get_engine
from logger import logger

def transform_data(df):
    try:
        logger.info("Iniciando transformação de dados...")
        #Identificar e remover valores ausentes
        df.dropna(inplace=True)
        # Remover duplicatas
        df = df.drop_duplicates()
        # Normalizar nomes de colunas
        df.columns = [col.lower() for col in df.columns]
        #Remove espaços extras
        df.columns = [col.str.strip() for col in df.columns]
        # Selecionar colunas importantes
        df = df[['country', 'countrycode', 'date', 'totalconfirmed', 'totaldeaths', 'totalrecovered']]
        logger.info("Transformação concluída")
        return df
    except Exception as e:
        logger.error(f"Erro na transformação: {e}")
        raise

def save_transformed(df, table_name='covid_clean'):
    from load import load_data
    load_data(df, table_name)
