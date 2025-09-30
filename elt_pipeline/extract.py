import requests
import pandas as pd
from config import API_CONFIG
from logger import logger

def extract_data():
    try:
        logger.info("Iniciando extração de dados da API...")
        response = requests.get(API_CONFIG['url'])
        response.raise_for_status()  # erro se status != 200
        data = response.json()
        df = pd.DataFrame(data['Countries'])
        logger.info(f"Extração concluída: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"Erro na extração: {e}")
        raise
