import requests
import pandas as pd
import logging
from sqlalchemy import create_engine
import sys
import numpy as np

# -------------------------
# CONFIGURAÇÃO DO LOG
# -------------------------
# Força UTF-8 no stdout para evitar problemas no Windows
sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    filename="etl_pipeline.log",           # arquivo onde o log será salvo
    level=logging.INFO,                    # nível de log (DEBUG, INFO, WARNING, ERROR)
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)

# Também mostrar logs no console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

# -------------------------
# 1. EXTRAÇÃO
# -------------------------
def extract_data(url: str) -> pd.DataFrame:
    logging.info("Iniciando extracao de dados.")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        logging.info(f"Extracao concluída. Linhas extraídas: {len(df)}")
        return df
    else:
        logging.error(f"Erro ao buscar dados. Código HTTP: {response.status_code}")
        raise Exception(f"Erro ao buscar dados: {response.status_code}")

# -------------------------
# 2. TRANSFORMAÇÃO
# -------------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Iniciando transformacao dos dados.")
    try:
        df = df[["country", "cases", "deaths", "recovered", "population"]]
        df = df.drop_duplicates()
        df.columns = df.columns.str.lower()

       # Verifica e substitui 'inf' e divide corretamente
        df["cases_per_million"] = np.where(
            df["population"] > 0, (df["cases"] / df["population"]) * 1_000_000, np.nan
        )

        logging.info(f"Transformacao concluída. Linhas após limpeza: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Erro na transformacao: {e}")
        raise

# -------------------------
# 3. CARREGAMENTO
# -------------------------
def load_data(df: pd.DataFrame, table_name: str, conn_string: str):
    logging.info(f"Iniciando carregamento para a tabela '{table_name}'.")
    try:
        engine = create_engine(conn_string)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Carregamento concluído. {len(df)} linhas inseridas na tabela '{table_name}'.")
    except Exception as e:
        logging.error(f"Erro no carregamento: {e}")
        raise

# -------------------------
# 4. PIPELINE PRINCIPAL
# -------------------------
def run_pipeline():
    url = "https://disease.sh/v3/covid-19/countries"
    conn_string = "mysql+mysqlconnector://root:senha@localhost/covid_database"

    logging.info("Iniciando pipeline ETL.")
    try:
        raw_df = extract_data(url)
        transformed_df = transform_data(raw_df)
        load_data(transformed_df, "covid_data", conn_string)
        logging.info("Pipeline finalizado com sucesso!")
    except Exception as e:
        logging.error(f"Pipeline falhou: {e}")

# Executar
if __name__ == "__main__":
    run_pipeline()