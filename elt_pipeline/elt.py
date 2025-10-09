import requests
import pandas as pd
import logging
import sys
from sqlalchemy import create_engine

# ========== LOGGER ==========
sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    filename="elt.log",           # arquivo onde o log será salvo
    level=logging.INFO,                    # nível de log (DEBUG, INFO, WARNING, ERROR)
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)
logger = logging.getLogger()

# Também mostrar logs no console
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)



# ========== CONFIGURAÇÕES ==========
DB_CONFIG = {
    'user': 'root',
    'password': '15073003the',
    'host': 'localhost',
    'port': '3306',
    'database': 'covid_db'
}

API_CONFIG = {
    'url': "https://disease.sh/v3/covid-19/countries"
}

# ========== CONEXÃO COM BANCO ==========
def get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

# ========== EXTRAÇÃO ==========
def extract_data() -> pd.DataFrame:
    logger.info("📥 Starting data extraction from API...")
    response = requests.get(API_CONFIG['url'])
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        logger.info(f"✅ Extraction completed: {len(df)} records")
        return df
    else:
        logger.error(f"❌ Error fetching data. HTTP code: {response.status_code}")
        raise Exception(f"Error fetching data: {response.status_code}")

# ========== CARGA ==========
def load_data(df: pd.DataFrame, table_name: str):
    try:
        logger.info(f"📤 Loading data into the table '{table_name}'...")
        engine = get_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"✅ Data loaded successfully into '{table_name}'")
    except Exception as e:
        logger.error(f"❌ Error loading data: {e}")
        raise

# ========== TRANSFORMAÇÃO ==========
def transform_data(table_name: str) -> pd.DataFrame:
    try:
        logger.info("🔄 Starting data transformation...")
        engine = get_engine()

        # Lê a tabela bruta do banco
        df = pd.read_sql_table(table_name, con=engine)

        # Processamento
        df.dropna(inplace=True)
        df = df.drop_duplicates()
        df.columns = [col.lower().strip() for col in df.columns]

        selected_columns = [
            'country',
            'countryinfo.iso2',
            'cases',
            'deaths',
            'recovered'
        ]

        # Verifica colunas esperadas
        missing = [col for col in selected_columns if col not in df.columns]
        if missing:
            raise Exception(f"Missing expected columns in data: {missing}")

        df = df[selected_columns]
        df.columns = [
            'country',
            'countrycode',
            'totalconfirmed',
            'totaldeaths',
            'totalrecovered'
        ]

        logger.info("✅ Transformation completed")
        return df
    except Exception as e:
        logger.error(f"❌ Error in transformation: {e}")
        raise

# ========== ORQUESTRADOR ELT ==========
def run_pipeline():
    try:
        logger.info("🚀 Starting ELT Pipeline...")

        # 1️⃣ Extract
        raw_df = extract_data()

        # 2️⃣ Load raw data
        load_data(raw_df, 'covid_raw')

        # 3️⃣ Transform after loading (from database)
        transformed_df = transform_data('covid_raw')

        # 4️⃣ Load transformed data
        load_data(transformed_df, 'covid_clean')

        logger.info("✅ ELT pipeline completed successfully!")
    except Exception as e:
        logger.error(f"❌ ELT pipeline failed: {e}")

# ========== EXECUÇÃO ==========
if __name__ == "__main__":
    run_pipeline()
