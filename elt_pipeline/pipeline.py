from extract import extract_data
from transform import transform_data, save_transformed
from load import load_data
from logger import logger

def run_pipeline():
    try:
        # 1️⃣ Extrair
        df_raw = extract_data()
        # 2️⃣ Carregar brutos
        load_data(df_raw, 'covid_raw')
        # 3️⃣ Transformar
        df_clean = transform_data(df_raw)
        # 4️⃣ Carregar transformados
        save_transformed(df_clean)
        logger.info("Pipeline ELT concluído com sucesso!")
    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")

if __name__ == "__main__":
    run_pipeline()
