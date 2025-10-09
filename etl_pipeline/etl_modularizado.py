import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# -------------------------
# 1. EXTRAÇÃO
# -------------------------
def extract_data(url: str) -> pd.DataFrame:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        print("✅ EXTRAÇÃO concluída!")
        return df
    else:
        raise Exception(f"Erro ao buscar dados: {response.status_code}")

# -------------------------
# 2. TRANSFORMAÇÃO
# -------------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Selecionar apenas colunas de interesse
    df = df[["country", "cases", "deaths", "recovered", "population"]]

    # Remover duplicatas
    df = df.drop_duplicates()

    # Renomear para snake_case
    df.columns = df.columns.str.lower()

# Verifica e substitui 'inf' e divide corretamente
    df["cases_per_million"] = np.where(
        df["population"] > 0, (df["cases"] / df["population"]) * 1_000_000, np.nan
    )

    print("✅ TRANSFORMAÇÃO concluída!")
    return df

# -------------------------
# 3. CARREGAMENTO
# -------------------------
def load_data(df: pd.DataFrame, table_name: str, conn_string: str):
    engine = create_engine(conn_string)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ CARREGAMENTO concluído! Dados salvos na tabela '{table_name}'")

# -------------------------
# 4. PIPELINE PRINCIPAL
# -------------------------
def run_pipeline():
    # URL da API pública
    url = "https://disease.sh/v3/covid-19/countries"

    # Conexão com banco (PostgreSQL como exemplo)
    conn_string = "mysql+mysqlconnector://root:senha@localhost/covid_database_teste"

    try:
        raw_df = extract_data(url)
        transformed_df = transform_data(raw_df)
        load_data(transformed_df, "covid_data", conn_string)
        print("🚀 Pipeline ETL finalizado com sucesso!")
    except Exception as e:
        print(f"❌ Erro no pipeline: {e}")

# Executar pipeline
if __name__ == "__main__":
    run_pipeline()