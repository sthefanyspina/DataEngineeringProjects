import pandas as pd
import numpy as np

# 1. Ler o dataset "sujo"
df = pd.read_csv("penguins_Iter",  encoding="utf-8")

# 2. (Opcional) Identificar e remover valores ausentes
df = df.dropna(inplace=True)

# 3. (Opcional) Remover duplicatas
df = df.drop_duplicates()

# OU Preencher valores ausentes com a média (para colunas numéricas)
df['coluna_numerica'].fillna(df['coluna_numerica'].mean(), inplace=True)


# 4. Normalizar coluna -> tirar espaços, padronizar capitalização
df['coluna'] = df['coluna'].str.strip().str.title()


# 5. Normalizar datas -> transformar para formato padrão YYYY-MM-DD
df['coluna_data'] = pd.to_datetime(df['coluna_data'], errors='coerce', dayfirst=True)


# 6. Normalizar salários -> remover separadores de milhar, trocar vírgula por ponto, converter para float
df['coluna'] = df['coluna'].astype(str)
df['coluna'] = df['coluna'].str.replace('.', '', regex=False)  # remove pontos
df['coluna'] = df['coluna'].str.replace(',', '.', regex=False)  # troca vírgula por ponto
df['coluna'] = df['coluna'].astype(float)


# 7. Salvar dataset "limpo"
df.to_csv("dados_limpos.csv", index=False)

print("Pipeline concluído! Arquivo salvo como 'dados_limpos.csv'")