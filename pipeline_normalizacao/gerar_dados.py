import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# -------------------------------
# 1. Configurações básicas
# -------------------------------
N = 500_000  # número de linhas

nomes = ["João", "Maria", "Ana", "Carlos", "Beatriz", "Marcos", "Paula", "José", "Fernanda", "Rafael"]
cidades = ["São Paulo", "rio de janeiro", "belo horizonte", "CURITIBA", "Porto Alegre", "salvador", "recife"]
formatos_datas = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y", "%d.%m.%Y"]

# -------------------------------
# 2. Gerar colunas corretas (base limpa)
# -------------------------------
def gerar_data():
    base = datetime(1970, 1, 1)
    dias = random.randint(0, 18000)  # até ~2020
    return base + timedelta(days=dias)

df = pd.DataFrame({
    "Nome": [random.choice(nomes) for _ in range(N)],
    "Data_nasc": [gerar_data().strftime(random.choice(formatos_datas)) for _ in range(N)],
    "Salário": [f"{random.randint(1000, 15000):,}".replace(",", ".") for _ in range(N)],
    "Cidade": [random.choice(cidades) for _ in range(N)]
})

# -------------------------------
# 3. Introduzir ERROS intencionais
# -------------------------------

# 3.1 Renomear algumas colunas erradas
df.columns = ["nome ", "dataNasc", "salario_reais", "cidade  "]  # espaços, nomes inconsistentes

# 3.2 Inserir valores ausentes (NaN)
for col in df.columns:
    df.loc[df.sample(frac=0.02).index, col] = np.nan  # 2% de valores ausentes

# 3.3 Inserir tipos errados
df.loc[df.sample(frac=0.01).index, "salario_reais"] = "mil"      # salário como texto
df.loc[df.sample(frac=0.01).index, "dataNasc"] = "31/02/2020"   # data impossível
df.loc[df.sample(frac=0.01).index, "cidade  "] = 12345           # número no lugar da cidade
df.loc[df.sample(frac=0.01).index, "nome "] = True               # boolean no nome

# 3.4 Inserir formatações quebradas
df.loc[df.sample(frac=0.005).index, "salario_reais"] = "5,000"   # mistura vírgula e ponto
df.loc[df.sample(frac=0.005).index, "dataNasc"] = "20201301"     # formato errado

# 3.5 Inserir espaços e capitalização errada
df["cidade  "] = df["cidade  "].astype(str) + ["  " if random.random() < 0.2 else "" for _ in range(N)]
df["nome "] = df["nome "].astype(str).str.lower()

# 3.6 Inserir duplicatas (5%)
df = pd.concat([df, df.sample(frac=0.05, random_state=42)], ignore_index=True)

# -------------------------------
# 4. Salvar dataset "bagunçado"
# -------------------------------
df.to_csv("dados_bagunçados.csv", index=False, encoding="utf-8")

print("✅ Dataset bagunçado criado com sucesso!")
print(f"Linhas totais: {len(df):,}")
print("Arquivo salvo como: dados_bagunçados.csv")
