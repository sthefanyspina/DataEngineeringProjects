import pandas as pd
import numpy as np
import os

# -----------------------------
# CONFIGURAÇÃO
# -----------------------------
np.random.seed(42)
os.makedirs("data", exist_ok=True)

# Período de 3 anos (2022–2024)
datas = pd.date_range(start="2022-01-01", end="2024-12-31", freq="D")

# Lojas e produtos
lojas = ["A", "B", "C", "D", "E"]
produtos = [f"P{i:03d}" for i in range(1, 21)]  # 20 produtos

# Gera combinações loja-produto-data (~200 mil linhas)
linhas = []
for loja in lojas:
    for produto in produtos:
        base = 100 + np.random.randint(-10, 10)  # base inicial diferente por produto
        tendencia = np.linspace(0, 50, len(datas))  # leve crescimento ao longo do tempo
        sazonal = 30 * np.sin(np.linspace(0, 24*np.pi, len(datas)))  # sazonalidade anual
        ruido = np.random.normal(0, 20, len(datas))  # ruído aleatório
        vendas = base + tendencia + sazonal + ruido

        df_temp = pd.DataFrame({
            "data": datas,
            "loja": loja,
            "produto": produto,
            "vendas": np.maximum(vendas, 0).astype(int)  # evita vendas negativas
        })
        linhas.append(df_temp)

df_final = pd.concat(linhas, ignore_index=True)

# Amostra total de 5 lojas × 20 produtos × ~1.095 dias ≈ 109.500 linhas
# Para chegar em 200k, duplicamos com pequenas variações aleatórias
df_final_extra = df_final.copy()
df_final_extra["vendas"] = (df_final_extra["vendas"] * np.random.uniform(0.9, 1.1, len(df_final_extra))).astype(int)

df_final = pd.concat([df_final, df_final_extra], ignore_index=True)
print(f"Total de linhas geradas: {len(df_final):,}")

# -----------------------------
# SALVAR
# -----------------------------
df_final.to_csv("data/vendas.csv", index=False)
print("✅ Arquivo salvo em data/vendas.csv")
