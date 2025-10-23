import pandas as pd
import random
from faker import Faker
from sqlalchemy import create_engine

# ==============================
# CONFIGURAÇÃO DO BANCO
# ==============================
# Troque para seu banco (PostgreSQL ou MySQL)
# Exemplo PostgreSQL: "postgresql://usuario:senha@localhost:5432/seu_banco"
# Exemplo MySQL: "mysql+pymysql://usuario:senha@localhost:3306/seu_banco"
DATABASE_URL = "mysql+pymysql://root:15073003@localhost:3306/data_warehouse"

engine = create_engine(DATABASE_URL)
faker = Faker('pt_BR')
random.seed(42)

# ==============================
# 1. GERAR DIM_CLIENTE
# ==============================
clientes = []
for _ in range(10000):
    clientes.append({
        "nome": faker.name(),
        "cidade": faker.city(),
        "estado": faker.estado_sigla(),
        "segmento": random.choice(["Varejo", "Atacado", "Corporativo"])
    })
df_clientes = pd.DataFrame(clientes)
df_clientes.to_sql("dim_cliente", engine, if_exists="replace", index=False)
print("✅ dim_cliente criada com", len(df_clientes), "registros.")

# ==============================
# 2. GERAR DIM_PRODUTO
# ==============================
produtos = [
    {"nome": "Notebook Dell", "categoria": "Eletrônicos", "preco": 3500.00},
    {"nome": "Mouse Logitech", "categoria": "Acessórios", "preco": 120.00},
    {"nome": "Teclado Mecânico", "categoria": "Acessórios", "preco": 450.00},
    {"nome": "Monitor LG 24''", "categoria": "Eletrônicos", "preco": 1100.00},
    {"nome": "Cadeira Gamer", "categoria": "Móveis", "preco": 950.00},
    {"nome": "Headset Razer", "categoria": "Acessórios", "preco": 800.00},
    {"nome": "Webcam Logitech", "categoria": "Acessórios", "preco": 400.00},
]
df_produtos = pd.DataFrame(produtos)
df_produtos.to_sql("dim_produto", engine, if_exists="replace", index=False)
print("✅ dim_produto criada com", len(df_produtos), "registros.")

# ==============================
# 3. GERAR DIM_TEMPO
# ==============================
datas = pd.date_range(start="2025-01-01", end="2025-09-30")
dim_tempo = [{"data": d, "ano": d.year, "mes": d.month, "dia": d.day} for d in datas]
df_tempo = pd.DataFrame(dim_tempo)
df_tempo.to_sql("dim_tempo", engine, if_exists="replace", index=False)
print("✅ dim_tempo criada com", len(df_tempo), "registros.")

# ==============================
# 4. GERAR FACT_VENDAS
# ==============================
fact_vendas = []
for _ in range(500000):  # 500 vendas sintéticas
    cliente_id = random.randint(1, len(df_clientes))
    produto_id = random.randint(1, len(df_produtos))
    tempo_id = random.randint(1, len(df_tempo))
    
    qtd = random.randint(1, 5)
    preco = df_produtos.iloc[produto_id - 1]["preco"]
    valor_total = qtd * preco
    
    fact_vendas.append({
        "id_cliente": cliente_id,
        "id_produto": produto_id,
        "id_tempo": tempo_id,
        "quantidade": qtd,
        "valor_total": valor_total
    })

df_fact = pd.DataFrame(fact_vendas)
df_fact.to_sql("fact_vendas", engine, if_exists="replace", index=False)
print("✅ fact_vendas criada com", len(df_fact), "registros.")
print("\n✅ Data Warehouse populado e análises executadas com sucesso!")
