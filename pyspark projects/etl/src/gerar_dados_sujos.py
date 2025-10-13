import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker('pt_BR')
os.makedirs("data", exist_ok=True)

# ===================================================
# 1️⃣ GERAR CLIENTES COM DADOS SUJOS
# ===================================================
num_clientes = 20000
clientes = []

estados = ["SP", "rj", "Mg", "rs", "Pr", "BA", "PE", "ce", "Df", "sc"]  # capitalização inconsistente

for i in range(1, num_clientes + 1):
    nome = fake.name()
    # erro de capitalização no nome
    nome = random.choice([nome.upper(), nome.lower(), nome.title()])

    idade = random.randint(18, 70)
    # 5% das idades viram strings inválidas
    if random.random() < 0.05:
        idade = random.choice(["vinte", "trinta", "", None])

    clientes.append({
        "ID_CLIENTE": i if random.random() > 0.01 else "",  # alguns IDs vazios
        "Nome": nome,
        "Idade": idade,
        "Genero": random.choice(["M", "f", "F", "m"]),
        "Estado": random.choice(estados),
        "Cidade": fake.city() if random.random() > 0.02 else "",  # cidade vazia
    })

df_clientes = pd.DataFrame(clientes)

# Duplicar ~1% das linhas
df_clientes = pd.concat([df_clientes, df_clientes.sample(frac=0.01)], ignore_index=True)

df_clientes.to_csv("data/clientes.csv", index=False)

# ===================================================
# 2️⃣ GERAR PRODUTOS COM DADOS SUJOS
# ===================================================
num_produtos = 5000
produtos = []
categorias = {
    "eletrônicos": ["Notebook", "Smartphone", "Tablet", "Fone de Ouvido", "Monitor"],
    "ROUPAS": ["Camiseta", "Calça", "Jaqueta", "Vestido", "Tênis"],
    "Beleza": ["Perfume", "Shampoo", "Creme", "Maquiagem", "Sabonete"],
    "Alimentos": ["Arroz", "Feijão", "Macarrão", "Café", "Leite"],
    "Casa": ["Sofá", "Cadeira", "Mesa", "Geladeira", "Fogão"]
}

id_produto = 1
for categoria, lista in categorias.items():
    for item in lista:
        for _ in range(200):  # repete para ter volume
            preco = round(random.uniform(10, 10000), 2)
            # 5% dos preços viram strings
            if random.random() < 0.05:
                preco = random.choice(["dez", "cem", "", None])

            produtos.append({
                "id_produto": id_produto if random.random() > 0.01 else "",
                "Produto": random.choice([item.upper(), item.lower(), item.title()]),
                "CATEGORIA": categoria,
                "Preco": preco
            })
            id_produto += 1

df_produtos = pd.DataFrame(produtos).head(20000)

# Inserir duplicatas (~2%)
df_produtos = pd.concat([df_produtos, df_produtos.sample(frac=0.02)], ignore_index=True)

df_produtos.to_csv("data/produtos.csv", index=False)

# ===================================================
# 3️⃣ GERAR VENDAS COM DADOS SUJOS
# ===================================================
num_vendas = 20000
vendas = []
data_inicial = datetime(2023, 1, 1)
data_final = datetime(2024, 12, 31)

for i in range(1, num_vendas + 1):
    produto = df_produtos.sample(1).iloc[0]
    cliente_id = random.randint(1, num_clientes)

    data_venda = data_inicial + timedelta(days=random.randint(0, 730))
    data_venda_str = data_venda.strftime("%Y-%m-%d")

    # 5% com formato errado de data
    if random.random() < 0.05:
        data_venda_str = data_venda.strftime("%d/%m/%Y")

    valor = produto["Preco"]
    # 5% dos valores com tipo errado
    if random.random() < 0.05:
        valor = random.choice(["mil", "3000reais", "", None])

    vendas.append({
        "id_Venda": i if random.random() > 0.01 else "",
        "ID_CLIENTE": cliente_id,
        "id_produto": produto["id_produto"],
        "Produto": produto["Produto"],
        "valor_venda": valor,
        "DATA_VENDA": data_venda_str
    })

df_vendas = pd.DataFrame(vendas)

# Duplicar 1%
df_vendas = pd.concat([df_vendas, df_vendas.sample(frac=0.01)], ignore_index=True)

df_vendas.to_csv("data/vendas.csv", index=False)

print("✅ Arquivos CSV GERADOS (com erros e duplicatas) em /data com sucesso!")
