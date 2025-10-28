# ==========================================
# generate_data.py
# ==========================================
import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def random_cpf():
    return ''.join(str(random.randint(0, 9)) for _ in range(11))

def random_email(name):
    domains = ['gmail.com', 'empresa.com', 'exemplo.org']
    return f"{name.lower()}@{random.choice(domains)}"

def generate_dataset(n=5000, seed=42):
    random.seed(seed)
    np.random.seed(seed)

    ids = list(range(1, n + 1))
    nomes = [f"User{str(i).zfill(4)}" for i in ids]
    cpfs = [random_cpf() for _ in ids]
    emails = [random_email(nomes[i - 1]) for i in ids]

    base_date = datetime(2024, 1, 1)
    data_inicio = [base_date + timedelta(days=random.randint(0, 365)) for _ in ids]
    data_fim = [d + timedelta(days=random.randint(0, 30)) for d in data_inicio]
    for i in range(0, n, 200):
        data_inicio[i] = data_fim[i] + timedelta(days=1)

    pais = ['BR' if random.random() < 0.8 else 'US' for _ in ids]
    estado = [('SP' if p == 'BR' and random.random() < 0.5 else 'RJ' if p == 'BR' else 'CA') for p in pais]
    cidade = [f"City{random.randint(1,200)}" for _ in ids]

    percentual = np.random.rand(n) * 100
    for i in range(0, n, 333):
        percentual[i] = 150.0
    for i in range(50, n, 400):
        percentual[i] = -5.0

    quantidade = np.random.randint(0, 20, size=n)
    for i in range(30, n, 500):
        quantidade[i] = -3

    preco = np.round(np.random.rand(n) * 1000, 2)
    status = [random.choice(['ativo', 'inativo', 'pendente']) for _ in ids]
    id_cliente = [random.randint(1, int(n / 2)) for _ in ids]
    valor_total = np.round(preco * quantidade + np.random.rand(n) * 10, 2)
    parcelas = [random.randint(1, 12) for _ in ids]

    created_at = [d - timedelta(days=random.randint(0, 10)) for d in data_inicio]
    updated_at = [c + timedelta(days=random.randint(0, 5)) for c in created_at]
    for i in range(0, n, 250):
        updated_at[i] = None

    df = pd.DataFrame({
        'id': ids,
        'nome': nomes,
        'cpf': cpfs,
        'email': emails,
        'data_inicio': data_inicio,
        'data_fim': data_fim,
        'pais': pais,
        'estado': estado,
        'cidade': cidade,
        'percentual': percentual,
        'quantidade': quantidade,
        'preco': preco,
        'status': status,
        'id_cliente': id_cliente,
        'valor_total': valor_total,
        'parcelas': parcelas,
        'created_at': created_at,
        'updated_at': updated_at
    })

    # duplicatas propositalmente
    df = pd.concat([df, df.sample(10, random_state=seed)], ignore_index=True)

    return df

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    df = generate_dataset(5000)
    df.to_csv("data/synthetic_data.csv", index=False)
    print("✅ Dataset sintético salvo em data/synthetic_data.csv")
