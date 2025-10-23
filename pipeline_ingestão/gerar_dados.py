import pandas as pd
import numpy as np
import random
import string

# -------------------------
# CONFIGURAÇÕES
# -------------------------
N = 500_000
random.seed(42)
np.random.seed(42)

# -------------------------
# FUNÇÕES AUXILIARES
# -------------------------
def random_name():
    nomes = ["Joao", "Maria", "Carlos", "Ana", "Pedro", "Julia", "Lucas", "Mariana", "Felipe", "Beatriz"]
    sobrenomes = ["Silva", "Souza", "Oliveira", "Santos", "Costa", "Pereira", "Rodrigues", "Almeida", "Lima", "Araujo"]
    nome = f"{random.choice(nomes)} {random.choice(sobrenomes)}"
    # Introduz erros de capitalização
    var = random.choice([
        nome.lower(),
        nome.upper(),
        nome.title(),
        nome.capitalize(),
        nome[:3].upper() + nome[3:].lower()
    ])
    return var

def random_email(name):
    domains = ["gmail.com", "hotmail.com", "yahoo.com", "empresa.com"]
    user = name.replace(" ", "").lower()
    return f"{user}@{random.choice(domains)}"

def random_phone():
    return f"+55{random.randint(10,99)}9{random.randint(1000,9999)}{random.randint(1000,9999)}"

def random_city():
    cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Recife", "Porto Alegre", "Salvador"]
    return random.choice(cidades)

# -------------------------
# GERAÇÃO DE DADOS
# -------------------------
data = {
    "id": np.arange(1, N + 1),
    "nome": [random_name() for _ in range(N)],
    "idade": np.random.randint(18, 80, N),
    "renda": np.random.uniform(1000, 20000, N).round(2),
    "cidade": [random_city() for _ in range(N)],
}

df = pd.DataFrame(data)

# -------------------------
# ADICIONAR ERROS
# -------------------------

# 1️⃣ Inserir valores ausentes (NaN)
for col in ["nome", "idade", "renda", "cidade"]:
    idx = np.random.choice(df.index, size=int(0.03 * N), replace=False)
    df.loc[idx, col] = np.nan

# 2️⃣ Inserir duplicados (2% das linhas)
duplicates = df.sample(frac=0.02, random_state=42)
df = pd.concat([df, duplicates])

# 3️⃣ Inserir erros de tipo (string em idade e renda)
for col in ["idade", "renda"]:
    idx = np.random.choice(df.index, size=int(0.01 * len(df)), replace=False)
    df.loc[idx, col] = random.choice(["erro", "n/a", "?", "None", "xx"])

# 4️⃣ Adicionar emails e telefones (com alguns inválidos)
df["email"] = [random_email(n if isinstance(n, str) else "usuario") for n in df["nome"]]
df["telefone"] = [random_phone() for _ in range(len(df))]

# Emails inválidos em 1%
idx_invalid = np.random.choice(df.index, size=int(0.01 * len(df)), replace=False)
df.loc[idx_invalid, "email"] = df.loc[idx_invalid, "email"].str.replace("@", "", regex=False)

# 5️⃣ Bagunçar capitalização em cidade
df["cidade"] = df["cidade"].apply(lambda x: str(x).lower() if random.random() < 0.5 else str(x).upper())

# -------------------------
# EMBARALHAR E SALVAR
# -------------------------
df = df.sample(frac=1, random_state=42).reset_index(drop=True)
df.to_csv("input/dados_sujo.csv", index=False)

print("✅ Dataset 'dados_sujo.csv' criado com sucesso!")
