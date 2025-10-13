import pandas as pd
import numpy as np

# Configuração de seed para reprodutibilidade
np.random.seed(42)

# Número de linhas
n = 200_000

# Gerar IDs de vendas únicos
id_venda = np.arange(1, n + 1)

# Gerar nomes de clientes aleatórios
nomes = ['Ana', 'Carlos', 'Beatriz', 'João', 'Marcos', 'Julia', 'Paula', 'Rafael', 'Fernanda', 'Lucas']
cliente = np.random.choice(nomes, size=n)

# Valores de venda aleatórios entre 10 e 1000
valor_venda = np.random.uniform(10, 1000, size=n).round(2)

# Criar DataFrame
df = pd.DataFrame({
    'id_venda': id_venda,
    'cliente': cliente,
    'valor_venda': valor_venda
})

# Salvar CSV
df.to_csv("input/vendas.csv", index=False)

print("CSV 'vendas.csv' criado com sucesso!")
