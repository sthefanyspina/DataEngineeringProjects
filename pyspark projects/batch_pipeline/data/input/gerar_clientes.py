import csv
import random

# Configurações
num_clientes = 200_000

# Gerar dados de clientes
with open("clientes.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    # Cabeçalho
    writer.writerow(["id", "nome", "idade", "renda"])
    
    for cliente_id in range(1, num_clientes + 1):
        nome = f"Cliente_{cliente_id}"
        idade = random.randint(18, 80)
        renda = round(random.uniform(1000.0, 10000.0), 2)
        writer.writerow([cliente_id, nome, idade, renda])
