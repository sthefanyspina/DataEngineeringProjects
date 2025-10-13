# ========================================
# Gerador de Logs Web Sintéticos (2 dias - 200k linhas)
# ========================================

import random
import datetime
import os

# -----------------------------
# CONFIGURAÇÕES
# -----------------------------
os.makedirs("data/logs", exist_ok=True)

arquivos = [
    ("data/logs/access_log_2025-10-10.log", datetime.date(2025, 10, 10)),
    ("data/logs/access_log_2025-10-11.log", datetime.date(2025, 10, 11)),
]
linhas_por_arquivo = 100_000  # total: 200k linhas

# -----------------------------
# LISTAS DE DADOS POSSÍVEIS
# -----------------------------
ips = [f"192.168.1.{i}" for i in range(1, 255)] + [f"10.0.0.{i}" for i in range(1, 255)]
metodos = ["GET", "POST", "PUT", "DELETE"]
urls = [
    "/", "/home", "/login", "/logout", "/produtos", "/carrinho", "/checkout",
    "/contato", "/sobre", "/blog", "/api/produtos", "/api/pedidos", "/api/usuarios",
    "/busca?q=iphone", "/busca?q=notebook", "/busca?q=smartwatch"
]
status_codes = [200]*80 + [301, 302]*5 + [404]*6 + [500, 503]*4  # 80% sucesso, 20% erros/redirecionamentos
user_agents = [
    "Mozilla/5.0", "Chrome/141.0.0.0", "Safari/537.36", 
    "Edge/130.0.0.0", "PostmanRuntime/7.31.1"
]

# -----------------------------
# FUNÇÃO PARA GERAR TIMESTAMP
# -----------------------------
def gerar_data_hora(dia_base):
    hora = random.randint(0, 23)
    minuto = random.randint(0, 59)
    segundo = random.randint(0, 59)
    data = datetime.datetime(
        dia_base.year, dia_base.month, dia_base.day, hora, minuto, segundo
    )
    return data.strftime("%d/%b/%Y:%H:%M:%S -0300")

# -----------------------------
# FUNÇÃO PRINCIPAL
# -----------------------------
def gerar_logs(output_file, dia_base, n_linhas):
    with open(output_file, "w") as f:
        for _ in range(n_linhas):
            ip = random.choice(ips)
            metodo = random.choice(metodos)
            url = random.choice(urls)
            status = random.choice(status_codes)
            tamanho = random.randint(200, 12000)
            datahora = gerar_data_hora(dia_base)
            agent = random.choice(user_agents)

            linha = f'{ip} - - [{datahora}] "{metodo} {url} HTTP/1.1" {status} {tamanho} "{agent}"\n'
            f.write(linha)

    print(f"✅ {output_file} gerado com {n_linhas:,} linhas para {dia_base}")

# -----------------------------
# GERAR LOGS PARA CADA DIA
# -----------------------------
for arquivo, dia in arquivos:
    gerar_logs(arquivo, dia, linhas_por_arquivo)

print("\n✅ Dataset completo gerado com sucesso!")
print("Arquivos salvos em: data/logs/")
