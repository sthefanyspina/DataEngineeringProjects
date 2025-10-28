# ==========================================
# validate_data_soda.py
# ==========================================
import os
import subprocess
import yaml
import json

# -------------------------
# 1️⃣ Configurações de Conexão com Snowflake
# -------------------------
SNOWFLAKE_CONFIG = {
    "type": "snowflake",
    "account": "seu_account_id",        # Exemplo: ab12345.sa-east-1
    "username": "seu_usuario",
    "password": "sua_senha",
    "role": "ANALYST_ROLE",
    "warehouse": "COMPUTE_WH",
    "database": "DATA_QUALITY_DB",
    "schema": "PUBLIC",
}

# -------------------------
# 2️⃣ Criação dos Arquivos de Configuração
# -------------------------
os.makedirs("soda", exist_ok=True)

configuration_yml = {
    "data_source snowflake_db": SNOWFLAKE_CONFIG
}

with open("soda/configuration.yml", "w") as f:
    yaml.dump(configuration_yml, f, sort_keys=False)

# -------------------------
# 3️⃣ Definição das Validações (20 regras)
# -------------------------
checks_yml = """
checks for snowflake_db:
  - schema:
      warn:
        when schema changes: any
  - missing_count(nome) = 0
  - duplicate_count(id) = 0
  - invalid_count(email) = 0:
      valid format: email
  - invalid_count(cpf) = 0:
      valid format: regex
      regex: '^[0-9]{11}$'
  - invalid_count(percentual) = 0:
      valid min: 0
      valid max: 100
  - invalid_count(parcelas) = 0:
      valid min: 1
      valid max: 12
  - invalid_count(quantidade) = 0:
      valid min: 0
  - invalid_count(preco) = 0:
      valid min: 0
  - values in (status) must exist:
      in:
        - ativo
        - inativo
        - pendente
  - invalid_count(data_inicio) = 0:
      valid format: date_iso8601
  - invalid_count(data_fim) = 0:
      valid format: date_iso8601
  - row_count > 0
  - failed rows:
      name: date_consistency
      fail condition: data_inicio > data_fim
      fail message: "data_inicio não pode ser maior que data_fim"
  - failed rows:
      name: valor_total_check
      fail condition: valor_total < preco * quantidade - 10 or valor_total > preco * quantidade + 10
      fail message: "valor_total inconsistente com preco * quantidade"
  - failed rows:
      name: negative_quantidade
      fail condition: quantidade < 0
  - failed rows:
      name: created_vs_updated
      fail condition: created_at > updated_at
  - failed rows:
      name: country_state
      fail condition: (pais = 'BR' and estado not in ('SP', 'RJ')) or (pais = 'US' and estado <> 'CA')
  - failed rows:
      name: id_cliente_valid
      fail condition: id_cliente <= 0
  - failed rows:
      name: valor_total_negativo
      fail condition: valor_total < 0
  - missing_count(updated_at) <= 10
"""

with open("soda/checks.yml", "w") as f:
    f.write(checks_yml.strip())

# -------------------------
# 4️⃣ Execução do Scan Soda
# -------------------------
print("🔍 Executando validações com Soda Core...")

result = subprocess.run(
    [
        "soda", "scan",
        "-d", "snowflake_db",
        "-c", "soda/configuration.yml",
        "soda/checks.yml"
    ],
    capture_output=True,
    text=True
)

os.makedirs("dq_reports", exist_ok=True)
with open("dq_reports/soda_output.log", "w") as f:
    f.write(result.stdout)

print(result.stdout)

# -------------------------
# 5️⃣ Geração de Relatório JSON
# -------------------------
summary = {
    "exit_code": result.returncode,
    "success": result.returncode == 0,
    "output": result.stdout.splitlines()[-10:]
}

with open("dq_reports/soda_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print("✅ Validação concluída! Relatórios salvos em dq_reports/")
