# ==========================================
# validate_data_ge.py
# ==========================================
import os
import great_expectations as ge
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.dataset import PandasDataset

# -------------------------
# 1️⃣ Configuração de Contexto
# -------------------------
BASE_DIR = os.getcwd()
DATA_PATH = os.path.join(BASE_DIR, "data", "synthetic_data.csv")
GE_DIR = os.path.join(BASE_DIR, "great_expectations")

# cria estrutura mínima, se não existir
if not os.path.exists(GE_DIR):
    ge.DataContext.create(os.getcwd())

context = BaseDataContext(context_root_dir=GE_DIR)

# -------------------------
# 2️⃣ Carregar Dataset
# -------------------------
df = ge.read_csv(DATA_PATH)
print(f"✅ Dataset carregado: {df.shape[0]} linhas, {df.shape[1]} colunas")

# -------------------------
# 3️⃣ Criar Expectation Suite
# -------------------------
SUITE_NAME = "data_quality_suite"
try:
    suite = context.get_expectation_suite(SUITE_NAME)
except:
    suite = context.create_expectation_suite(SUITE_NAME, overwrite_existing=True)

# -------------------------
# 4️⃣ Adicionar 20 Expectativas
# -------------------------
df.expect_column_values_to_not_be_null("nome")
df.expect_column_values_to_be_unique("id")
df.expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+")
df.expect_column_values_to_be_between("preco", min_value=0, max_value=None)
df.expect_column_pair_values_to_be_in_set("data_inicio", "data_fim", [("<= ")] )  # Apenas simbólica
df.expect_column_values_to_be_between("percentual", 0, 100)
df.expect_column_values_to_be_in_set("status", ["ativo", "inativo", "pendente"])
df.expect_table_row_count_to_be_between(min_value=1, max_value=10000)
df.expect_column_pair_values_to_be_equal("valor_total", "quantidade")  # simplificação de consistência
df.expect_column_pair_values_to_be_increasing("created_at", "updated_at")
df.expect_column_values_to_be_between("parcelas", 1, 12)
df.expect_column_values_to_be_in_set("estado", ["SP", "RJ", "CA"])
df.expect_column_values_to_not_match_regex("data_fim", r"2999")  # evitar datas absurdas
df.expect_column_value_lengths_to_be_between("nome", 1, 100)
df.expect_column_values_to_be_between("id", 1, None)
df.expect_column_value_lengths_to_equal("cpf", 11)
df.expect_column_values_to_be_between("quantidade", 0, None)
df.expect_column_values_to_be_between("preco", 0, None)
df.expect_column_values_to_be_in_type_list("created_at", ["datetime64[ns]", "str"])
df.expect_column_values_to_be_between("id_cliente", 1, None)

context.save_expectation_suite(df.get_expectation_suite(SUITE_NAME))

# -------------------------
# 5️⃣ Criar Checkpoint
# -------------------------
checkpoint_name = "data_quality_checkpoint"
checkpoint_config = {
    "name": checkpoint_name,
    "config_version": 1.0,
    "class_name": "SimpleCheckpoint",
    "run_name_template": "%Y-%m-%d_%H-%M-%S",
    "validations": [
        {
            "batch_request": {
                "datasource_name": "default_pandas_datasource",
                "data_connector_name": "default_runtime_data_connector_name",
                "data_asset_name": "synthetic_data",
                "runtime_parameters": {"batch_data": df},
                "batch_identifiers": {"default_identifier_name": "default_id"},
            },
            "expectation_suite_name": SUITE_NAME,
        }
    ],
}
context.add_or_update_checkpoint(**checkpoint_config)

# -------------------------
# 6️⃣ Executar Validação
# -------------------------
print("🔍 Executando validações com Great Expectations...")
results = context.run_checkpoint(checkpoint_name=checkpoint_name)
context.build_data_docs()
print("✅ Validação concluída!")

# -------------------------
# 7️⃣ Acessar Resultados
# -------------------------
validation_result_identifier = results.list_validation_result_identifiers()[0]
data_docs_url = context.get_docs_sites_urls(resource_identifier=validation_result_identifier)[0]["site_url"]
print(f"📊 Relatório interativo disponível em:\n{data_docs_url}")
