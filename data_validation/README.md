🧠 Monitoramento de Qualidade de Dados – Great Expectations | Soda Core | Deequ | Snowflake
📦 Descrição do Projeto

Este projeto implementa um sistema completo de Monitoramento de Qualidade de Dados utilizando três das principais ferramentas do mercado:
Great Expectations, Soda Core e Deequ, com integração adicional ao Snowflake.

O objetivo é garantir confiabilidade, consistência e conformidade dos dados antes de serem carregados em produção, detectando automaticamente problemas como:

Dados ausentes ou inválidos

Duplicatas

Inconsistência de chaves estrangeiras

Formatos incorretos (e-mail, CPF etc.)

Outliers e mudanças inesperadas no schema

⚙️ Estrutura do Projeto
data_quality_monitoring/
│
├── data/
│   └── synthetic_data.csv               # Dataset de exemplo
│
├── expectations/                        # Regras do Great Expectations
│   └── validations/                     # Relatórios automáticos
│
├── dq_reports/                          # Saída dos relatórios (Soda, Deequ)
│
├── validate_data_greatexpectations.py   # Script 1 - Great Expectations
├── validate_data_soda_snowflake.py      # Script 2 - Soda Core + Snowflake
├── validate_data_soda_local.py          # Script 3 - Soda Core (local)
├── validate_data_deequ.py               # Script 4 - Deequ (PyDeequ + Spark)
│
└── README.md                            # Este arquivo

🧩 Ferramentas Utilizadas
Categoria	Ferramenta	Função Principal
🧪 Validação Local	Great Expectations	Definir e validar expectativas de dados
☁️ Data Quality Cloud	Soda Core	Validação leve com YAML, integração com Snowflake
⚡ Big Data Quality	Deequ (PyDeequ)	Testes em escala usando Spark
🧱 Data Warehouse	Snowflake	Integração com repositório analítico
🐍 Linguagem	Python 3.10+	Automação dos scripts
🔥 Engine de Processamento	Apache Spark 3.3+	Execução distribuída para Deequ
✅ Validações Implementadas

Cada ferramenta executa as seguintes 20 verificações de qualidade de dados:

Nº	Tipo de Validação	Descrição
1	Valores Nulos	Campos obrigatórios não podem estar vazios
2	Unicidade PK	IDs devem ser únicos
3	Duplicatas	Detecção de linhas repetidas
4	Integridade Referencial	FK deve existir na tabela pai
5	Tipos de Dados	Validação contra schema esperado
6	Regex (Formato)	E-mails, CPF, CNPJ, etc.
7	Faixas Numéricas	Ex.: percentual entre 0 e 100
8	Domínios Válidos	Ex.: status ∈ {‘ativo’, ‘inativo’}
9	Regras de Negócio	Ex.: valor_total = qtd * preço
10	Datas	data_início ≤ data_fim
11	Hierarquia Geográfica	país → estado → cidade
12	Porcentagens	Totais devem somar 100%
13	Valores Negativos	Preço ou total não podem ser negativos
14	Consistência de Fontes	Dados idênticos entre tabelas
15	Cobertura de Dados	Todas as partições esperadas presentes
16	Atualização (SLA)	Dados dentro do prazo esperado
17	Volume Histórico	Detecta variações incomuns de volume
18	Outliers	Detecta valores anormais
19	Mudança de Schema	Detecção automática
20	Colunas de Auditoria	Campos created_at, updated_at obrigatórios
🧰 Instalação e Configuração

Clone o repositório:

git clone https://github.com/seuusuario/data-quality-monitoring.git
cd data-quality-monitoring


Crie um ambiente virtual e instale as dependências:

python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

📦 Instalar pacotes necessários:
Para Great Expectations:
pip install great_expectations pandas

Para Soda Core (local + Snowflake):
pip install soda-core soda-core-snowflake

Para Deequ:
pip install pydeequ pyspark


⚠️ Requer Java (JDK 8+) e Spark 3.3+ para rodar o Deequ.

🚀 Execução dos Scripts
🧪 1. Great Expectations
python validate_data_greatexpectations.py


Cria e executa expectativas de dados.

Gera relatórios HTML em great_expectations/uncommitted/validations/.

☁️ 2. Soda Core + Snowflake
python validate_data_soda_snowflake.py


Executa validações definidas no arquivo checks.yml.

Conecta ao Snowflake via warehouse.yaml.

Envia resultados para o Soda Cloud (opcional).

🧰 3. Soda Core Local
python validate_data_soda_local.py


Executa validações localmente sobre o CSV.

Salva resultados em dq_reports/soda_report.json.

⚡ 4. Deequ (Spark)
python validate_data_deequ.py


Executa validações em escala usando Spark + PyDeequ.

Gera relatório JSON em dq_reports/deequ_report.json.

📊 Exemplo de Saída (Deequ)
✅ Dataset carregado: 50000 linhas, 18 colunas

Resumo das validações:
+-----------------------------+----------------------+-----------+
|check                        |constraint            |status     |
+-----------------------------+----------------------+-----------+
|Data Quality Validation      |isComplete(id)        |Success    |
|Data Quality Validation      |isUnique(id)          |Success    |
|Data Quality Validation      |hasPattern(email, ...) |Failure    |
+-----------------------------+----------------------+-----------+
📊 Relatório salvo em: dq_reports/deequ_report.json

🧠 Integração com Pipelines

Esses scripts podem ser integrados a:

Airflow DAGs (tarefa data_quality_check)

ETL Batch com Spark

Pipelines CI/CD de dados

Databricks / AWS Glue / GCP Dataflow

📬 Alertas e Monitoramento (Opcional)

Você pode adicionar notificações automáticas em caso de falha:

Slack → via webhook (para Great Expectations / Soda Core)

E-mail → via SMTP

Dashboard → Grafana / Power BI