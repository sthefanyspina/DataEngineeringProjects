# 🧩 Data Pipeline + Dashboard — Normalização de Dataset

Projeto completo de engenharia de dados e BI, que demonstra a criação de um **pipeline de dados automatizado**, capaz de:

- Coletar e consolidar dados (de CSV/API)
- Tratar e **normalizar informações inconsistentes**
- Gerar um **dataset limpo e padronizado**
- Persistir os dados em um **banco relacional (PostgreSQL)**
- Conectar o banco a uma ferramenta de **Business Intelligence (Power BI / Metabase / Looker Studio)**
- Criar um **dashboard interativo** de acompanhamento

---

## 📁 Estrutura do Projeto

📦 pipeline-dados
├── data/
│ ├── raw/
│ │ └── dados_clientes_raw.csv # Dataset sujo (original)
│ └── processed/
│ └── dados_clientes_limpo.csv # Dataset limpo e normalizado
│
├── pipeline.py # Script principal do pipeline
├── gerar_dataset_sujo.py # Script para gerar o dataset com erros
├── requirements.txt # Dependências do projeto
├── README.md # Este arquivo
└── dashboard/ # Pasta opcional para prints ou arquivos do dashboard

yaml
Copiar código

---

## ⚙️ Etapas do Projeto

### 1️⃣ Gerar o Dataset “Sujo”

O script `gerar_dataset_sujo.py` cria um dataset com **mais de 200.000 linhas**, contendo:

- **id** → com duplicatas e erros de tipo  
- **nome** → com capitalização inconsistente e espaçamentos extras  
- **data_nascimento** → com múltiplos formatos de data e valores inválidos  
- **valor_compra** → com números, textos incorretos e valores ausentes  

**Execute:**

```bash
python gerar_dataset_sujo.py
O arquivo gerado será salvo em:

bash
Copiar código
data/raw/dados_clientes_raw.csv
2️⃣ Executar o Pipeline de Limpeza
O script pipeline.py realiza as seguintes etapas:

Leitura dos dados brutos (raw)

Padronização e normalização (nomes, datas, tipos numéricos)

Remoção de duplicatas e correção de erros

Geração de um CSV “limpo”

Gravação dos dados em um banco PostgreSQL

Execute:

bash
Copiar código
python pipeline.py
O dataset limpo será salvo em:

bash
Copiar código
data/processed/dados_clientes_limpo.csv
3️⃣ Banco de Dados (PostgreSQL)
Crie o banco e configure o acesso:

sql
Copiar código
CREATE DATABASE meubanco;
CREATE USER usuario WITH PASSWORD 'senha';
GRANT ALL PRIVILEGES ON DATABASE meubanco TO usuario;
Edite no pipeline.py:

python
Copiar código
engine = create_engine("postgresql+psycopg2://usuario:senha@localhost:5432/meubanco")
A tabela clientes será criada automaticamente após executar o pipeline.

4️⃣ Conexão com o BI
Você pode conectar o banco de dados a ferramentas como:

🟡 Power BI
Vá em Obter Dados → Banco de Dados PostgreSQL

Insira as credenciais do seu banco

Selecione a tabela clientes

🔵 Metabase
Adicione nova conexão → PostgreSQL

Configure servidor, banco, usuário e senha

Crie dashboards com filtros e visualizações

🔴 Looker Studio
Conecte via conector PostgreSQL

Autentique com suas credenciais

Crie gráficos e painéis interativos

📊 Métricas e Visualizações Sugeridas
Número total de clientes

Valor total de compras

Média de compras por cliente

Distribuição de clientes por idade

Clientes com valores de compra acima da média

🧠 Tecnologias Utilizadas
Categoria	Tecnologias
Linguagem	Python 3.9+
Bibliotecas	Pandas, NumPy, Faker, SQLAlchemy, psycopg2
Banco de Dados	PostgreSQL
Visualização	Power BI / Metabase / Looker Studio
Sistema Operacional	Windows / Linux / macOS