# 🚀 Projeto PySpark - Leitura, Transformação e EDA de CSV

Projeto completo utilizando **PySpark** para leitura, limpeza, transformação e análise exploratória de grandes volumes de dados em CSV.  
Ideal para portfólio de **Engenharia e Análise de Dados**, com integração de múltiplas fontes e visualizações gráficas.

---

## 🧩 Objetivos do Projeto

- Ler **CSVs grandes** com o Spark (`spark.read.csv()`).
- Limpar e **normalizar colunas** (nomes, datas, valores).
- Salvar resultados em **formato Parquet** (otimizado).
- Fazer **Análise Exploratória (EDA)** com PySpark e matplotlib.
- **Converter formatos** (CSV → Parquet → JSON).
- Medir diferença de desempenho entre formatos.
- Fazer **joins e normalização** entre datasets relacionados.

---

## 🏗️ Estrutura do Pipeline

Etapas executadas no script `src/pipeline_pyspark.py`:

| Etapa | Descrição |
|-------|------------|
| 1️⃣ | Leitura de datasets CSV grandes |
| 2️⃣ | Limpeza e padronização de colunas |
| 3️⃣ | Conversão de tipos e remoção de duplicatas |
| 4️⃣ | Salvamento em Parquet |
| 5️⃣ | EDA: médias, contagens, distribuições |
| 6️⃣ | Visualizações (Top produtos, Distribuição, Evolução temporal) |
| 7️⃣ | Conversão entre formatos (CSV, Parquet, JSON) |
| 8️⃣ | Junção entre clientes, produtos e vendas |
| 9️⃣ | Salvamento do dataset final tratado |

---

## 📊 Exemplos de Visualizações

### 🔹 Top 10 Produtos Mais Vendidos
![Top 10 Produtos](output/top10_produtos.png)

### 🔹 Distribuição de Valores de Venda
![Distribuição](output/distribuicao_valor_venda.png)

### 🔹 Evolução de Vendas no Tempo
![Evolução de Vendas](output/evolucao_vendas.png)

---

## ⚙️ Tecnologias Utilizadas

- **Python 3.10+**
- **Apache Spark (PySpark)**
- **Pandas & Matplotlib**
- **Faker** (para gerar dados falsos)
- **Logging** (para registrar execução)
- **Parquet / JSON** (armazenamento eficiente)

---

## 📦 Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seuusuario/projeto-pyspark-eda.git
   cd projeto-pyspark-eda

Crie e ative um ambiente virtual:

python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

Instale as dependências:

pip install -r requirements.txt

Gere os datasets:

python src/gerar_datasets.py

Execute o pipeline PySpark:

python src/pipeline_pyspark.py