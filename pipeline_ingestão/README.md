# 🚀 Pipeline de Ingestão de Arquivos CSV/JSON em Lote (MySQL)

Este projeto implementa um **pipeline de ingestão batch** (em lote) para arquivos **CSV** e **JSON**, com validação de dados, geração de logs e tratamento de registros rejeitados.  
O objetivo é automatizar o carregamento de dados em um banco **MySQL**, garantindo integridade e rastreabilidade.

## 🧠 Visão Geral

### 🧱 Arquitetura
┌──────────────┐
│ Pasta Input │ ← CSV / JSON
└──────┬───────┘
│
▼
┌──────────────┐
│ Pipeline ETL │ ← Python + Pandas + SQLAlchemy
│ - Validação │
│ - Log Erros │
│ - Rejeições │
└──────┬───────┘
│
▼
┌──────────────┐
│ Banco MySQL │
└──────────────┘

## ⚙️ Funcionalidades

✅ Leitura automática de arquivos **CSV e JSON**  
✅ Validação de estrutura e tipos de dados  
✅ Registro detalhado de logs (`/logs`)  
✅ Salvamento de **registros rejeitados** (`/rejected`)  
✅ Inserção dos dados válidos no **MySQL**  
✅ Modular, extensível e fácil de automatizar  


## 📂 Estrutura do Projeto

ingest_pipeline/
│
├── input/ # Arquivos CSV/JSON a serem processados
├── rejected/ # Registros rejeitados por erro de validação
├── logs/ # Logs de execução
└── ingest_pipeline.py # Script principal

## 🧰 Tecnologias Utilizadas

| Categoria | Ferramenta |
|------------|-------------|
| Linguagem | Python 3.9+ |
| Banco de Dados | MySQL |
| Bibliotecas Principais | pandas, sqlalchemy, pymysql |
| Logging | Módulo `logging` padrão do Python |


## 🔧 Configuração do Ambiente

### 1. Clonar o Repositório

git clone https://github.com/seuusuario/pipeline-ingestao-mysql.git
cd pipeline-ingestao-mysql

2. Criar o Banco de Dados MySQL
Acesse seu MySQL e crie o banco:

CREATE DATABASE ingest_db;
Opcional: criar tabela manualmente (ou será criada automaticamente):

CREATE TABLE dados_ingestao (
    id INT,
    nome VARCHAR(100),
    valor FLOAT,
    data DATE
);

3. Instalar Dependências
pip install pandas sqlalchemy pymysql

4. Configurar a Conexão no Script
No arquivo ingest_pipeline.py, edite a linha:

DB_URL = "mysql+pymysql://root:senha123@localhost:3306/ingest_db"
Substitua root e senha123 pelas suas credenciais do MySQL.

▶️ Execução do Pipeline
Coloque seus arquivos CSV ou JSON na pasta input/
Exemplo:

id,nome,valor,data
1,Ana,100.5,2025-10-21
2,Pedro,200.0,2025-10-22

Execute o script:
python ingest_pipeline.py

Verifique:
Logs: logs/ingest_YYYYMMDD_HHMMSS.log

Rejeitados: rejected/

Banco MySQL:
SELECT * FROM dados_ingestao;

🧩 Validações Realizadas
Validação	Descrição
Colunas obrigatórias	Garante presença de id, nome, valor, data
Tipagem	Converte valor → float e data → datetime
Nulos	Bloqueia arquivos com valores nulos
Erros de leitura	Arquivos com formato incorreto são logados e ignorados

🪵 Logs e Registros Rejeitados
Todos os logs são gravados em /logs com timestamp.

Registros inválidos são exportados para a pasta /rejected com nome:
rejected_nomearquivo_YYYYMMDDHHMMSS.csv