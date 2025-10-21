# 🧹 Pipeline de Normalização de Dataset (Data Cleaning Project)

Este projeto implementa um **pipeline automatizado de limpeza e normalização de dados** em Python.  
Ele foi criado para processar datasets desorganizados — com colunas inconsistentes, valores ausentes, tipos errados e duplicatas — e gerar uma versão limpa, pronta para análise ou carga em banco de dados.

---

## 🚀 Objetivo

O objetivo deste pipeline é:
- Corrigir **nomes de colunas** e **formatações inconsistentes**  
- Padronizar **datas, textos e valores numéricos**  
- Remover **duplicatas e registros inválidos**  
- Gerar logs de erros e auditoria  
- Salvar o resultado limpo em **CSV** e também em um **banco de dados**

---

## 🧠 Tecnologias Utilizadas

- **Python 3.9+**
- **Pandas** → manipulação e limpeza de dados  
- **NumPy** → tratamento de valores nulos e tipos  
- **SQLite** (padrão, mas compatível com PostgreSQL/MySQL)  
- **Regex** → correção e padronização de nomes de colunas  

---

## 📁 Estrutura do Projeto
📦 data-pipeline-normalizacao
├── dados_bagunçados.csv # Dataset original com 500k+ linhas e erros
├── dados_limpos.csv # Dataset limpo gerado pelo pipeline
├── dados_normalizados.db # Banco SQLite com os dados limpos
├── log_erros.csv # Log com erros detectados e valores problemáticos
├── gerar_dataset.py # Script para gerar dataset sintético (bagunçado)
├── pipeline_normalizacao.py # Pipeline principal de limpeza e normalização
└── README.md # Este arquivo

## 🧩 Funcionalidades Principais

| Funcionalidade | Descrição |
|----------------|------------|
| 🧾 Correção de nomes de colunas | Remove espaços e padroniza nomes para o schema final |
| 🧍 Normalização de texto | Remove espaços extras e padroniza capitalização |
| 📅 Normalização de datas | Converte múltiplos formatos para `YYYY-MM-DD`, logando inválidas |
| 💰 Normalização de números | Corrige separadores decimais, converte strings para `float` |
| 🧹 Limpeza de duplicatas/nulos | Remove registros redundantes ou completamente vazios |
| 🪵 Log de erros detalhado | Cria arquivo `log_erros.csv` com linha, coluna, valor e tipo de erro |
| 🗄️ Integração com banco | Carrega dados limpos em banco SQLite (ou PostgreSQL/MySQL) |

---

## 🧰 Como Executar

### 1️⃣ Criar o dataset “bagunçado”
Execute o script de geração:

```bash
python gerar_dataset.py
Isso criará um arquivo dados_bagunçados.csv com mais de 500.000 linhas e diversos erros intencionais.

2️⃣ Rodar o pipeline de normalização
bash
python pipeline_normalizacao.py

O script:
Lerá dados_bagunçados.csv
Corrigirá inconsistências
Gerará dados_limpos.csv
Criará o log log_erros.csv
E salvará os dados no banco dados_normalizados.db

🧱 Integração com Banco de Dados
Por padrão, o pipeline usa SQLite, mas você pode facilmente trocar por outro banco:

Exemplo para PostgreSQL
python
Copy code
from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://usuario:senha@host:porta/banco")
df.to_sql("clientes", engine, if_exists="replace", index=False)

Exemplo para MySQL
python
Copy code
engine = create_engine("mysql+pymysql://usuario:senha@host:porta/banco")

🧾 Log de Erros
O arquivo log_erros.csv contém todas as linhas que apresentaram falhas de conversão:

linha	coluna	valor	erro
1234	Data_nasc	31/02/2020	Data inválida
9876	Salário	mil	Número inválido

Esses registros podem ser revisados manualmente ou descartados conforme a política de qualidade dos dados.

⚙️ Personalizações
Você pode ajustar:
Mapeamento de colunas → no dicionário dentro da função corrigir_nomes_colunas
Tratamentos adicionais → ex: padronização de UF, CEP, gênero etc.
Banco de destino → trocando a string de conexão na função principal

👩‍💻 Autor
Sthefany Spina
Projeto educativo de Engenharia de Dados • Pipeline de Normalização de Datasets
