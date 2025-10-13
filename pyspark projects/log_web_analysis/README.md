# 🧠 Análise Avançada de Logs Web com PySpark

Projeto de **Data Engineering** utilizando **PySpark** para processar e analisar **logs de servidores web (Apache/Nginx)**.  
O objetivo é extrair informações úteis, como IPs, URLs acessadas, horários e códigos de status, gerando métricas de uso e erros.

---

## 📋 Objetivos

- Ler arquivos de log de acesso (`access.log`)
- Extrair campos relevantes:
  - IP de origem  
  - Data e hora do acesso  
  - Método HTTP (`GET`, `POST`, etc.)  
  - URL acessada  
  - Código de status (`200`, `404`, `500`...)  
  - Tamanho da resposta  
- Calcular estatísticas:
  - 📊 Acessos por hora  
  - 🌐 Páginas mais acessadas  
  - 👥 IPs mais ativos  
  - ❌ Erros HTTP (404, 500 etc.)  
  - 📈 Taxa de erro (%)

---

## ⚙️ Tecnologias Utilizadas

| Tecnologia | Descrição |
|-------------|------------|
| **Python 3.9+** | Linguagem principal |
| **Apache Spark (PySpark)** | Engine de processamento distribuído |
| **Parquet** | Formato de saída otimizado |
| **Regex (expressões regulares)** | Extração de dados do log |
| **Logging** | Registro de execução e erros |

---

## 📂 Estrutura de Pastas

pyspark-analise-logs-web/
│
├── data/
│ ├── logs/
│ │ ├── access_log_2025-10-10.log
│ │ ├── access_log_2025-10-11.log
│ └── output/
│ ├── acessos_por_hora/
│ ├── paginas_populares/
│ ├── ips_ativos/
│ ├── erros_http/
│
├── logs/
│ └── analise_logs_web_avancada.log
│
├── analise_logs_web_avancada.py
└── README.md

yaml
Copy code

---

## 🧩 Exemplo de Linha de Log (Apache/Nginx)

192.168.1.10 - - [12/Oct/2025:10:45:22 -0300] "GET /produtos HTTP/1.1" 200 5321

yaml
Copy code

---

## 🚀 Execução do Projeto

### 1️⃣ Criar o ambiente
Crie e ative um ambiente virtual Python:
```bash
python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows
2️⃣ Instalar dependências
bash
Copy code
pip install pyspark
3️⃣ Estrutura de dados
Coloque seus arquivos de log em:

bash
Copy code
/data/logs/
4️⃣ Executar o script
bash
Copy code
spark-submit analise_logs_web_avancada.py
📊 Saídas Geradas
Os resultados são salvos em formato Parquet na pasta /data/output/:

Métrica	Caminho	Descrição
Acessos por Hora	/data/output/acessos_por_hora/	Total de acessos por hora
Páginas Populares	/data/output/paginas_populares/	URLs mais acessadas
IPs Ativos	/data/output/ips_ativos/	IPs que mais acessaram o site
Erros HTTP	/data/output/erros_http/	Contagem de códigos 4xx e 5xx

📈 Exemplo de Saída
Acessos por Hora
hora	count
0	154
1	201
2	189

Páginas mais Acessadas
url	count
/home	1289
/produtos	912
/carrinho	405

Erros HTTP
status	count
404	38
500	5

Taxa de erro: 3.1%

🛠️ Como o Código Funciona
Lê os logs em formato texto (spark.read.text())

Extrai campos com expressões regulares (regexp_extract)

Converte o timestamp (to_timestamp)

Calcula métricas com groupBy e count

Salva resultados em formato Parquet

Gera logs de execução (/logs/analise_logs_web_avancada.log)

📦 Próximos Passos
 Adicionar Delta Lake para histórico incremental

 Criar pipeline no Airflow para execução diária

 Enviar alertas em caso de alta taxa de erro

 Visualizar métricas em Power BI / Grafana

🧑‍💻 Autor
Sthefany Spina
💬 Projeto educacional com foco em Engenharia de Dados e PySpark.