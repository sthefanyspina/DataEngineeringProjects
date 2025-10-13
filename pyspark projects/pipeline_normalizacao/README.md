# 🧹 Pipeline de Normalização de Dados com PySpark

Projeto completo de **limpeza, padronização e validação de dados** usando **Apache Spark (PySpark)**.  
O pipeline lê um dataset bruto de clientes, normaliza campos (nome, CPF, data e telefone), valida formatos e separa registros **válidos** e **rejeitados** em arquivos otimizados.

---

## ⚙️ **Funcionalidades**

✅ Padronização de campos:
- **Nome:** remoção de espaços e formatação em maiúsculas  
- **CPF:** limpeza de caracteres e validação por dígito verificador  
- **Data de Nascimento:** conversão para formato `yyyy-MM-dd`  
- **Telefone:** limpeza de símbolos e validação de tamanho  

✅ Validação avançada:
- CPF real (com dígito verificador)
- Telefone com 10 ou 11 dígitos
- Nomes não vazios
- Datas válidas

✅ Separação automática:
- **dados_limpios.parquet** → registros validados e limpos  
- **dados_rejeitados.csv** → registros com erros + motivo(s) de rejeição  

✅ Registro de logs:
- Logs detalhados em `logs/pipeline_normalizacao_avancado.log`

---

## 🧱 **Estrutura do Projeto**

pipeline_normalizacao/
│
├── data/
│ ├── raw/
│ │ └── clientes_com_invalidos.csv # dataset bruto (205.000 linhas)
│ ├── processed/
│ │ ├── dados_limpios.parquet # dados limpos
│ │ └── dados_rejeitados.csv # registros rejeitados
│
├── logs/
│ └── pipeline_normalizacao_avancado.log
│
├── main_normalizacao_avancado.py
├── README.md

yaml
Copy code

---

## 🧠 **Exemplo de Entrada**

`clientes_com_invalidos.csv`
```csv
nome,cpf,data_nascimento,telefone
João da Silva,111.222.333-44,01/05/1990,(11) 99999-8888
Maria Souza,22233344455,10/12/1985,21 88888-7777
,33344455566,15/03/1978,9999
📊 Exemplo de Saída
🟢 dados_limpios.parquet
nome	cpf	data_nascimento	telefone
JOÃO DA SILVA	11122233344	1990-05-01	11999998888
MARIA SOUZA	22233344455	1985-12-10	21888887777

🔴 dados_rejeitados.csv
nome	cpf	data_nascimento	telefone	motivo_rejeicao
33344455566	15/03/1978	9999	Nome vazio, Telefone inválido

🚀 Como Executar o Pipeline
1️⃣ Criar ambiente
bash
Copy code
python -m venv venv
source venv/bin/activate   # ou venv\Scripts\activate no Windows
2️⃣ Instalar dependências
bash
Copy code
pip install pyspark faker pandas
3️⃣ Rodar o script
bash
Copy code
python main_normalizacao_avancado.py
🧩 Dataset
Um dataset sintético com 205.000 registros foi gerado com a biblioteca Faker para testes:

📄 clientes_com_invalidos.csv

200.000 registros válidos

5.000 registros inválidos para testar rejeições

🧰 Tecnologias Utilizadas
Tecnologia	Função
Apache Spark (PySpark)	Processamento distribuído
Python 3.x	Linguagem principal
Pandas + Faker	Geração de dataset de teste
Parquet/CSV	Armazenamento otimizado
Logging	Registro de execução e erros

📈 Resultados Esperados
Após a execução:

O diretório data/processed/ conterá os arquivos limpos e rejeitados.

O log completo estará em logs/pipeline_normalizacao_avancado.log.

🧑‍💻 Autor
Sthefany Spina
💡 Projeto educativo e demonstrativo de Data Engineering com PySpark
📬 Sinta-se livre para clonar, melhorar e compartilhar!