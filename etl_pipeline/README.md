# 🧬 ETL Pipeline

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** em Python para coletar, processar e armazenar dados de casos de COVID-19 por país, utilizando a API pública [disease.sh](https://disease.sh/).

Os dados são extraídos em formato JSON, transformados em um `DataFrame` do **pandas**, tratados e carregados em um banco de dados **MySQL**.

---

## 🧠 Descrição do Projeto

A pipeline realiza as seguintes etapas:

1. **Extração (Extract):**  
   Faz uma requisição HTTP para o endpoint público da API de COVID-19 e obtém os dados em formato JSON.

2. **Transformação (Transform):**  
   - Seleciona colunas relevantes (`country`, `cases`, `deaths`, `recovered`, `population`)  
   - Remove duplicatas  
   - Calcula a métrica `cases_per_million`  
   - Garante compatibilidade de caracteres UTF-8  

3. **Carregamento (Load):**  
   Insere os dados limpos e transformados em uma tabela do banco de dados **MySQL**.

---

## 🧰 Tecnologias Utilizadas

| Biblioteca / Ferramenta | Descrição |
|--------------------------|-----------|
| **Python 3.9+** | Linguagem principal |
| **requests** | Requisições HTTP para a API |
| **pandas** | Manipulação e transformação de dados |
| **numpy** | Cálculos numéricos |
| **SQLAlchemy** | Integração com o banco de dados |
| **mysql-connector-python** | Driver MySQL para SQLAlchemy |
| **logging** | Registro de logs da execução |

---

## 🏗️ Arquitetura da Pipeline

    +----------------+
    |   API COVID    |
    | disease.sh     |
    +--------+-------+
             |
             v
      [ Extração de Dados ]
             |
             v
      [ Transformação ]
      Limpeza de dados
      Cálculo de métricas
             |
             v
      [ Carregamento ] 
         Banco MySQL
             |
             v
    +----------------+
    | Tabela: covid_data |
    +----------------+

---

## Requirements
- requests
- pandas
- numpy
- SQLAlchemy
- mysql-connector-python

---

## 🗄️Configuração do Banco de Dados
A conexão com o MySQL é feita através da string definida no código:
conn_string = "mysql+mysqlconnector://root:senha@localhost/covid_database"


Substitua:
root → seu usuário do MySQL
senha → sua senha
covid_database → nome do seu banco de dados

A pipeline cria (ou substitui) automaticamente a tabela covid_data.

---

## 🧾 Logs
Durante a execução, os logs são gerados em:

Arquivo: etl_pipeline.log

Console: saída padrão (terminal)

Cada execução registra:
- Início e término das etapas (Extração, Transformação, Carregamento)
- Número de linhas processadas
- Erros ou falhas detalhadas

