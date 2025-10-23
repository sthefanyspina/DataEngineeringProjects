📦 Data Warehouse Básico de Vendas
🧠 Descrição do Projeto

Este projeto implementa um Data Warehouse (DW) básico utilizando o modelo estrela (Star Schema) para simular um cenário de vendas comerciais.
O objetivo é demonstrar, de forma prática, como projetar, popular e consultar um DW com dados sintéticos — ideal para portfólio de Engenharia de Dados ou prática de SQL analítico.

O DW contém:
Fato de Vendas (fact_vendas)
Dimensões de Cliente, Produto e Tempo

🧱 Modelagem Dimensional
🎯 Esquema Estrela
                 DIM_CLIENTE
                 +-----------+
                 | id_cliente|
                 | nome      |
                 | cidade    |
                 | estado    |
                 | segmento  |
                 +-----------+
                       |
DIM_PRODUTO      FACT_VENDAS        DIM_TEMPO
+-----------+   +----------------+  +------------+
| id_produto|---| id_produto     |--| id_tempo   |
| nome      |   | id_cliente     |  | data       |
| categoria |   | id_tempo       |  | ano        |
| preço     |   | quantidade     |  | mes        |
+-----------+   | valor_total    |  | dia        |
                +----------------+  +------------+


Tabela Fato (fact_vendas) → Armazena transações de vendas, com medidas numéricas.
Tabelas Dimensão → Contêm atributos descritivos para análise (cliente, produto e tempo).

⚙️ Tecnologias Utilizadas
Tipo	Ferramenta
Linguagem	Python 3
Banco de Dados	PostgreSQL / MySQL
ORM / Conexão	SQLAlchemy
Geração de Dados	Faker
Manipulação de Dados	Pandas
Bibliotecas auxiliares	PyMySQL, psycopg2

🚀 Execução do Projeto
1️⃣ Clone o repositório
git clone https://github.com/<seu-usuario>/dw-vendas.git
cd dw-vendas

2️⃣ Instale as dependências
pip install pandas faker sqlalchemy psycopg2-binary pymysql

3️⃣ Configure a conexão com o banco

Edite a variável DATABASE_URL no início do arquivo dw_vendas.py:

DATABASE_URL = "postgresql://usuario:senha@localhost:5432/data_warehouse"


Para MySQL:

DATABASE_URL = "mysql+pymysql://usuario:senha@localhost:3306/data_warehouse"

4️⃣ Execute o script
python dw_vendas.py


📊 O script irá:
Criar as tabelas dimensão e fato
Gerar dados sintéticos realistas (clientes, produtos, tempo, vendas)
Popular o Data Warehouse
Executar e exibir automaticamente 10 consultas analíticas

📊 Consultas de Negócio
Nº	Nome da Consulta	Objetivo
1️⃣	Total de Vendas por Cliente	Identifica os melhores clientes em receita
2️⃣	Total de Vendas por Produto	Mostra produtos mais vendidos
3️⃣	Receita por Mês	Avalia sazonalidade e crescimento
4️⃣	Ticket Médio por Cliente	Mede o gasto médio por compra
5️⃣	Receita por Categoria	Mostra categorias mais lucrativas
6️⃣	Total de Vendas por Estado	Analisa performance geográfica
7️⃣	Vendas Diárias e Crescimento %	Calcula evolução percentual diária
8️⃣	Produtos por Segmento	Preferência de compra por segmento
9️⃣	Top 5 Clientes	Identifica maiores compradores
🔟	Médias Gerais	Calcula médias de vendas e quantidades

🧩 Otimizações Implementadas
Uso de CTEs (Common Table Expressions) para legibilidade e performance.
Agregações e joins otimizados (somas antes de joins).

Índices sugeridos para escalabilidade:
CREATE INDEX idx_fact_cliente ON fact_vendas (id_cliente);
CREATE INDEX idx_fact_produto ON fact_vendas (id_produto);
CREATE INDEX idx_fact_tempo ON fact_vendas (id_tempo);
CREATE INDEX idx_dim_tempo_data ON dim_tempo (data);
CREATE INDEX idx_dim_cliente_estado ON dim_cliente (estado);

📈 Exemplo de Saída no Terminal
✅ dim_cliente criada com 50 registros.
✅ dim_produto criada com 7 registros.
✅ dim_tempo criada com 273 registros.
✅ fact_vendas criada com 1000 registros.

--- 1️⃣ Total de Vendas por Cliente ---
          nome         total_vendas
0   João da Silva         17400.00
1   Maria Souza           12200.00
...

✅ Data Warehouse populado e análises executadas com sucesso!

📚 Estrutura do Projeto
📂 dw-vendas/
├── dw_vendas.py          # Script principal (ETL + queries)
├── README.md             # Documentação
└── requirements.txt      # Dependências (opcional)
