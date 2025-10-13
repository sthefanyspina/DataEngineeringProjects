Projeto PySpark - WordCount

Contagem de palavras em arquivos de texto usando PySpark.
Inclui abordagens com RDDs, DataFrames e Streaming.
Projeto didático para aprendizado de processamento distribuído com Spark.

📂 Estrutura do Projeto
wordcount_pyspark/
│
├── texto.txt                # Arquivo de exemplo
├── wordcount_rdd.py         # Versão RDD
├── wordcount_dataframe.py   # Versão DataFrame
├── wordcount_streaming.py   # Versão Streaming
├── README.md                # Este arquivo
├── saida/                   # Resultado da versão RDD
└── saida_df/                # Resultado da versão DataFrame

⚙️ Pré-requisitos

Python 3.7+

PySpark instalado:

pip install pyspark


Para streaming: criar as pastas stream_input e stream_output.

1️⃣ WordCount com RDDs

Arquivo: wordcount_rdd.py

Descrição:

Lê arquivo de texto

Remove pontuação e normaliza palavras

Conta a frequência de cada palavra usando RDDs

Salva resultado em arquivo e imprime no terminal

Execução:

python wordcount_rdd.py


Exemplo de saída:

python: 3
spark: 2
aprendizado: 1
dados: 4


Saída: salva na pasta saida/contagem_palavras.

2️⃣ WordCount com DataFrames

Arquivo: wordcount_dataframe.py

Descrição:

Lê arquivo como DataFrame

Limpa e separa palavras em linhas individuais

Agrupa por palavra e conta ocorrências

Ordena do mais frequente para o menos frequente

Salva resultado em CSV e mostra no terminal

Execução:

python wordcount_dataframe.py


Exemplo de saída no terminal:

palavra	count
dados	4
python	3
spark	2
aprendizado	1

Saída: salva na pasta saida_df/contagem_palavras em CSV.

3️⃣ WordCount em Streaming

Arquivo: wordcount_streaming.py

Descrição:

Monitora continuamente a pasta stream_input

Processa arquivos de texto adicionados em tempo real

Limpa e separa palavras, conta frequências

Mostra resultados no console continuamente

Execução:

python wordcount_streaming.py


Como testar:

Enquanto o script estiver rodando, adicione arquivos .txt na pasta stream_input.

A contagem de palavras será atualizada automaticamente no console.

🔄 Estrutura de processamento
Abordagem	Fluxo de processamento
RDD	textFile → flatMap → map → reduceByKey → collect/save
DataFrame	read.text → regexp_replace → split → explode → groupBy → count → write
Streaming	readStream.text → regexp_replace → split → explode → groupBy → count → writeStream
💡 Dicas

Evite collect() em arquivos grandes; use write para salvar resultados.

Use expressões regulares para limpar pontuação e caracteres especiais.

Combine DataFrame + Streaming para pipelines mais eficientes.

Pode salvar saída de streaming em CSV, Parquet ou enviar para Kafka.

📈 Exemplo Visual de Pipeline (DataFrame/Streaming)
Arquivo de Texto
      │
      ▼
  Spark DataFrame
      │
  Limpeza e Normalização
      │
   Split/Explode
      │
   Agrupamento por Palavra
      │
   Contagem de Frequência
      │
   Saída (Console / Arquivo / Kafka)
