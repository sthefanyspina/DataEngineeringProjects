💳 Detecção de Fraudes com PySpark & MLlib








🚀 Projeto de Detecção de Fraudes em larga escala utilizando PySpark + MLlib, com pipeline completo de ingestão, feature engineering, modelagem e avaliação.

🧠 Visão Geral

Fraudes financeiras representam bilhões de dólares em prejuízo por ano.
Este projeto demonstra como construir um sistema escalável de detecção de fraudes com PySpark, capaz de processar milhões de transações com alta eficiência.

📌 Objetivos principais:

Criar e processar um dataset de transações simuladas.

Gerar features relevantes para detecção de fraudes.

Treinar modelos de Machine Learning distribuído com MLlib.

Avaliar desempenho usando AUC ROC e selecionar o melhor modelo.

🧱 Arquitetura do Projeto
fraud_detection_pyspark/
│
├── data/
│   └── transactions.csv         # Dataset sintético (200.000 linhas)
│
├── logs/
│   └── fraud_detection.log      # Logs de execução
│
├── models/                      # Modelos salvos (.model)
│
├── generate_dataset.py          # Geração do dataset de exemplo
├── pipeline_basic.py            # Pipeline simples
├── pipeline_advanced.py         # Pipeline com tuning
├── pipeline_balanced.py         # Pipeline com oversampling
├── pipeline_professional.py     # Pipeline completo com CV
└── README.md

📊 Dataset Sintético

O dataset contém 200.000 transações financeiras com 2% de fraudes.

Coluna	Tipo	Descrição
transaction_id	int	ID único da transação
amount	float	Valor da transação
time	string	Timestamp da transação
merchant_type	string	Tipo de comerciante
is_fraud	int	1 = fraude / 0 = legítima

📁 Gerado automaticamente pelo script:

python generate_dataset.py

⚙️ Execução
🔹 Instalação de dependências
pip install pyspark

🔹 Geração do dataset
python generate_dataset.py

🔹 Rodar o pipeline profissional
spark-submit pipeline_professional.py

🤖 Modelos Treinados
Modelo	Tipo	Métrica Avaliada	AUC ROC	Observações
Logistic Regression	Linear	Classificação Binária	0.93	Melhor equilíbrio entre precisão e recall
Decision Tree	Árvores	Classificação	0.88	Boa interpretabilidade
Random Forest	Ensemble	Classificação	0.95	Excelente performance, porém mais pesado
GBTClassifier	Ensemble Boosted	Classificação	0.97	Melhor desempenho geral

📈 Melhor modelo: GBTClassifier (Gradient Boosted Trees)

🧩 Componentes do Pipeline

Feature Engineering

Indexação e vetorização de variáveis categóricas (merchant_type)

Normalização de valores (amount)

Criação de vetor de features (VectorAssembler)

Treinamento de Modelo

Logistic Regression, Decision Tree, Random Forest, GBT

Avaliação

BinaryClassificationEvaluator(metricName="areaUnderROC")

Persistência

Salvamento do modelo no diretório /models

Leitura para produção com PipelineModel.load()

📈 Exemplo de Saída no Log
[INFO] Iniciando pipeline de detecção de fraudes...
[INFO] Dados carregados: 200000 registros.
[INFO] Treinando modelo: Logistic Regression
[INFO] AUC = 0.932
[INFO] Melhor modelo: GBTClassifier com AUC = 0.971
[INFO] Modelo salvo em: models/fraud_detection_GBTClassifier_pro

🧰 Tecnologias
Tecnologia	Uso
PySpark	Processamento distribuído e ML
MLlib	Treinamento e avaliação de modelos
Spark SQL	Manipulação de dados
Python	Scripts e orquestração
CSV / Parquet	Armazenamento de dados
🔬 Próximos Passos

 Adicionar detecção de anomalias em tempo real (Streaming com Kafka)

 Implementar explainability (SHAP/LIME)

 Deploy do modelo com MLflow

 Dashboard com Grafana/Streamlit

🧑‍💻 Autor

Sthefany Spina
💼 Projetos em Big Data, Spark e Machine Learning
📧 Contato: sthefanyspina.dev@gmail.com

🪪 Licença

Este projeto está sob a licença MIT.
Sinta-se livre para usar, modificar e compartilhar!