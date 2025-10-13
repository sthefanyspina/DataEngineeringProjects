# 🚀 PySpark Streaming com Kafka

Este projeto demonstra um pipeline **de streaming em tempo real** usando **PySpark Structured Streaming + Kafka**.

## 📂 Estrutura
pyspark_streaming_kafka/
│
├── app/
│ ├── producer.py # Gera eventos e envia para o Kafka
│ └── consumer_stream.py # Lê, processa e exibe resultados com PySpark
│
├── requirements.txt
└── README.md

markdown
Copy code

## ⚙️ Requisitos
- Python 3.8+
- Apache Kafka (rodando localmente)
- PySpark 3.5+
- kafka-python

Instale as dependências:
```bash
pip install -r requirements.txt
▶️ Como Rodar
Suba o Kafka local:

bash
Copy code
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
kafka-topics.sh --create --topic pedidos_topic --bootstrap-server localhost:9092
Rode o Producer:

bash
Copy code
python app/producer.py
Rode o Streaming:

bash
Copy code
spark-submit app/consumer_stream.py
📊 Resultado
O PySpark exibirá, em tempo real, o total de pedidos por status no console.

lua
Copy code
+----------+---------------+
| status   | total_pedidos |
+----------+---------------+
| pago     | 15            |
| pendente | 7             |
| cancelado| 3             |
+----------+---------------+
🧩 Extensões possíveis
Escrever a saída em PostgreSQL ou Delta Lake

Criar janelas de tempo (window) para agregações por minuto

Enviar alertas em tempo real (ex: volume alto de pedidos cancelados)

📍 Autor: Sthefany Spina
💡 Tecnologias: PySpark, Kafka, Structured Streaming