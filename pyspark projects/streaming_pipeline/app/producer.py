import json
import time
import random
from kafka import KafkaProducer

# Configuração do Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

statuses = ["pago", "pendente", "cancelado"]

print("🚀 Enviando pedidos para o Kafka...")
pedido_id = 1

while True:
    evento = {
        "pedido_id": pedido_id,
        "valor": round(random.uniform(50, 500), 2),
        "status": random.choice(statuses)
    }
    producer.send("pedidos_topic", value=evento)
    print(f"📦 Enviado: {evento}")
    pedido_id += 1
    time.sleep(2)  # Envia um evento a cada 2 segundos
