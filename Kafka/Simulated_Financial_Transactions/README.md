# CDC with Debezium, Kafka, Postgres, Docker 

## Overview

This Python script is designed to generate simulated financial transactions and insert them into a PostgreSQL database. It's particularly useful for setting up a test environment for Change Data Capture (CDC) with Debezium. The script uses the `faker` library to create realistic, yet fictitious, transaction data and inserts it into a PostgreSQL table.

## System Architecture
![system architecture.png](system%20architecture.png)

## Prerequisites

Before running this script, ensure you have the following installed:
- Python 3.9+
- `psycopg2` library for Python
- `faker` library for Python
- PostgreSQL server running locally or accessible remotely
- Docker and Docker Compose installed on your machine.
- Basic understanding of Docker, Kafka, and Postgres.

## Services in the Compose File

- **Zookeeper:** A centralized service for maintaining configuration information, naming, providing distributed synchronization, and providing group services.
- **Kafka Broker:** A distributed streaming platform that is used here for handling real-time data feeds.
- **Confluent Control Center:** A web-based tool for managing and monitoring Apache Kafka.
- **Debezium:** An open-source distributed platform for change data capture.
- **Debezium UI:** A user interface for managing and monitoring Debezium connectors.
- **Postgres:** An open-source relational database.

## Customization
You can modify the Docker Compose file to suit your needs. For example, you might want to persist data in Postgres by adding a volume for the Postgres service.