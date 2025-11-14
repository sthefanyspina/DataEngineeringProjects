# Airflow Data Pipeline Example

This project demonstrates a simple **data pipeline** using **Apache Airflow**, which downloads a CSV, performs transformations, and loads the results into a database.

---


## Project Overview

The pipeline performs the following steps:

1. **Download CSV**: Fetches a CSV file from a URL.
2. **Transform Data**: Cleans and transforms the CSV using Python and Pandas.
3. **Load to Database**: Inserts the cleaned data into a PostgreSQL or SQLite database.

---

## Dataset

The sample dataset used is the **Air Travel dataset**:

- Source: [Air Travel CSV](https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv)
- The dataset contains monthly passenger numbers for international flights.

---

## Pipeline

The pipeline is implemented as an **Airflow DAG** with three main tasks:

1. `download_csv` – Downloads the CSV file.
2. `transform_csv` – Cleans the dataset, standardizes columns, and calculates totals.
3. `load_to_db` – Loads the transformed data into a database.

---

## Installation

### Prerequisites

- Python 3.9+
- Apache Airflow 2.x
- PostgreSQL or SQLite
- Pip packages: `pandas`, `requests`, `psycopg2`

### Install dependencies

```bash
pip install apache-airflow pandas requests psycopg2-binary

### Database
By default, the pipeline uses PostgreSQL, but it can be easily switched to SQLite.

PostgreSQL Connection Example:

python
Copiar código
conn = psycopg2.connect(
    host="localhost",
    database="mydatabase",
    user="username",
    password="password"
)
The transformed data is stored in the table: airtravel_table.

### Airflow DAG
- DAG ID: airflow_data_pipeline
- Schedule Interval: @daily
- Tasks:
    - download_csv
    - transform_csv
    - load_to_db