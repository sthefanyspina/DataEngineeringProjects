# 🧩 Data Pipeline + Dashboard

A complete Data Engineering & BI project demonstrating the creation of an **automated data pipeline** capable of:

- Collecting and consolidating data (from CSV / API)
- Cleaning and normalizing inconsistent information
- Generating a clean and standardized dataset
- Persisting the data in a relational database (PostgreSQL)
- Connecting the database to a Business Intelligence tool, in this case Power BI 
- Creating an interactive monitoring dashboard

---

## 📁 Project Structure

📦 data-pipeline
├── data/
│ ├── raw/
│ │ └── dados_clientes_raw.csv # Original dirty dataset
│ └── processed/
│ └── dados_clientes_limpo.csv # Clean & normalized dataset
│
├── pipeline.py # Main pipeline script
├── gerar_dataset_sujo.py # Script to generate a dirty dataset
├── requirements.txt # Project dependencies
├── README.md # This file
└── dashboard/ # Optional folder for dashboard screens/files

---

## ⚙️ Project Steps

### 1️⃣ Generate the “Dirty” Dataset

The script `generate_data.py` creates a dataset with **over 200,000 rows**, containing:

- **id** → with duplicates and wrong data types  
- **name** → inconsistent capitalization and extra spaces  
- **birth_date** → multiple formats + invalid values  
- **purchase_value** → numbers, incorrect text, missing values  


### 2️⃣ Run the Cleaning Pipeline
The script pipeline.py performs the following steps:

- Reads raw data
- Standardizes and normalizes (names, dates, numeric types)
- Removes duplicates and fixes errors
- Generates a clean CSV
- Writes data into PostgreSQL

### 3️⃣ Database (PostgreSQL)
Create the database and configure access:

### 4️⃣ BI Connection
You can connect the database to:

Power BI
- Get Data → PostgreSQL Database


### 🧠 Technologies Used
- Python 3.9+
- Pandas, NumPy, Faker, SQLAlchemy, psycopg2
- PostgreSQL
- Power BI
