import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# =========================
# Function to clean and standardize DataFrames
# =========================
def clean_dataframe(df, dtypes={}, drop_duplicates=True):
    if drop_duplicates:
        df = df.drop_duplicates()
    for col, dtype in dtypes.items():
        try:
            df[col] = df[col].astype(dtype)
        except:
            if dtype == 'float':
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif dtype == 'int':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            elif dtype == 'datetime':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(str).fillna('')
    return df

# =========================
# 1. Load CSVs
# =========================
sales = pd.read_csv("data/Sales_Identification.csv")
customers = pd.read_csv("data/Customer_Data.csv")
products = pd.read_csv("data/Product_Data.csv")
transactions = pd.read_csv("data/Transaction_Details.csv")
delivery = pd.read_csv("data/Logistics_Delivery.csv")
sellers = pd.read_csv("data/Seller_Operation_Info.csv")
financial = pd.read_csv("data/Advanced_Financial_Data.csv")

# =========================
# 2. Cleaning and standardization
# =========================
sales = clean_dataframe(sales, {
    "Sale_ID": str,
    "Sale_Date": 'datetime',
    "Sale_Time": str,
    "Sale_Type": str,
    "Sale_Status": str
})

customers = clean_dataframe(customers, {
    "Customer_ID": str,
    "Customer_Name": str,
    "Customer_Type": str,
    "Customer_Segment": str,
    "Location": str,
    "Acquisition_Channel": str
})

products = clean_dataframe(products, {
    "Product_ID": str,
    "Product_Name": str,
    "Unit_Cost": 'float',
    "Unit_Price": 'float'
})

transactions = clean_dataframe(transactions, {
    "Sale_ID": str,
    "Product_ID": str,
    "Quantity": 'int',
    "Total_Product_Price": 'float',
    "Discount_Amount": 'float',
    "Discount_Percent": 'float',
    "Taxes": 'float',
    "Shipping": 'float',
    "Final_Sale_Value": 'float'
})

delivery = clean_dataframe(delivery, {
    "Sale_ID": str,
    "Delivery_Status": str,
    "Shipping_Date": 'datetime',
    "Delivery_Date": 'datetime',
    "Carrier": str,
    "Tracking_Code": str,
    "Destination_Zip": str
})

sellers = clean_dataframe(sellers, {
    "Seller_ID": str,
    "Seller_Name": str,
    "Store_Branch": str,
    "Region": str,
    "Sales_Channel": str
})

financial = clean_dataframe(financial, {
    "Sale_ID": str,
    "Gross_Margin": 'float',
    "Net_Margin": 'float',
    "CAC": 'float',
    "LTV": 'float',
    "Chargeback": 'float'
})

# =========================
# 3. Save cleaned CSVs (optional)
# =========================
sales.to_csv("data/clean/sales.csv", index=False)
customers.to_csv("data/clean/customers.csv", index=False)
products.to_csv("data/clean/products.csv", index=False)
transactions.to_csv("data/clean/transactions.csv", index=False)
delivery.to_csv("data/clean/delivery.csv", index=False)
sellers.to_csv("data/clean/sellers.csv", index=False)
financial.to_csv("data/clean/financial.csv", index=False)

# =========================
# 4. PostgreSQL Connection
# =========================
# Replace with your credentials:
engine = create_engine(f'postgresql+psycopg2://postgres:15073003the@localhost:5432/sales')

# =========================
# 5. Save tables to PostgreSQL
# =========================
sales.to_sql('sales', engine, if_exists='replace', index=False)
customers.to_sql('customers', engine, if_exists='replace', index=False)
products.to_sql('products', engine, if_exists='replace', index=False)
transactions.to_sql('transactions', engine, if_exists='replace', index=False)
delivery.to_sql('delivery', engine, if_exists='replace', index=False)
sellers.to_sql('sellers', engine, if_exists='replace', index=False)
financial.to_sql('financial', engine, if_exists='replace', index=False)

print("Pipeline completed! Cleaned data saved to PostgreSQL.")
