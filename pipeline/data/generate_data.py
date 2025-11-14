import pandas as pd
import numpy as np
import random
from faker import Faker

fake = Faker('en_US')
np.random.seed(42)
random.seed(42)

# =========================
# Settings
# =========================
n_sales = 120_000       
n_customers = 100_000   
n_products = 50_000     
n_sellers = 5_000       

# =========================
# 1. Sales Identification
# =========================
sales = pd.DataFrame({
    "Sale_ID": [f"S{1000000+i}" for i in range(n_sales)],
    "Sale_Date": [fake.date_between(start_date='-1y', end_date='today') for _ in range(n_sales)],
    "Sale_Time": [fake.time() for _ in range(n_sales)],
    "Sale_Type": np.random.choice(["in_store", "online", "marketplace", "phone"], n_sales),
    "Sale_Status": np.random.choice(["completed", "canceled", "returned", "pending", "processing"], n_sales)
})

# Introduce duplicates and missing values
sales.loc[5, "Sale_ID"] = sales.loc[4, "Sale_ID"]
sales.loc[10, "Sale_Status"] = None

sales.to_csv("data/Sales_Identification.csv", index=False)

# =========================
# 2. Customer Data
# =========================
customers = pd.DataFrame({
    "Customer_ID": [f"C{100000+i}" for i in range(n_customers)],
    "Customer_Name": [fake.name() if random.random() > 0.05 else None for _ in range(n_customers)],
    "Customer_Type": np.random.choice(["Individual", "Corporate"], n_customers),
    "Customer_Segment": np.random.choice(["retail", "wholesale", "premium"], n_customers),
    "Location": [f"{fake.city()}/{fake.state_abbr()}" for _ in range(n_customers)],
    "Acquisition_Channel": np.random.choice(["organic", "ad", "affiliate", "referral"], n_customers),
    "Age_Range": np.random.choice([None, "18-25", "26-35", "36-45", "46-60", "60+"], n_customers)
})

# Introduce errors
customers.loc[2, "Customer_ID"] = customers.loc[1, "Customer_ID"]
customers.loc[3, "Age_Range"] = "twenty-five"

customers.to_csv("data/Customer_Data.csv", index=False)

# =========================
# 3. Product Data
# =========================
products = pd.DataFrame({
    "Product_ID": [f"P{200000+i}" for i in range(n_products)],
    "Product_Name": [fake.word().capitalize() for _ in range(n_products)],
    "Product_Category": np.random.choice(["Electronics", "Clothing", "Food", "Toys"], n_products),
    "Product_Subcategory": np.random.choice(["SubA", "SubB", "SubC"], n_products),
    "SKU": [f"SKU{i:06d}" for i in range(n_products)],
    "Brand": [fake.company() for _ in range(n_products)],
    "Supplier": [fake.company() for _ in range(n_products)],
    "Unit_Cost": np.round(np.random.uniform(5, 100, n_products), 2),
    "Unit_Price": np.round(np.random.uniform(10, 200, n_products), 2)
})

# Introduce errors
products.loc[7, "Product_ID"] = products.loc[6, "Product_ID"]
products.loc[8, "Unit_Cost"] = "ten"
products.loc[9, "Unit_Price"] = None

products.to_csv("data/Product_Data.csv", index=False)

# =========================
# 4. Transaction Details
# =========================
transactions = pd.DataFrame({
    "Sale_ID": np.random.choice(sales["Sale_ID"], n_sales),
    "Product_ID": np.random.choice(products["Product_ID"], n_sales),
    "Quantity": np.random.randint(1, 5, n_sales),
    "Total_Product_Price": np.random.uniform(10, 200, n_sales),  # simplified calculation
    "Discount_Amount": np.random.choice([0, 5, 10, None], n_sales),
    "Discount_Percent": np.random.choice([0, 5, 10, None], n_sales),
    "Taxes": np.random.uniform(0, 20, n_sales),
    "Shipping": np.random.choice([0, 15, 20, None], n_sales),
    "Payment_Method": np.random.choice(["card", "pix", "boleto", "cash", "store_credit"], n_sales),
    "Installments": np.random.choice(["full", "2x no interest", "3x with interest", None], n_sales),
    "Final_Sale_Value": np.random.uniform(20, 500, n_sales)
})

transactions.loc[1, "Quantity"] = "two"
transactions.loc[2, "Sale_ID"] = transactions.loc[3, "Sale_ID"]

transactions.to_csv("data/Transaction_Details.csv", index=False)

# =========================
# 5. Logistics / Delivery
# =========================
delivery = pd.DataFrame({
    "Sale_ID": np.random.choice(sales["Sale_ID"], n_sales),
    "Delivery_Status": np.random.choice(["shipped", "delivered", "in_transit", "delayed"], n_sales),
    "Shipping_Date": [fake.date_between(start_date='-1y', end_date='today') for _ in range(n_sales)],
    "Delivery_Date": [fake.date_between(start_date='-1y', end_date='today') for _ in range(n_sales)],
    "Carrier": np.random.choice(["USPS", "FedEx", "UPS"], n_sales),
    "Tracking_Code": [fake.bothify(text='??#####') for _ in range(n_sales)],
    "Destination_Zip": [fake.postcode() for _ in range(n_sales)]
})

delivery.loc[0, "Delivery_Date"] = None
delivery.loc[1, "Sale_ID"] = delivery.loc[2, "Sale_ID"]

delivery.to_csv("data/Logistics_Delivery.csv", index=False)

# =========================
# 6. Seller / Operation Info
# =========================
sellers = pd.DataFrame({
    "Seller_ID": [f"SL{10000+i}" for i in range(n_sellers)],
    "Seller_Name": [fake.name() for _ in range(n_sellers)],
    "Store_Branch": [fake.company() for _ in range(n_sellers)],
    "Region": np.random.choice(["North", "South", "East", "West"], n_sellers),
    "Sales_Channel": np.random.choice(["e-commerce", "physical_store", "marketplace"], n_sellers)
})

sellers.loc[1, "Seller_ID"] = sellers.loc[0, "Seller_ID"]
sellers.loc[2, "Region"] = 123

sellers.to_csv("data/Seller_Operation_Info.csv", index=False)

# =========================
# 7. Advanced Financial Data
# =========================
financial = pd.DataFrame({
    "Sale_ID": np.random.choice(sales["Sale_ID"], n_sales),
    "Gross_Margin": np.random.uniform(0, 50, n_sales),
    "Net_Margin": np.random.uniform(-10, 40, n_sales),
    "CAC": np.random.uniform(5, 50, n_sales),
    "LTV": np.random.uniform(100, 1000, n_sales),
    "Chargeback": np.random.choice([0, 1, None], n_sales)
})

financial.loc[1, "LTV"] = "high"
financial.loc[2, "Sale_ID"] = financial.loc[3, "Sale_ID"]

financial.to_csv("data/Advanced_Financial_Data.csv", index=False)

print("All 7 datasets was saved as CSV files.")
