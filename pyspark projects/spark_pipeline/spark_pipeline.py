from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, upper, to_date, lit
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql import DataFrame
import re

# =========================
# 1. Initialize Spark Session
# =========================
spark = SparkSession.builder \
    .appName("SalesDataETL") \
    .getOrCreate()

# =========================
# 2. Utility Functions
# =========================

# Safe type conversion
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def safe_int(x):
    try:
        return int(x)
    except:
        return None

# Normalize phone numbers (example US format)
def normalize_phone(phone):
    if phone is None:
        return None
    digits = re.sub(r'\D', '', phone)
    return digits if len(digits) >= 10 else None

from pyspark.sql.functions import udf
safe_float_udf = udf(safe_float, FloatType())
safe_int_udf = udf(safe_int, IntegerType())
normalize_phone_udf = udf(normalize_phone, StringType())

# Log rejected records
def save_rejected(df: DataFrame, condition, file_name: str):
    rejected = df.filter(condition)
    rejected.write.mode("overwrite").csv(file_name, header=True)

# =========================
# 3. Read CSVs
# =========================
sales_df = spark.read.csv("data/raw/Sales_Identification.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("data/raw/Customer_Data.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/raw/Product_Data.csv", header=True, inferSchema=True)
transactions_df = spark.read.csv("data/raw/Transaction_Details.csv", header=True, inferSchema=True)
delivery_df = spark.read.csv("data/raw/Logistics_Delivery.csv", header=True, inferSchema=True)
sellers_df = spark.read.csv("data/raw/Seller_Operation_Info.csv", header=True, inferSchema=True)
financial_df = spark.read.csv("data/raw/Advanced_Financial_Data.csv", header=True, inferSchema=True)

# =========================
# 4. Deduplicate
# =========================
sales_df = sales_df.dropDuplicates(["Sale_ID"])
customers_df = customers_df.dropDuplicates(["Customer_ID"])
products_df = products_df.dropDuplicates(["Product_ID"])
sellers_df = sellers_df.dropDuplicates(["Seller_ID"])

# =========================
# 5. Correct types
# =========================
products_df = products_df.withColumn("Unit_Cost", safe_float_udf(col("Unit_Cost"))) \
                         .withColumn("Unit_Price", safe_float_udf(col("Unit_Price")))

transactions_df = transactions_df.withColumn("Quantity", safe_int_udf(col("Quantity")))

financial_df = financial_df.withColumn("LTV", safe_float_udf(col("LTV")))

# =========================
# 6. Normalize columns
# =========================
customers_df = customers_df.withColumn("Customer_Name", upper(col("Customer_Name")))
# Example phone normalization if phone column exists
if "Phone" in customers_df.columns:
    customers_df = customers_df.withColumn("Phone", normalize_phone_udf(col("Phone")))

sales_df = sales_df.withColumn("Sale_Date", to_date(col("Sale_Date"), 'yyyy-MM-dd'))
delivery_df = delivery_df.withColumn("Delivery_Date", to_date(col("Delivery_Date"), 'yyyy-MM-dd'))

# =========================
# 7. Save rejected records
# =========================
save_rejected(products_df, col("Unit_Cost").isNull() | col("Unit_Price").isNull(), "logs/products_rejected")
save_rejected(transactions_df, col("Quantity").isNull(), "logs/transactions_rejected")
save_rejected(financial_df, col("LTV").isNull(), "logs/financial_rejected")

# =========================
# 8. Join datasets
# =========================
df_full = transactions_df \
    .join(sales_df, "Sale_ID", "left") \
    .join(customers_df, "Customer_ID", "left") \
    .join(products_df, "Product_ID", "left") \
    .join(sellers_df, "Seller_ID", "left") \
    .join(financial_df, "Sale_ID", "left") \
    .join(delivery_df, "Sale_ID", "left")

# =========================
# 9. Exploratory Data Analysis (EDA)
# =========================
# Count by sale status
df_full.groupBy("Sale_Status").count().show()

# Average final sale value by customer type
df_full.groupBy("Customer_Type").agg(avg("Final_Sale_Value").alias("avg_sale_value")).show()

# Total quantity per product
df_full.groupBy("Product_ID", "Product_Name").agg({"Quantity":"sum"}).show()

# =========================
# 10. Save cleaned data to Parquet
# =========================
df_full.write.mode("overwrite").parquet("cleaned_sales_data.parquet")

# =========================
# 11. Performance comparison CSV vs Parquet
# =========================
import time

start = time.time()
spark.read.csv("1_Sales_Identification.csv", header=True).count()
print("CSV read time:", time.time() - start)

start = time.time()
spark.read.parquet("cleaned_sales_data.parquet").count()
print("Parquet read time:", time.time() - start)

# =========================
# 12. Stop Spark
# =========================
spark.stop()
print("ETL pipeline finished successfully!")
