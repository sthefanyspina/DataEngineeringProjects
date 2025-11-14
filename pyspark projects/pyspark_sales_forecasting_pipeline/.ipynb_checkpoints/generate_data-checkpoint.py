import pandas as pd
import numpy as np

# Parameters
num_rows = 200_000
num_stores = 50
num_products = 20
start_date = pd.to_datetime("2020-01-01")
end_date = pd.to_datetime("2023-01-01")

# Generate random dates between start_date and end_date
dates = pd.to_datetime(np.random.randint(start_date.value//10**9, end_date.value//10**9, num_rows), unit='s')

# Generate random stores and products
stores = np.random.randint(1, num_stores+1, size=num_rows)
products = np.random.randint(1, num_products+1, size=num_rows)

# Generate synthetic sales: base sales + random noise + product effect
sales = (np.random.randint(100, 2000, size=num_rows) 
         + 50 * np.random.rand(num_rows) * products * 0.1)

# Create DataFrame
df = pd.DataFrame({
    "date": dates,
    "store": stores,
    "product": products,
    "sales": np.round(sales, 2)
})

# Optional: sort by date
df = df.sort_values("date").reset_index(drop=True)

# Show sample
print(df.head())

df.to_csv("sales_data.csv", index=False)