import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# -----------------------------
# Function to generate random dates
# -----------------------------
def random_date(start, end):
    """Generate a random datetime between two datetime objects."""
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=rand_seconds)

# -----------------------------
# Create base clean dataset
# -----------------------------
num_rows = 100

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

types = ["Online", "In-store", "Phone", "Mobile"]
statuses = ["Completed", "Pending", "Cancelled"]

data = {
    "ID_Venda": [i for i in range(1, num_rows + 1)],
    "Data_Venda": [random_date(start_date, end_date).date() for _ in range(num_rows)],
    "Hora_Venda": [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}" for _ in range(num_rows)],
    "Tipo_Venda": [random.choice(types) for _ in range(num_rows)],
    "Status_Venda": [random.choice(statuses) for _ in range(num_rows)],
}

df = pd.DataFrame(data)

# -----------------------------
# Introduce data quality issues
# -----------------------------

# 1) Duplicate rows
duplicate_rows = df.sample(5, replace=False)
df = pd.concat([df, duplicate_rows], ignore_index=True)

# 2) Missing values
for col in df.columns:
    for _ in range(3):
        idx = random.randint(0, len(df) - 1)
        df.loc[idx, col] = np.nan

# 3) Inconsistent data types
df.loc[random.randint(0, len(df)-1), "ID_Venda"] = "ABC"        # string instead of int
df.loc[random.randint(0, len(df)-1), "Data_Venda"] = "32/13/99" # invalid date format
df.loc[random.randint(0, len(df)-1), "Hora_Venda"] = 2500       # invalid hour

# 4) Disorganized text data
df.loc[random.randint(0, len(df)-1), "Tipo_Venda"] = "  online "    # extra spaces, lower case
df.loc[random.randint(0, len(df)-1), "Status_Venda"] = "Completedd" # misspelling

# -----------------------------
# Shuffle the dataset to increase disorder
# -----------------------------
df = df.sample(frac=1).reset_index(drop=True)

# -----------------------------
# Save the dataset
# -----------------------------
output_file = "data/sales_data.csv"
df.to_csv(output_file, index=False)

print(f"Dataset created successfully: {output_file}")
