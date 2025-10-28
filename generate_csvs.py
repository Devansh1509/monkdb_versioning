import pandas as pd
import random

names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
cities = ["Delhi", "Mumbai", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Ahmedabad", "Lucknow"]
countries = ["India", "USA", "UK", "Germany", "France", "Australia", "Japan", "Canada"]

def generate_data(start_id):
    data = []
    for i in range(start_id, start_id + 100):
        record = {
            "id": i,
            "name": random.choice(names),
            "city": random.choice(cities),
            "country": random.choice(countries),
            "purchase_amount": round(random.uniform(1000, 5000), 2),
            "loyalty_score": round(random.uniform(0.1, 1.0), 2)
        }
        data.append(record)
    return pd.DataFrame(data)

df_v1 = generate_data(start_id=1)
df_v1.to_csv("data_v1.csv", index=False)
print("✅ data_v1.csv created (100 records).")

df_v2 = generate_data(start_id=101)
df_v2.to_csv("data_v2.csv", index=False)
print("✅ data_v2.csv created (100 records).")
