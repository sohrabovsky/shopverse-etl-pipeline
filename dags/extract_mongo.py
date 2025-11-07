import pandas as pd
from faker import Faker
import random
import csv
from datetime import datetime, timedelta

fake = Faker()

# ✅ Configurations
N_TRANSACTIONS = 1_000_000
N_CUSTOMERS = 100_000
N_CITIES = 10
N_PRODUCTS = 10
N_CATEGORIES = 3
DEVICE_TYPES = ["mobile", "desktop"]

# ✅ Pre-generate reusable pools (to keep data consistent and fast)
customers = [fake.name() for _ in range(N_CUSTOMERS)]
cities = [fake.city() for _ in range(N_CITIES)]
categories = [f"Category_{i+1}" for i in range(N_CATEGORIES)]
products = [f"Product_{i+1}" for i in range(N_PRODUCTS)]

# ✅ Utility function
def random_session():
    start = fake.date_time_between(start_date='-1y', end_date='now')
    duration = random.randint(1, 3600)  # session between 1 sec – 1 hour
    end = start + timedelta(seconds=duration)
    return start, end

# ✅ Generate one transaction record
def generate_transaction(i):
    session_start, session_end = random_session()
    return {
        "id": i + 1,
        "customer_name": random.choice(customers),
        "city": random.choice(cities),
        "product_name": random.choice(products),
        "product_category": random.choice(categories),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(5, 500), 2),
        "created_at": fake.date_time_between(start_date='-1y', end_date='now').isoformat(),
        "session_start": session_start.isoformat(),
        "session_end": session_end.isoformat(),
        "device_type": random.choice(DEVICE_TYPES)
    }

# ✅ Stream to CSV (memory efficient)
def generate_to_csv(filename="transactions.csv", batch_size=100_000):
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "id", "customer_name", "city", "product_name", "product_category",
            "quantity", "price", "created_at", "session_start", "session_end", "device_type"
        ])
        writer.writeheader()
        total = 0
        while total < N_TRANSACTIONS:
            batch = [generate_transaction(i + total) for i in range(min(batch_size, N_TRANSACTIONS - total))]
            writer.writerows(batch)
            total += len(batch)
            print(f"✅ Generated {total:,} / {N_TRANSACTIONS:,} records", end="\r")
    print(f"\n🎉 Done! File saved as {filename}")

if __name__ == "__main__":
    generate_to_csv("./shopverse-etl-pipeline/data/orders.csv")


