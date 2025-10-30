import pandas as pd
from faker import Faker
import random
from tqdm import tqdm
import os

# setting
NUM_ROWS = 1_000_000
fake = Faker()
Faker.seed(42)

# product category
categories = ['electronics', 'books', 'clothing', 'home', 'sports', 'beauty']

# result
def generate_product(i):
    return {
        "product_id": i + 1,
        "name": fake.word().capitalize() + " " + fake.word().capitalize(),
        "category": random.choice(categories),
        "price": round(random.uniform(10.0, 500.0), 2),
        "stock": random.randint(0, 1000),
        "created_at": fake.date_time_this_decade().isoformat()
    }

# data generation
print(f"Generate {NUM_ROWS} Data Row...")
products = [generate_product(i) for i in tqdm(range(NUM_ROWS))]

# DataFrame
df = pd.DataFrame(products)

# stored CSV
output_dir = "data/raw"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "products.csv")
df.to_csv(output_path, index=False)

print(f"✅ Saved: {output_path}")
