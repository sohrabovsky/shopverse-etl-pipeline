import os
csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "products.csv"))
print("Resolved CSV path:", csv_path)
print("Exists:", os.path.exists(csv_path))

import pandas as pd
from pymongo import MongoClient

# 📁 تعیین مسیر مطلق فایل CSV
base_dir = os.path.abspath(os.path.dirname(__file__))
csv_path = os.path.join(base_dir, "..", "..", "data", "raw", "products.csv")

print("📄 CSV Path:", csv_path)
print("✅ File Exists:", os.path.exists(csv_path))

# 🧠 اتصال به MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["shopverse"]
collection = db["products"]

# 📥 خواندن CSV
df = pd.read_csv(csv_path, encoding='utf-8', sep=",")
records = df.to_dict(orient="records")

# 💾 درج در MongoDB
collection.insert_many(records)

print(f"✅ {len(records)} records were imported into MongoDB.")
