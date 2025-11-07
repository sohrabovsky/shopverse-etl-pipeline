import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text


POSTGRES_URL="postgresql://postgres:postgres@postgres_app:5432/shopverse"

# Creating postgres engine
def creating_postgres_tables():
    engine = create_engine(POSTGRES_URL)
    with engine.begin() as con:
        with open(r"sql/create_tables.sql") as file:
            query = text(file.read())
            con.execute(query)

# Loading Mongo data to Postgres
def load_dim_tables():
    client = MongoClient("mongodb://mongo:27017/")
    shopverse_db = client["shopverse"]
    engine = create_engine(POSTGRES_URL)
    
    # Loading dim_customers table
    orders = list(shopverse_db["orders"].find())
    if not orders:
        print("⚠️ No data found in MongoDB collection 'orders'.")
        return "no data loaded"
    df_orders = pd.DataFrame(orders).drop(columns= "_id", errors= 'ignore')

    # Load each dim table (deduplicating)
    dim_customers = df_orders[["customer_name"]].drop_duplicates()
    dim_cities = df_orders[["city_name"]].drop_duplicates()
    dim_products = df_orders[["product_name", "product_category"]].drop_duplicates()
    dim_events = df_orders[["session_start", "session_end", "device_type"]].drop_duplicates()

    # Write to Postgres
    dim_customers.to_sql("dim_customers", engine, if_exists="append", index=False)
    dim_cities.to_sql("dim_cities", engine, if_exists="append", index=False)
    dim_products.to_sql("dim_products", engine, if_exists="append", index=False)
    dim_events.to_sql("dim_events", engine, if_exists="append", index=False)

    print("✅ Dimension tables loaded successfully.")
    return "tables loaded"