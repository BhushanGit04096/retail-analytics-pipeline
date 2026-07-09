import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os
from google.cloud import storage

# === CONFIG ===
PROJECT_ID = "playground-s-11-4db4d9ec"
BUCKET_NAME = f"retail-raw-{PROJECT_ID}"
fake = Faker()

def generate_daily_orders(date, num_orders=200):
    data = []
    for _ in range(num_orders):
        data.append({
            "order_id": fake.uuid4(),
            "customer_id": random.randint(1, 1000),
            "product_id": random.randint(1, 500),
            "product_name": fake.word().capitalize() + " " + random.choice(["Pro", "Lite", "Max", "Basic"]),
            "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"]),
            "store_id": random.randint(1, 50),
            "channel": random.choice(["online", "in-store"]),
            "quantity": random.randint(1, 5),
            "unit_price": round(random.uniform(5.0, 500.0), 2),
            "order_date": date.strftime("%Y-%m-%d")
        })
    return pd.DataFrame(data)

def upload_to_gcs(df, date, channel):
    blob_path = f"date={date.strftime('%Y-%m-%d')}/channel={channel}/orders.csv"
    temp_file = f"/tmp/orders_{date.strftime('%Y%m%d')}_{channel}.csv"
    df.to_csv(temp_file, index=False)
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(temp_file)
    print(f"✅ Uploaded {len(df)} rows to gs://{BUCKET_NAME}/{blob_path}")
    os.remove(temp_file)

if __name__ == "__main__":
    print("🚀 Generating fake retail orders for 30 days...")
    start_date = datetime(2025, 1, 1)
    
    for i in range(30):
        date = start_date + timedelta(days=i)
        for channel in ["online", "in-store"]:
            num_orders = random.randint(100, 300)
            df = generate_daily_orders(date, num_orders)
            upload_to_gcs(df, date, channel)
    
    print("✅ Done! All 30 days of data uploaded to GCS.")
