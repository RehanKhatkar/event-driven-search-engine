import pandas as pd
import pymongo
import random
import uuid
from bson.decimal128 import Decimal128
client = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")
db = client["ecommerce_search"]
collection = db["products"]
print("Loading dataset into memory...")
df = pd.read_csv("amzon_sample_pod_cat.csv")
products = []
colors = ["Black", "White", "Silver", "Red", "Blue", "Space Grey"]
sizes = ["S", "M", "L", "XL", "Standard"]
for index, row in df.iterrows():
    if pd.isna(row.get('title')):
        continue
    name = str(row.get('title', ''))
    raw_price = row.get('price', 29.99)
    base_price = float(raw_price) if pd.notna(raw_price) else 29.99
    category = str(row.get('category_name', 'General'))
    variants = []
    for _ in range(random.randint(1, 3)):
        calc_price = round(base_price + random.uniform(0.0, 15.0), 2)
        variants.append({
            "sku": f"SKU-{uuid.uuid4().hex[:8].upper()}",
            "color": random.choice(colors),
            "size": random.choice(sizes),
            "price": Decimal128(str(calc_price)),
            "inventoryCount": random.randint(0, 500)
        })

    product = {
        "name": name,
        "description": f"{name}. A premium item in the {category} category.",
        "category": category,
        "tags": [category.lower(), "trending", "amazon"],
        "variants": variants
    }
    products.append(product)

print(f"Parsed {len(products)} products. Beginning bulk insert...")
batch_size = 1000
for i in range(0, len(products), batch_size):
    batch = products[i:i + batch_size]
    collection.insert_many(batch)
    print(f"Inserted {min(i + batch_size, len(products))} / {len(products)}")

print("Database seeding complete!")
