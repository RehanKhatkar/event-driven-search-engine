import pymongo
import json
client = pymongo.MongoClient("mongodb://localhost:27017/?directConnection=true")
collection = client["ecommerce_search"]["products"]

print("Fetching IDs from MongoDB...")
cursor = collection.find({}, {"_id": 1})
ids = [str(doc["_id"]) for doc in cursor]
with open("ids.json", "w") as f:
    json.dump(ids, f)
print(f"Successfully exported {len(ids)} IDs to ids.json")
