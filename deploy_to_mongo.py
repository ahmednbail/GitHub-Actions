import os
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient

host = os.environ["MONGO_HOST"]
db_name = os.environ["MONGO_DB"]
user = os.environ["MONGO_USER"]
password = os.environ["MONGO_PASSWORD"]

uri = f"mongodb+srv://{user}:{password}@{host}/?appName=Cluster0"
client = MongoClient(uri)
db = client[db_name]

# Read all CSVs from data/ folder and create a collection for each
data_folder = Path("data")
for csv_file in data_folder.glob("*.csv"):
    collection_name = csv_file.stem          # clean_sales.csv → clean_sales
    df = pd.read_csv(csv_file)
    records = df.to_dict("records")

    # Add timestamp to each record
    loaded_at = datetime.now(timezone.utc)
    for record in records:
        record["loaded_at"] = loaded_at

    collection = db[collection_name]
    collection.delete_many({})
    collection.insert_many(records)
    print(f"Deployed {len(df)} records → {db_name}.{collection_name}")