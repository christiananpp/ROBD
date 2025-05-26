### --- load_mongo.py ---
import pandas as pd
from pymongo import MongoClient

# Load data
csv_path = "appointment_salon_20000.csv"
df = pd.read_csv(csv_path)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["salon_db"]
collection = db["appointments"]

# Insert data
collection.delete_many({})  # clear before insert
collection.insert_many(df.to_dict("records"))

# Create indexes
collection.create_index("id_pegawai")
collection.create_index("tanggal")
print("Data inserted and indexed in MongoDB.")