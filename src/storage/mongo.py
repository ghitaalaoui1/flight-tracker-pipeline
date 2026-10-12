import os

from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27018")


def save_planes(planes):
    if not planes:
        return 0
    with MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) as client:
        collection = client["flights"]["positions"]
        collection.create_index([("icao24", 1), ("timestamp", 1)], unique=True)
        operations = [
            UpdateOne(
                {"icao24": p["icao24"], "timestamp": p["timestamp"]},
                {"$set": p},
                upsert=True,
            )
            for p in planes
        ]
        result = collection.bulk_write(operations)
        return result.upserted_count
