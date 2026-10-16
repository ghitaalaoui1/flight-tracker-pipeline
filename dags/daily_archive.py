import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
PARQUET_DIR = "/opt/airflow/data/parquet"
RETENTION_DAYS = 3


def get_collection():
    return MongoClient(MONGO_URI)["flights"]["positions"]


@dag(
    dag_id="daily_archive",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["flights"],
)
def daily_archive():

    @task
    def extract(data_interval_start=None):
        """Lit les positions de la journee ecoulee."""
        start = int(data_interval_start.timestamp())
        end = start + 86400
        cursor = get_collection().find(
            {"timestamp": {"$gte": start, "$lt": end}}, {"_id": 0}
        )
        rows = list(cursor)
        print(f"{len(rows)} positions extraites")
        return rows

    @task
    def write_parquet(rows, data_interval_start=None):
        """Ecrit les donnees en Parquet, partitionnees par date."""
        if not rows:
            print("Rien a archiver")
            return None
        day = data_interval_start.strftime("%Y-%m-%d")
        os.makedirs(PARQUET_DIR, exist_ok=True)
        path = f"{PARQUET_DIR}/positions_{day}.parquet"
        pd.DataFrame(rows).to_parquet(path, index=False)
        print(f"{len(rows)} lignes ecrites dans {path}")
        return path

    @task
    def compute_stats(rows):
        """Quelques statistiques du jour."""
        if not rows:
            return
        df = pd.DataFrame(rows)
        print("Avions distincts :", df["icao24"].nunique())
        print("Pays :", df["country"].nunique())
        print("Altitude moyenne :", round(df["altitude_m"].mean()))
        print("Top 5 pays :")
        print(df["country"].value_counts().head())

    @task
    def cleanup(path):
        """Purge MongoDB au-dela de la retention."""
        if path is None:
            print("Archivage vide, purge annulee")
            return
        cutoff = int((datetime.utcnow() - timedelta(days=RETENTION_DAYS)).timestamp())
        result = get_collection().delete_many({"timestamp": {"$lt": cutoff}})
        print(f"{result.deleted_count} documents supprimes")

    rows = extract()
    path = write_parquet(rows)
    compute_stats(rows)
    cleanup(path)


daily_archive()