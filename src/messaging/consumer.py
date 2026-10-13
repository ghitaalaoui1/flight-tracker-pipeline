import json
import os
import time

from kafka import KafkaConsumer
from pymongo.errors import PyMongoError

from src.storage.mongo import save_planes

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "flight-positions"
MAX_RETRY_DELAY = 60


def save_with_retry(plane):
    delay = 1
    while True:
        try:
            return save_planes([plane])
        except PyMongoError as e:
            print(f"MongoDB indisponible ({type(e).__name__}), nouvel essai dans {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="mongo-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # Laisse le temps d'attendre MongoDB sans être exclu du groupe
        max_poll_interval_ms=600000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print("En attente de messages...")
    for message in consumer:
        inserted = save_with_retry(message.value)
        # On ne valide l'offset qu'une fois le message enregistré
        consumer.commit()
        if inserted:
            print("Enregistré :", message.value["callsign"] or message.value["icao24"])


if __name__ == "__main__":
    main()
