import json
import os

from kafka import KafkaConsumer

from src.storage.mongo import save_planes

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "flight-positions"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="mongo-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print("En attente de messages...")
    for message in consumer:
        inserted = save_planes([message.value])
        if inserted:
            print("Enregistré :", message.value["callsign"] or message.value["icao24"])


if __name__ == "__main__":
    main()