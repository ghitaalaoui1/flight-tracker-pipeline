import json
import os
import time

from kafka import KafkaProducer

from src.fetch_flights import BBOX_MAROC, fetch_states, parse_state, is_valid

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "flight-positions"
INTERVAL = 60


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    while True:
        states = fetch_states(BBOX_MAROC)
        planes = [parse_state(s) for s in states]
        planes = [p for p in planes if is_valid(p)]
        for plane in planes:
            producer.send(TOPIC, key=plane["icao24"], value=plane)
        producer.flush()
        print(f"{len(planes)} positions envoyées dans {TOPIC}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()