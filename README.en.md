# Flight Tracker Pipeline

Real-time data pipeline tracking air traffic over Morocco.

[Version francaise](README.md)

Aircraft positions are collected from the OpenSky Network API, published to
Kafka, stored in MongoDB, and continuously analyzed by Spark Structured
Streaming to detect notable events.

## Architecture

```
                                    ┌──> Consumer ──> MongoDB
OpenSky API ──> Producer ──> Kafka ─┤
                                    └──> Spark Streaming ──> Alerts
```

| Component | Role |
|---|---|
| Producer | Polls the OpenSky API every 60 s, cleans and validates the data, publishes to the `flight-positions` topic |
| Kafka | Message queue decoupling collection from processing |
| Consumer | Reads the topic and writes to MongoDB |
| Spark Streaming | Analyzes the same stream in parallel, detecting emergencies, approaches and holding patterns |
| MongoDB | Operational storage for positions |

Decoupling through Kafka means a new consumer can be added without touching
the producer.

## Running the project

Requirements: Docker Desktop, and a correctly synchronized system clock
(see Known pitfalls).

```bash
docker compose up -d --build
docker compose ps
```

Follow the activity:

```bash
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f spark
```

Stop:

```bash
docker compose down
```

Data persists in the `mongo_data` volume.

## Accessing the data

MongoDB is exposed on the host on port 27018 (not 27017, to avoid clashing
with a local instance):

```
mongodb://localhost:27018
```

Database `flights`, collection `positions`.

## Real-time detections

| Alert | Criteria |
|---|---|
| `EMERGENCY` | Squawk 7500 (hijack), 7600 (radio failure) or 7700 (general emergency) |
| `LOW_AND_SLOW` | Altitude below 1500 m, speed below 400 km/h, airborne: approach or departure |
| `HOLDING_PATTERN` | At least 4 positions within a 10-minute window, below 6000 m, moving less than 0.15 degrees in both latitude and longitude |

Holding pattern detection uses 10-minute sliding windows (5-minute slide)
with a 2-minute watermark.

## Data quality

- Input validation: positions without latitude or longitude are rejected
- Missing value handling: the API frequently returns empty fields
- Idempotency: a unique index on `icao24` + `timestamp` prevents duplicates
- Unit conversion: speed is converted from m/s to km/h at ingestion
- At-least-once semantics: Kafka offsets are committed after the database
  write, not before, so no message can be marked as processed without having
  been stored. Any duplicates are absorbed by the unique index.
- Unit tests on the raw data transformation

## Resilience

Kafka decouples collection from storage: if MongoDB becomes unavailable,
messages accumulate in the topic instead of being lost.

Tested: MongoDB stopped during collection.

- The producer kept publishing without interruption
- The consumer waited for the database with exponential backoff
  (1, 2, 4, 8, 16 s) instead of crash-looping
- Once the database returned, it resumed from its offset and caught up fully:
  452 messages consumed, consumer lag back to 0, 1400 positions stored,
  no data loss

On startup, the producer and consumer wait for Kafka to pass its healthcheck.
`depends_on` alone is not enough: it waits for the container to start, not
for the broker to accept connections.

## Validation

### HOLDING_PATTERN detection

Script: `scripts/inject_holding.sh [icao24]` (defaults to `test01`). It
injects 5 positions of a near-stationary fictional aircraft near Casablanca
into `flight-positions` (max 0.01 degree drift, around 3000 m), one every
2 minutes, with timestamp = send time. Usage, expected result and Mongo
cleanup are documented at the top of the script.

Result on 2026-10-15:

| Window (UTC) | Points | Spark output |
|---|---|---|
| [14:15, 14:25) | 2 | none (< 4 positions) |
| [14:20, 14:30) | 5 | `HOLDING_PATTERN` test01, nb_positions=5, delta_lat=delta_lon=0.01, altitude_moy=3004 |
| [14:25, 14:35) | 3 | none (< 4 positions) |

The neighbouring windows act as negative controls: they confirm the
`nb_positions >= 4` threshold actually filters, rather than merely showing
that the detection produces output.

The alert fires 2 to 3 minutes after the window closes, once the watermark
passes. The `test01` documents were removed from `flights.positions` after
the test.

Message injection was chosen as the validation method because the target
events are rare in practice: an emergency squawk or a holding pattern implies
an abnormal situation or airport congestion.

## Known pitfalls

**The system clock must be correct.** Injected timestamps come from the local
clock, real traffic timestamps from OpenSky. With a Windows clock running one
hour behind, the Spark watermark, driven by real traffic, ends up one hour
ahead: injected messages are dropped as late, with no error in the logs. The
script now checks the offset against OpenSky and aborts beyond 60 s.

The Docker VM can drift after sleep. Check with
`docker run --rm alpine date -u` and resynchronize with `wsl --shutdown`
followed by a Docker Desktop restart.

**Python buffers its output** when not writing to a terminal. Without
`ENV PYTHONUNBUFFERED=1` in the image, `docker compose logs` stays empty even
when the pipeline is working.

## Tech stack

Python, Apache Kafka, Apache Spark (Structured Streaming), MongoDB,
Docker Compose, pytest

## Tests

```bash
python -m pytest
```

## Project structure

```
src/
├── fetch_flights.py       collection, transformation, validation
├── messaging/
│   ├── producer.py        publishing to Kafka
│   └── consumer.py        reading Kafka and writing to MongoDB
├── streaming/
│   └── spark_job.py       real-time detections
└── storage/
    └── mongo.py           database access

docker/
├── collector.Dockerfile   shared producer/consumer image
├── spark.Dockerfile       Spark image with Kafka connectors
└── download_jars.sh       connector download at build time

scripts/
└── inject_holding.sh      test message injection

tests/
└── test_parsing.py
```

## Known limitations

Tested on the Morocco bounding box, meaning a few dozen simultaneous
aircraft. The architecture is designed to scale but has not been validated on
large volumes.

The producer polls every 60 s, capping each aircraft at 10 positions per
10-minute window. A higher frequency would sharpen detection but consume more
API credits (400 per day anonymously, 4000 with an account).

## Next steps

- Visualization dashboard: live map and alerts
- Airflow orchestration and daily Parquet archiving
- Star-schema data warehouse for historical analysis