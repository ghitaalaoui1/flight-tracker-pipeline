# Flight Tracker Pipeline

Real-time data pipeline tracking air traffic over Morocco.

[Version francaise](README.md)

Aircraft positions are collected from the OpenSky Network API, published to
Kafka, stored in MongoDB, continuously analyzed by Spark Structured
Streaming, visualized in a Streamlit dashboard, and archived nightly to
Parquet by an Airflow DAG.

![Dashboard](docs/images/airtrafic.png)

## Architecture

```
                                    ┌──> Consumer ──> MongoDB ──> Dashboard
OpenSky API ──> Producer ──> Kafka ─┤                    │
                                    └──> Spark Streaming │──> Alerts
                                                         │
                                         Airflow (02:00) ┴──> Parquet
```

| Component | Role |
|---|---|
| Producer | Polls the OpenSky API every 60 s, cleans and validates the data, publishes to the `flight-positions` topic |
| Kafka | Message queue decoupling collection from processing |
| Consumer | Reads the topic and writes to MongoDB |
| Spark Streaming | Analyzes the same stream in parallel, detecting emergencies, approaches and holding patterns |
| MongoDB | Operational storage for positions (hot layer) |
| Dashboard | Streamlit interface: live map, metrics, aircraft on approach |
| Airflow | Orchestrates the daily Parquet archive and the MongoDB purge |
| PostgreSQL | Airflow's own metadata database (run history) |

Decoupling through Kafka means a new consumer can be added without touching
the producer.

The project combines both processing regimes: **streaming** to detect and
display in real time, **batch** to archive, aggregate and clean on a schedule.

## Running the project

Requirements: Docker Desktop, and a correctly synchronized system clock
(see Known pitfalls).

```bash
docker compose up -d --build
docker compose ps
```

Interfaces available once the stack is up:

| Service | Address | Credentials |
|---|---|---|
| Dashboard | `http://localhost:8501` | none |
| Airflow | `http://localhost:8080` | `admin` / `admin` |

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

Data persists in the `mongo_data` and `pg_data` volumes.

## Accessing the data

MongoDB is exposed on the host on port 27018 (not 27017, to avoid clashing
with a local instance):

```
mongodb://localhost:27018
```

Database `flights`, collection `positions`.

Parquet archives are written to `data/parquet/`, one file per day:
`positions_YYYY-MM-DD.parquet`.

## Data lifecycle

| Data | Location | Purpose |
|---|---|---|
| Last 3 days | MongoDB | Dashboard, alerts, live queries |
| Full history | Parquet files | Multi-week analysis |

Splitting the hot and cold layers keeps the operational database from growing
without bound, while preserving history in a compact columnar format.

## Dashboard

Streamlit interface backed by MongoDB, showing the last 15 minutes of traffic:

- Metrics: aircraft tracked, countries represented, average altitude
- Map centred on Morocco, one position per aircraft, with a tooltip
  (callsign, country, altitude)
- Breakdown by country of registration
- Table of aircraft on approach or departure
- Full flight table, sorted by altitude

To run it on its own, outside Docker:

```bash
streamlit run src/dashboard/app.py
```

## Real-time detections

| Alert | Criteria |
|---|---|
| `EMERGENCY` | Squawk 7500 (hijack), 7600 (radio failure) or 7700 (general emergency) |
| `LOW_AND_SLOW` | Altitude below 1500 m, speed below 400 km/h, airborne: approach or departure |
| `HOLDING_PATTERN` | At least 4 positions within a 10-minute window, below 6000 m, moving less than 0.15 degrees in both latitude and longitude |

Holding pattern detection uses 10-minute sliding windows (5-minute slide)
with a 2-minute watermark.

## Orchestration

![Airflow](docs/images/airflow.png)

The `daily_archive` DAG runs every night at 02:00:

```
extract ──┬──> compute_stats
          │
          └──> write_parquet ──> cleanup
```

| Task | Role |
|---|---|
| `extract` | Reads the previous day's positions from MongoDB |
| `write_parquet` | Writes them to `data/parquet/positions_<date>.parquet` |
| `compute_stats` | Computes distinct aircraft, countries, average altitude, top 5 countries |
| `cleanup` | Deletes positions older than 3 days from MongoDB |

**Task ordering here is a design decision, not a convenience.** `cleanup`
takes the path produced by `write_parquet` as its input, creating an explicit
dependency: Airflow never purges before the archive has succeeded. If the
archive fails, the purge does not run; if the batch is empty, it cancels
itself. The reasoning mirrors the Kafka offsets: between two possible
orderings, pick the one whose failure is recoverable. Wasted disk space can
be reclaimed, lost data cannot.

Each task is configured with `retries: 2` and a 5-minute delay.

Run of 2026-10-16 over the 15th:

| Task | Result |
|---|---|
| `extract` | 5313 positions extracted |
| `write_parquet` | 5313 rows written to `positions_2026-10-15.parquet` |
| `compute_stats` | average altitude 10,657 m |
| `cleanup` | 873 documents deleted |

Extracted positions matching written rows is the check that matters: no rows
lost between MongoDB and the Parquet file.

## Data quality

- Input validation: positions without latitude or longitude are rejected
- Missing value handling: the API frequently returns empty fields
- Idempotency: a unique index on `icao24` + `timestamp` prevents duplicates
- Unit conversion: speed is converted from m/s to km/h at ingestion
- At-least-once semantics: Kafka offsets are committed after the database
  write, not before, so no message can be marked as processed without having
  been stored. Any duplicates are absorbed by the unique index.
- Archive before purge: nothing is deleted from MongoDB without having been
  written to Parquet first
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
for the broker to accept connections. Airflow waits on PostgreSQL's
healthcheck the same way.

The dashboard shows an explicit message when MongoDB is unreachable, instead
of surfacing a Python traceback.

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

**Automatic map framing follows the data.** With `st.map`, the view drifted
onto Spain whenever traffic clustered in the north of the bounding box. The
dashboard therefore pins the centre and zoom explicitly through `pydeck`.

**Logs from older Airflow task tries can be lost.** The UI serves them from
the worker that ran the task; if that container has been recreated, it shows
an `HTTPConnectionPool` error instead. Only the latest try is viewable. When
in doubt, read the files directly under `/opt/airflow/logs`.

## Tech stack

Python, Apache Kafka, Apache Spark (Structured Streaming), MongoDB,
Apache Airflow, PostgreSQL, Parquet, Streamlit, Docker Compose, pytest

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
├── dashboard/
│   └── app.py             Streamlit interface
└── storage/
    └── mongo.py           database access

dags/
└── daily_archive.py       Airflow DAG: archive and purge

docker/
├── collector.Dockerfile   shared producer/consumer image
├── spark.Dockerfile       Spark image with Kafka connectors
├── dashboard.Dockerfile   Streamlit image
└── download_jars.sh       connector download at build time

scripts/
└── inject_holding.sh      test message injection

tests/
└── test_parsing.py

data/parquet/              daily archives (git-ignored)
```

## Known limitations

Tested on the Morocco bounding box, meaning a few dozen simultaneous
aircraft. The architecture is designed to scale but has not been validated on
large volumes.

The producer polls every 60 s, capping each aircraft at 10 positions per
10-minute window. A higher frequency would sharpen detection but consume more
API credits (400 per day anonymously, 4000 with an account).

The dashboard reads MongoDB on each manual refresh, with no automatic update.

Airflow runs with `LocalExecutor` and default credentials, which suits local
use only. A real deployment would need secret management and a distributed
executor.

## Next steps

- Star-schema data warehouse for historical analysis
- Spark alerts surfaced in the dashboard
- OpenSky OAuth2 authentication to move from 400 to 4000 daily API credits
