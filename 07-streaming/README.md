# Module 7 — Streaming with Kafka & PyFlink

**Course:** [Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp/)
**Module:** 07 — Streaming
**Dataset:** Green Taxi Trip Data — October 2025

---

## Overview

This module covers real-time stream processing using **Redpanda** (a Kafka-compatible broker) and **PyFlink**. You will build Kafka producers and consumers in Python, then write Flink jobs that process a stream of NYC green taxi trips using tumbling and session windows.

### Key Learning Areas

- Setting up Redpanda as a drop-in Kafka replacement
- Creating Kafka topics with `rpk`
- Building Python Kafka producers using `kafka-python`
- Building Python Kafka consumers and computing trip statistics
- Writing PyFlink streaming jobs with Table API / SQL
- Applying **tumbling windows** and **session windows** in Flink
- Sinking Flink results to PostgreSQL via JDBC connector
- Querying aggregated streaming results with SQL

### Dataset Information

| Property        | Value                                                                                          |
|-----------------|-----------------------------------------------------------------------------------------------|
| Dataset name    | Green Taxi Trip Records — October 2025                                                         |
| File format     | Parquet                                                                                        |
| Download URL    | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet                |
| Kafka topic     | `green-trips`                                                                                  |
| Broker          | Redpanda on `localhost:9092` (internal: `redpanda:29092`)                                     |
| Sink            | PostgreSQL on `localhost:5432`                                                                 |

---

## Project Setup

1. **Download the dataset** into the `data/` directory:

   ```bash
   mkdir -p data
   curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet
   mv green_tripdata_2025-10.parquet data/
   ```

2. **Build the Docker image and start services** (run from the `07-streaming/` directory):

   ```bash
   docker compose build
   docker compose up -d
   ```

   This starts:
   - **Redpanda** (Kafka-compatible broker) on `localhost:9092`
   - **Flink Job Manager** at http://localhost:8081
   - **Flink Task Manager**
   - **PostgreSQL** on `localhost:5432` (user: `postgres`, password: `postgres`)

3. **Clean start** (if you have old containers/volumes from a previous run):

   ```bash
   docker compose down -v
   docker compose build
   docker compose up -d
   ```

4. **Create the `green-trips` Kafka topic:**

   ```bash
   docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
   ```

5. **Install Python dependencies:**

   ```bash
   pip install kafka-python pandas pyarrow
   ```

6. **Create PostgreSQL tables** (before running Flink jobs):

   ```bash
   docker exec -it 07-streaming-postgres-1 psql -U postgres -d postgres
   ```

   Then run the DDL statements from [`src/queries.sql`](src/queries.sql).

---

## Project Structure

```
07-streaming/
├── docker-compose.yaml          # Redpanda + Flink + PostgreSQL services
├── Dockerfile.flink             # Custom Flink image with PyFlink & JDBC driver
├── flink-config.yaml            # Flink configuration
├── pyproject.flink.toml         # Python dependencies for Flink container
├── data/
│   └── green_tripdata_2025-10.parquet
└── src/
    ├── queries.sql              # PostgreSQL DDL + result queries
    ├── producers/
    │   └── producer.py          # Kafka producer — sends green taxi trips
    ├── consumers/
    │   └── consumer.py          # Kafka consumer — counts long-distance trips
    └── job/
        ├── aggregation_job.py   # Q4: 5-min tumbling window per PULocationID
        ├── session_job.py       # Q5: Session window per PULocationID
        └── tumbling_tip_job.py  # Q6: 1-hour tumbling window for tip_amount
```

### Pipeline Metadata

| Property           | Value                                      |
|--------------------|--------------------------------------------|
| Kafka topic        | `green-trips`                              |
| Kafka broker       | `redpanda:29092` (internal)                |
| PostgreSQL DB      | `postgres` (host: `postgres`, port: `5432`) |
| Flink parallelism  | `1` (single partition topic)               |
| Checkpoint interval| 10 seconds                                 |

---

## Homework Solutions

### Question 1. Redpanda Version

Run `rpk version` inside the Redpanda container:

```bash
docker exec -it 07-streaming-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Answer:** **v25.3.9**

**Result:**

```
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
```

**Explanation:** The `rpk` CLI is bundled with Redpanda and reports its version when called with the `version` subcommand. The exact version is determined by the Docker image tag used in `docker-compose.yaml` (`redpandadata/redpanda:v25.3.9`), so the answer reflects that pinned image version. Checking the version confirms the environment is set up correctly before proceeding with the homework tasks.

---

### Question 2. Sending Data to Redpanda

Create a topic called `green-trips`:

```bash
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
```

Run the producer to send all green taxi trip records from October 2025:

```bash
python src/producers/producer.py
```

The producer reads the parquet file, keeps only the required columns, converts datetime columns to strings, and sends each row as a JSON message to the `green-trips` topic. It measures the total time taken to send all records and flush.

**Columns sent:**

| Column                  | Type     |
|-------------------------|----------|
| `lpep_pickup_datetime`  | string   |
| `lpep_dropoff_datetime` | string   |
| `PULocationID`          | integer  |
| `DOLocationID`          | integer  |
| `passenger_count`       | float    |
| `trip_distance`         | float    |
| `tip_amount`            | float    |
| `total_amount`          | float    |

How long did it take to send the data?

- [x] **10 seconds**
- [ ] 60 seconds
- [ ] 120 seconds
- [ ] 300 seconds

**Answer:** **~10 seconds** (`took 12.81 seconds`)

**Result:**

```
took 12.81 seconds
```

**Explanation:** The producer uses `kafka-python`'s `KafkaProducer` with JSON serialization. Each row is converted to a dictionary and sent individually; datetime objects are cast to strings since JSON does not have a native datetime type. The wall-clock time is measured around the main send loop plus the `producer.flush()` call, which waits until all in-flight messages are acknowledged by the broker. Timing varies by hardware and network, but typically falls in the tens of seconds range for this dataset size.

---

### Question 3. Consumer — Trip Distance

Run the consumer to read all messages from the `green-trips` topic:

```bash
python src/consumers/consumer.py
```

The consumer reads from the earliest offset and counts trips where `trip_distance > 5.0`.

How many trips have `trip_distance` > 5?

- [ ] 6,506
- [ ] 7,506
- [x] **8,506**
- [ ] 9,506

**Answer:** **8,506**

**Result:**

```
Reading all messages from 'green-trips'...
Total trips:            49416
Trips with distance > 5: 8506
```

**Explanation:** The consumer subscribes to `green-trips` with `auto_offset_reset='earliest'`, ensuring it reads all messages from the beginning. It deserializes each JSON message and checks the `trip_distance` field. The `consumer_timeout_ms=10_000` setting causes the consumer to stop after 10 seconds of no new messages, which cleanly terminates the loop once all records have been consumed. The resulting count reflects the full October 2025 dataset.

---

## Part 2: PyFlink (Questions 4–6)

Before running Flink jobs, **create the required PostgreSQL tables**. Connect to PostgreSQL and run the DDL:

```bash
docker exec -it 07-streaming-postgres-1 psql -U postgres -d postgres
```

```sql
-- Question 4 table
CREATE TABLE processed_events_aggregated (
    window_start  TIMESTAMP,
    PULocationID  INTEGER,
    num_trips     BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

-- Question 5 table
CREATE TABLE processed_events_session (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    PULocationID  INTEGER,
    num_trips     BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, PULocationID)
);

-- Question 6 table
CREATE TABLE tip_per_hour (
    window_start  TIMESTAMP,
    window_end    TIMESTAMP,
    total_tip     DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
```

> **Note:** If you sent data to the topic multiple times, delete and recreate it before running Flink jobs to avoid duplicate records:
> ```bash
> docker exec -it 07-streaming-redpanda-1 rpk topic delete green-trips
> docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips
> # then re-run the producer
> ```

Submit Flink jobs with:

```bash
docker exec -it 07-streaming-jobmanager-1 flink run -py /opt/src/job/<job_file.py>
```

Let each job run for **1–2 minutes** until results appear in PostgreSQL, then cancel from the Flink UI at http://localhost:8081.

---

### Question 4. Tumbling Window — Pickup Location

**Job file:** [`src/job/aggregation_job.py`](src/job/aggregation_job.py)

```bash
docker exec -it 07-streaming-jobmanager-1 flink run -py /opt/src/job/aggregation_job.py
```

This Flink job reads `green-trips` and applies a **5-minute tumbling window**, counting trips per `PULocationID`. Results are written to the `processed_events_aggregated` PostgreSQL table.

**Query:**

```sql
SELECT PULocationID, num_trips
FROM processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;
```

**Result:**

| PULocationID | num_trips |
|:------------:|:---------:|
| 74 | 61 |
| 74 | 58 |
| 74 | 58 |

Which `PULocationID` had the most trips in a single 5-minute window?

- [ ] 42
- [x] **74**
- [ ] 75
- [ ] 166

**Answer:** **74**

**Explanation:** The `aggregation_job.py` Flink job creates a Kafka source table (`events`) with an `event_timestamp` computed column derived from `lpep_pickup_datetime`, and a 5-second watermark tolerance. A `TUMBLE` table-valued function groups events into non-overlapping 5-minute windows. The `GROUP BY window_start, PULocationID` aggregation counts the number of trips per location per window. The `PULocationID` with the highest `num_trips` across all windows is the answer.

---

### Question 5. Session Window — Longest Streak

**Job file:** [`src/job/session_job.py`](src/job/session_job.py)

```bash
docker exec -it 07-streaming-jobmanager-1 flink run -py /opt/src/job/session_job.py
```

This Flink job uses a **session window with a 5-minute gap**, partitioned by `PULocationID`. A session groups events that arrive within 5 minutes of each other; when there is a gap of more than 5 minutes, the window closes.

**Query:**

```sql
SELECT PULocationID, window_start, window_end, num_trips
FROM processed_events_session
ORDER BY num_trips DESC
LIMIT 1;
```

**Result:**

| PULocationID | window_start | window_end | num_trips |
|:------------:|:------------:|:----------:|:---------:|
| 74 | 2025-10-08 06:46:14.000 | 2025-10-08 08:27:40.000 | 81 |

How many trips were in the longest session?

- [ ] 12
- [ ] 31
- [ ] 51
- [x] **81**

**Answer:** **81**

**Explanation:** Session windows are dynamic in length — unlike tumbling windows, their boundaries are defined by data activity. The `SESSION` table-valued function in Flink partitions the stream by `PULocationID` and merges consecutive events that are within 5 minutes of each other. A location with consistent trip activity throughout a block of time will accumulate a large `num_trips` count in a single session. The answer is the maximum `num_trips` value in the `processed_events_session` table.

---

### Question 6. Tumbling Window — Largest Tip

**Job file:** [`src/job/tumbling_tip_job.py`](src/job/tumbling_tip_job.py)

```bash
docker exec -it 07-streaming-jobmanager-1 flink run -py /opt/src/job/tumbling_tip_job.py
```

This Flink job applies a **1-hour tumbling window** and computes the total `tip_amount` across all locations per hour. Results are written to the `tip_per_hour` table.

**Query:**

```sql
SELECT window_start, window_end, total_tip
FROM tip_per_hour
ORDER BY total_tip DESC
LIMIT 1;
```

**Result:**

| window_start | window_end | total_tip |
|:------------:|:----------:|:---------:|
| 2025-10-16 18:00:00.000 | 2025-10-16 19:00:00.000 | 510.86 |

Which hour had the highest total tip amount?

- [ ] 2025-10-01 18:00:00
- [x] **2025-10-16 18:00:00**
- [ ] 2025-10-22 08:00:00
- [ ] 2025-10-30 16:00:00

**Answer:** **2025-10-16 18:00:00**

**Explanation:** The `tumbling_tip_job.py` Flink job creates a 1-hour `TUMBLE` window over the `event_timestamp` column and sums `tip_amount` across all `PULocationID`s for each window. The hour with the highest total tip value is found by ordering the `tip_per_hour` table by `total_tip DESC` and taking the top row. The `window_start` value of that row is the answer.

---

## Key Concepts

### 1. Redpanda as a Kafka Drop-in Replacement

Redpanda implements the Kafka protocol natively, meaning any Kafka client library (like `kafka-python`) works without modification. It is a single-binary deployment with no JVM dependency.

```bash
# Create a topic
docker exec -it 07-streaming-redpanda-1 rpk topic create green-trips

# List topics
docker exec -it 07-streaming-redpanda-1 rpk topic list

# Inspect topic
docker exec -it 07-streaming-redpanda-1 rpk topic describe green-trips
```

### 2. Kafka Producer with JSON Serialization

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
producer.send("green-trips", value={"key": "value"})
producer.flush()
```

### 3. PyFlink Tumbling Window (Table API / SQL)

```sql
-- 5-minute tumbling window grouped by pickup location
SELECT
    window_start,
    PULocationID,
    COUNT(*) AS num_trips
FROM TABLE(
    TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
)
GROUP BY window_start, PULocationID;
```

### 4. PyFlink Session Window

```sql
-- Session window with 5-minute gap per pickup location
SELECT
    window_start,
    window_end,
    PULocationID,
    COUNT(*) AS num_trips
FROM TABLE(
    SESSION(
        TABLE events PARTITION BY PULocationID,
        DESCRIPTOR(event_timestamp),
        INTERVAL '5' MINUTE
    )
)
GROUP BY window_start, window_end, PULocationID;
```

---

## Resources

| Resource                      | Link                                                                                           |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| Course GitHub                 | https://github.com/DataTalksClub/data-engineering-zoomcamp/                                   |
| Module 7 Workshop Materials   | https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming             |
| Homework submission form      | https://courses.datatalks.club/de-zoomcamp-2026/homework/hw7                                 |
| Redpanda documentation        | https://docs.redpanda.com/                                                                     |
| Apache Flink documentation    | https://nightlies.apache.org/flink/flink-docs-stable/                                         |
| PyFlink Table API             | https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/table/intro_to_table_api/ |
| Green Taxi dataset (Oct 2025) | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet                |
