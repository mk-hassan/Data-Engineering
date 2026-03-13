# Module 6: Batch Processing — Homework

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) |
**Module:** 06-batch |
**Dataset:** NYC Yellow Taxi Trips — November 2025 |
**Year:** 2026

---

## Overview

This module covers batch processing at scale using **Apache Spark** and **PySpark**. You will learn how to spin up a local Spark session, load and repartition large Parquet datasets, execute distributed DataFrame transformations, run SQL queries via Spark's Catalyst engine, and monitor job execution with the Spark UI. All exercises use the official NYC Yellow Taxi trip data for November 2025 (~4 million records).

### Key Learning Areas

- Installing and configuring PySpark locally with a SparkSession
- Reading and writing **Parquet** files with controlled partitioning
- Using **DataFrame API** (`filter`, `withColumn`, `orderBy`) for analytical queries
- Computing derived columns (trip duration) with `unix_timestamp` and `F.round`
- Joining large trip data with small lookup tables (broadcast-eligible)
- Running **Spark SQL** queries on registered temp views
- Monitoring Spark jobs and stages via the **Spark UI** (port 4040)
- Understanding the impact of `repartition()` on output file sizes

### Dataset Information

| Property | Value |
|---|---|
| Dataset | NYC Yellow Taxi Trip Records |
| Month | November 2025 |
| Source URL | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet` |
| Zone Lookup URL | `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` |
| Format | Parquet (snappy compressed) |
| Approx. Row Count | ~4 million |
| Output Partitions | 4 |

---

## Project Setup

1. **Install Java** — PySpark requires a JVM (Java 8 or 11 recommended):
   ```bash
   java -version
   ```

2. **Install PySpark** — Follow the official [setup guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/):
   ```bash
   pip install pyspark
   ```

3. **Download the dataset**:
   ```bash
   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet \
        -O data/yellow_tripdata_2025-11.parquet

   wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv \
        -O data/taxi_zone_lookup.csv
   ```

4. **Launch Jupyter Notebook**:
   ```bash
   jupyter notebook Homework.ipynb
   ```

5. **Or run in VS Code** — open `Homework.ipynb` and select the Python kernel that has PySpark installed.

---

## Project Structure

```
06-batch/
├── Homework.ipynb          # Main solution notebook
├── README.md               # This file
├── data/
│   ├── taxi_zone_lookup.csv
│   ├── yellow_tripdata_2025-11.parquet
│   └── yellow/
│       └── 2025/
│           └── 11/         # Repartitioned output (4 parquet files)
└── spark-warehouse/        # Spark metastore artifacts
```

### Pipeline / Session Metadata

| Property | Value |
|---|---|
| Spark App Name | `test` |
| Master | `local[*]` (all available cores) |
| Spark Version | `4.1.1` |
| Input Format | Parquet |
| Output Format | Parquet (snappy, 4 partitions) |
| Write Mode | `overwrite` |

---

## Homework Solutions

### Question 1: Install Spark and PySpark

> Install Spark, run PySpark, create a local Spark session, and execute `spark.version`. What's the output?

- [ ] 3.3.2
- [ ] 3.4.0
- [ ] 4.0.0
- [x] 4.1.1

**Answer:** `4.1.1`

```python
spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()

spark.version
```

| Result |
|--------|
| `'4.1.1'` |

**Explanation:** Running `spark.version` on the active `SparkSession` returns the installed Spark version string. The environment used in this solution has **Spark 4.1.1** installed. This is a recent release in the 4.x series, which introduced significant performance improvements via Project Tungsten and the Spark Connect API. Always verify your Spark version against the course requirements to ensure API compatibility.

---

### Question 2: Yellow November 2025

> Read the November 2025 Yellow Taxi data into a Spark DataFrame. Repartition it to 4 partitions and save it to Parquet. What is the **average size** of the Parquet files created (in MB)?

- [ ] 6MB
- [x] 25MB
- [ ] 75MB
- [ ] 100MB

**Answer:** `25MB`

```python
trips = spark.read.parquet('data/yellow_tripdata_2025-11.parquet')
trips.repartition(4).write.parquet('data/yellow/2025/11', mode='overwrite')
```

```bash
ls -lhF data/yellow/2025/11
```

| File | Size |
|------|------|
| `part-00000-...snappy.parquet` | **24M** |
| `part-00001-...snappy.parquet` | **24M** |
| `part-00002-...snappy.parquet` | **24M** |
| `part-00003-...snappy.parquet` | **24M** |
| `_SUCCESS` | 0B |

**Explanation:** After calling `repartition(4)`, Spark evenly distributes the data across exactly 4 output files. Each file is approximately **24 MB**, which rounds to the closest answer of **25 MB**. The files use Snappy compression by default, which balances compression ratio with fast decompression speed. The `_SUCCESS` marker file indicates a successful write and is not a data file. Repartitioning is a key tuning technique — too few partitions limits parallelism while too many creates excessive small-file overhead.

---

### Question 3: Count Records

> How many taxi trips were there on the **15th of November**? Consider only trips that started on November 15th.

- [ ] 62,610
- [ ] 102,340
- [x] 162,604
- [ ] 225,768

**Answer:** `162,604`

```python
trips.filter(
    (trips.tpep_pickup_datetime >= '2025-11-15') &
    (trips.tpep_pickup_datetime <  '2025-11-16')
).count()
```

| Result |
|--------|
| **162,604** |

**Explanation:** The filter uses a half-open interval `[2025-11-15, 2025-11-16)` to capture all trips with a pickup timestamp on November 15, 2025. Spark's Catalyst optimizer pushes this filter down to the Parquet scan level (predicate pushdown), making it efficient even on large datasets. The result of **162,604 trips** is a plausible single-day figure for NYC Yellow Taxi demand in November.

---

### Question 4: Longest Trip

> What is the length of the **longest trip** in the dataset, in hours?

- [ ] 22.7
- [ ] 58.2
- [x] 90.6
- [ ] 134.5

**Answer:** `90.6`

```python
trips = trips.withColumn(
    'trip_duration',
    F.round(
        (F.unix_timestamp(trips.tpep_dropoff_datetime) -
         F.unix_timestamp(trips.tpep_pickup_datetime)) / 3600,
        3
    )
)

trips \
    .select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration') \
    .orderBy('trip_duration', ascending=False) \
    .limit(5) \
    .show()
```

| tpep_pickup_datetime | tpep_dropoff_datetime | trip_duration |
|---|---|---|
| 2025-11-26 20:22:12 | 2025-11-30 15:01:00 | **90.647** |
| 2025-11-27 04:22:41 | 2025-11-30 09:19:35 | 76.948 |
| 2025-11-03 10:42:55 | 2025-11-06 14:55:45 | 76.214 |
| 2025-11-07 11:23:22 | 2025-11-10 08:40:41 | 69.289 |
| 2025-11-18 17:12:47 | 2025-11-21 12:17:37 | 67.081 |

**Explanation:** Trip duration in hours is computed by differencing Unix timestamps (seconds) of dropoff and pickup, then dividing by 3,600. The longest trip spans nearly **4 days** (pickup Nov 26 → dropoff Nov 30), yielding **90.647 hours**, which matches the option **90.6**. Trips this long almost certainly represent data quality issues — either meter forgot to close, or a test/corrupt record — rather than genuine passenger journeys of 90+ hours.

> **Note:** The top 5 longest trips all exceed 67 hours (nearly 3 days), which strongly suggests data quality anomalies (e.g., meters left running, test records). These outliers would need to be filtered in production pipelines.

---

### Question 5: User Interface

> Spark's User Interface which shows the application's dashboard runs on which local port?

- [ ] 80
- [ ] 443
- [x] 4040
- [ ] 8080

**Answer:** `4040`

**Explanation:** Spark automatically starts a Web UI for every running `SparkSession` or `SparkContext`, accessible at [http://localhost:4040](http://localhost:4040) by default. The UI shows active jobs, stages, tasks, executors, storage, and environment details — invaluable for performance debugging. If port 4040 is already in use, Spark will try 4041, 4042, etc. Port 8080 is typically the Spark **Master** Web UI in standalone cluster mode, not the application UI.

---

### Question 6: Least Frequent Pickup Location Zone

> Load the zone lookup data into a Spark temp view. Using the zone lookup and Yellow November 2025 data, what is the name of the **LEAST frequent pickup location Zone**?
>
> *If multiple answers are correct, select any.*

- [x] Governor's Island/Ellis Island/Liberty Island
- [ ] Arden Heights
- [ ] Rikers Island
- [ ] Jamaica Bay

**Answer:** `Governor's Island/Ellis Island/Liberty Island`

```python
zones = spark.read.csv('data/taxi_zone_lookup.csv', header=True, inferSchema=True)

trips.registerTempTable('trips')
zones.registerTempTable('zones')

spark.sql('''
    SELECT
        zones.Zone,
        trips_count.location_count
    FROM zones
    JOIN (
        SELECT
            COUNT(*) AS location_count,
            PULocationID AS pickup_location_id
        FROM trips
        GROUP BY PULocationID
    ) AS trips_count
      ON trips_count.pickup_location_id = zones.LocationId
    ORDER BY trips_count.location_count ASC
    LIMIT 5
''').head(5)
```

| Zone | location_count |
|------|---------------|
| Governor's Island/Ellis Island/Liberty Island | **1** |
| Eltingville/Annadale/Prince's Bay | **1** |
| Arden Heights | **1** |
| Port Richmond | 3 |
| Green-Wood Cemetery | 4 |

**Explanation:** The inner subquery aggregates trip counts by `PULocationID`, then joins with the zone lookup table on `LocationId` to resolve zone names. Sorting ascending by count reveals the least frequent zones. Both **Governor's Island/Ellis Island/Liberty Island** and **Arden Heights** tied at a count of **1** pickup each; the question accepts either. Governor's Island is a small island in New York Harbor with very limited vehicular access, making a single recorded taxi pickup plausible yet unusual. The inner join means zones with zero pickups are excluded from results.

> **Note:** Three zones share the minimum count of 1 pickup (`Governor's Island/Ellis Island/Liberty Island`, `Eltingville/Annadale/Prince's Bay`, and `Arden Heights`). All three are valid answers per the homework instructions.

---

## Key Concepts

### 1. Repartitioning vs. Coalescing

```python
# Repartition: full shuffle, creates exactly N equal-sized partitions
df.repartition(4).write.parquet('output/')

# Coalesce: avoids full shuffle, merges existing partitions (only reduces)
df.coalesce(4).write.parquet('output/')
```

Use `repartition()` when you need evenly distributed output files; use `coalesce()` when reducing partition count to avoid a full shuffle.

### 2. Predicate Pushdown with Parquet

```python
# Spark pushes the filter into the Parquet reader — only reads relevant row groups
trips.filter(
    (trips.tpep_pickup_datetime >= '2025-11-15') &
    (trips.tpep_pickup_datetime < '2025-11-16')
).count()
```

Parquet stores row group statistics (min/max per column), enabling Spark to skip entire row groups that cannot satisfy the filter — dramatically reducing I/O.

### 3. Deriving Duration with Unix Timestamps

```python
from pyspark.sql import functions as F

trips = trips.withColumn(
    'trip_duration_hours',
    F.round(
        (F.unix_timestamp('tpep_dropoff_datetime') -
         F.unix_timestamp('tpep_pickup_datetime')) / 3600,
        3
    )
)
```

`F.unix_timestamp()` converts a timestamp column to seconds since epoch. Subtracting and dividing by 3,600 gives duration in hours as a `DoubleType` column.

### 4. Spark SQL Temp Views

```python
trips.registerTempTable('trips')   # deprecated but functional
# Preferred modern API:
trips.createOrReplaceTempView('trips')

spark.sql('SELECT COUNT(*) FROM trips WHERE PULocationID = 1').show()
```

Temp views are session-scoped — they disappear when the `SparkSession` ends. They allow seamless mixing of the DataFrame API and SQL syntax within the same application.

---

## Resources

| Resource | Link |
|---|---|
| Course Repository | [DataTalksClub/data-engineering-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/) |
| Module 6 Setup Guide | [06-batch/setup](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/) |
| Homework Page | [cohorts/2026/06-batch/homework.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/06-batch/homework.md) |
| PySpark Documentation | [spark.apache.org/docs/latest/api/python](https://spark.apache.org/docs/latest/api/python/) |
| NYC TLC Trip Record Data | [nyc.gov/site/tlc/about/tlc-trip-record-data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Homework Submission Form | [de-zoomcamp-2026/homework/hw6](https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6) |
| Solution Notebook | [06-batch/Homework.ipynb](Homework.ipynb) |
