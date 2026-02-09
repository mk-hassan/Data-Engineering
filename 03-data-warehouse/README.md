# Module 3: Data Warehousing & BigQuery - NYC Taxi Analysis

**Course:** [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)  
**Module:** 03 - Data Warehousing & BigQuery  
**Dataset:** Yellow Taxi Trip Records (January 2024 - June 2024)  
**Year:** 2026

---

## Overview

This module covers data warehousing concepts using Google BigQuery with NYC Yellow Taxi Trip Records. The focus areas include:

- Creating external tables from cloud storage (GCS)
- Building materialized tables in BigQuery
- Understanding columnar storage and query optimization
- Implementing partitioning and clustering strategies
- Analyzing query costs and data scanning patterns

### Dataset Information

- **Source:** [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Period:** January 2024 - June 2024 (6 months)
- **Format:** Parquet files
- **Location:** GCS bucket (`gs://dez-w3-26/yellow_tripdata_2024-*.parquet`)
- **Records:** 20M+ taxi trip records

---

## BigQuery Setup

### Tables Created

1. **External Table:** `nyc_yellow_taxi_trips.nyc_tripdata`
   - Reads directly from GCS Parquet files
   - No data duplication in BigQuery
   - Used for cost analysis queries

2. **Materialized Table:** `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
   - Data copied to BigQuery storage
   - Used as baseline for performance comparison
   - Regular (unpartitioned, unclustered) table

3. **Optimized Table:** `nyc_yellow_taxi_trips.nyc_tripdata_partitioned_and_clustered`
   - Partitioned by `tpep_dropoff_datetime`
   - Clustered by `VendorID`
   - Optimized for typical query patterns

---

## Homework Solutions

### Question 1: Counting Records

**Question:** What is count of records for the 2024 Yellow Taxi Data?

**Options:**
- [ ] 65,623
- [ ] 840,402
- [x] 20,332,093
- [ ] 85,431,289

**Answer:** 

**SQL Query:**
```sql
CREATE OR REPLACE EXTERNAL TABLE `nyc_yellow_taxi_trips.nyc_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dez-w3-26/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*)
FROM nyc_yellow_taxi_trips.nyc_tripdata;
```

**Explanation:**
- External table created pointing to all Parquet files matching the pattern
- COUNT(*) aggregates records across all 6 months of data
- This is a full table scan operation

---

### Question 2: Data Read Estimation

**Question:** Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

**Options:**
- [ ] 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- [x] 0 MB for the External Table and 155.12 MB for the Materialized Table
- [ ] 2.14 GB for the External Table and 0MB for the Materialized Table
- [ ] 0 MB for the External Table and 0MB for the Materialized Table

**Answer:** 

**SQL Queries:**
```sql
-- Query on External Table
SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata`;

-- Query on Materialized Table
SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;
```

**Explanation:**
- External tables don't charge for data scanned, showing 0 MB estimated
- Materialized tables scan the data stored in BigQuery
- BigQuery estimates bytes based on column statistics and compression
- The PULocationID column requires ~155 MB to scan in the materialized table

---

### Question 3: Understanding Columnar Storage

**Question:** Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

**Options:**
- [x] BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
- [ ] BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- [ ] BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- [ ] When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

**Answer:** 

**SQL Queries:**
```sql
-- Query 1: Single column (PULocationID)
-- Estimated: 155.12 MB
SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;

-- Query 2: Two columns (PULocationID, DOLocationID)
-- Estimated: 310.24 MB
SELECT DISTINCT PULocationID, DOLocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;
```

**Explanation:**
- BigQuery is a **columnar database** - it stores and retrieves data by columns, not rows
- When querying only PULocationID, BigQuery scans only that column's data (~155 MB)
- When querying two columns, BigQuery must scan both columns (~310 MB, roughly double)
- This is the key advantage of columnar storage: only necessary columns are read
- Row-based databases would read entire rows regardless of which columns are needed

---

### Question 4: Counting Zero Fare Trips

**Question:** How many records have a fare_amount of 0?

**Options:**
- [ ] 128,210
- [ ] 546,578
- [ ] 20,188,016
- [x] 8,333

**Answer:** 

**SQL Query:**
```sql
SELECT COUNT(1)
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
WHERE fare_amount = 0;
```

**Explanation:**
- Filters the materialized table for zero-fare trips
- Common in taxi data (e.g., test trips, system errors, or shared rides not charged to all passengers)
- Uses WHERE clause to reduce rows before counting
- Note: This is a full table scan since the table is not partitioned by fare_amount

---

### Question 5: Partitioning and Clustering

**Question:** What is the best strategy to make an optimized table in BigQuery if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID?

**Options:**
- [x] Partition by tpep_dropoff_datetime and Cluster on VendorID
- [ ] Cluster by tpep_dropoff_datetime and Cluster on VendorID
- [ ] Cluster on tpep_dropoff_datetime Partition by VendorID
- [ ] Partition by tpep_dropoff_datetime and Partition by VendorID

**Answer:** 

**SQL Query:**
```sql
CREATE OR REPLACE TABLE `nyc_yellow_taxi_trips.nyc_tripdata_partitioned_and_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID 
AS SELECT * FROM `nyc_yellow_taxi_trips.nyc_tripdata`;
```

**Explanation:**
- **Partitioning** (Primary): Used for frequently filtered columns (tpep_dropoff_datetime)
  - Enables partition pruning to skip irrelevant data
  - Significantly reduces data scanned
  - Charges apply based on data scanned
  
- **Clustering** (Secondary): Used for columns in ORDER BY or GROUP BY (VendorID)
  - Further optimizes within each partition
  - Improves query performance
  - No additional charges

- **Best Practice:** Filter columns → Partition | Sort/Group columns → Cluster

---

### Question 6: Partition Benefits

**Question:** Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Compare estimated bytes using the materialized table vs. the partitioned table. What are these values?

**Options:**
- [ ] 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- [x] 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
- [ ] 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- [ ] 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

**Answer:** 

**SQL Queries:**
```sql
-- Query on Non-Partitioned Table
-- Estimated: 310.24 MB (full table scan)
SELECT DISTINCT VendorID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
WHERE tpep_dropoff_datetime > '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

-- Query on Partitioned Table
-- Estimated: 26.84 MB (partition pruning)
SELECT DISTINCT VendorID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_partitioned_and_clustered`
WHERE tpep_dropoff_datetime > '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';
```

**Explanation:**
- **Non-partitioned table:** Must scan ALL 6 months of data → 310.24 MB
- **Partitioned table:** Only scans 15 days (March 1-15) → 26.84 MB
- **Performance Improvement:** ~91% reduction in data scanned! (310.24 MB → 26.84 MB)
- **Cost Savings:** Query on partitioned table costs ~11x less
- **Note:** Clustering by VendorID further optimizes the data retrieval within the partition

---

### Question 7: External Table Storage

**Question:** Where is the data stored in the External Table you created?

**Options:**
- [ ] BigQuery
- [ ] Container Registry
- [x] GCP Bucket
- [ ] BigTable

**Answer:** 

**Explanation:**
- External tables are **metadata pointers** to data stored elsewhere
- In this case, data resides in **GCS Bucket** (gs://dez-w3-26/yellow_tripdata_2024-*.parquet)
- BigQuery doesn't copy or move the data
- External tables:
  - Don't store data in BigQuery (no storage costs)
  - May have query costs depending on your BigQuery pricing tier
  - Are useful for exploratory analysis before loading to BigQuery
  - Support lazy loading and quick data federation

---

### Question 8: Clustering Best Practices

**Question:** It is best practice in BigQuery to always cluster your data: True or False?

**Options:**
- [ ] True
- [x] False

**Answer:** 

**Explanation:**
- **False** - Clustering is NOT always best practice
- When to cluster:
  - Your queries frequently filter/group by specific columns
  - Table is large (> 1 GB)
  - You need optimized query performance
  - Column has high cardinality but limited range queries
  
- When NOT to cluster:
  - Small tables (< 1 GB) - overhead not worth it
  - No consistent query patterns
  - Queries don't filter/group by the same columns
  - Random access patterns across all values

- **Best Practice:** Profile your queries first, then optimize based on actual usage patterns

---

### Question 9: Understanding Table Scans (No Points)

**Question:** Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

**SQL Query:**
```sql
SELECT COUNT(*)
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;
```

**Explanation:**
- **Estimated Bytes:** 0 MB (or very minimal)
- **Why:** BigQuery optimizes COUNT(*) aggregates on unfiltered tables
  - COUNT(*) doesn't need to read actual data values
  - BigQuery stores row count metadata
  - Uses pre-computed statistics instead of scanning
  - This is a significant optimization for simple count operations
  - However, COUNT(*) with WHERE clause requires full table scan

---

## Project Structure

```
03-data-warehouse/
├── README.md                    # This file - homework solutions
├── bigquery-hw.sql              # All SQL queries for homework
├── load_yellow_taxi_data.py     # Python script to load data to GCS
├── DLT_upload_to_GCP.ipynb      # Jupyter notebook (DLT method)
└── queries/
    └── (Additional analytical queries)
```

---

## Key Concepts

### Columnar Storage
- BigQuery stores data by column, not by row
- Only requested columns are scanned during queries
- Significant cost and performance savings for wide tables

### Partitioning vs. Clustering

| Aspect | Partitioning | Clustering |
|--------|--------------|-----------|
| **Purpose** | Filter data efficiently | Sort within partitions |
| **Cost** | Reduces data scanned (cost savings) | Secondary optimization |
| **Best For** | Date ranges, categories | GROUP BY, ORDER BY columns |
| **Cardinality** | Low to medium | High cardinality columns |

### External vs. Materialized Tables

| Aspect | External | Materialized |
|--------|----------|--------------|
| **Storage** | GCS/Cloud Storage | BigQuery |
| **Query Cost** | Typically free tier* | Per byte scanned |
| **Performance** | Slower (remote read) | Faster (local) |
| **Use Case** | Exploration, federation | Analysis, joins |

---

## SQL Query Files

All homework queries are documented in [bigquery-hw.sql](./bigquery-hw.sql) with:
- External table creation
- Materialized table creation
- Partitioned and clustered table creation
- All question queries with comments

---

## Learning Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
- [Columnar Storage Explained](https://en.wikipedia.org/wiki/Column-oriented_DBMS)
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [GCP Regions](https://docs.cloud.google.com/storage/docs/locations)
---

## Submission

- **Form:** https://courses.datatalks.club/de-zoomcamp-2026/homework/hw3
- **Status:** ✅ Completed
- **Repository:** This GitHub repository
- **Solutions Included:** SQL queries in README + bigquery-hw.sql

---

**Last Updated:** February 9, 2026  
**Status:** Active & Maintained
