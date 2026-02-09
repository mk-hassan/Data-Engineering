-- Question 1. Counting records
CREATE OR REPLACE EXTERNAL TABLE `nyc_yellow_taxi_trips.nyc_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dez-w3-26/yellow_tripdata_2024-*.parquet']
);


SELECT COUNT(*)
FROM nyc_yellow_taxi_trips.nyc_tripdata;

-- Question 2. Data read estimation
CREATE OR REPLACE TABLE `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
AS SELECT * FROM nyc_yellow_taxi_trips.nyc_tripdata;

SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata`;

SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;

-- Question 3. Understanding columnar storage
-- 155.12 MB
SELECT DISTINCT PULocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;

-- 310.24 MB
SELECT DISTINCT PULocationID, DOLocationID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;

-- Question 4. Counting zero fare trips
SELECT COUNT(1)
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
WHERE fare_amount = 0;

-- Question 5. Partitioning and clustering
CREATE OR REPLACE TABLE `nyc_yellow_taxi_trips.nyc_tripdata_partitioned_and_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID 
AS SELECT * FROM `nyc_yellow_taxi_trips.nyc_tripdata`;


-- Question 6. Partition benefits
-- 310.24 MB
SELECT DISTINCT VendorID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`
WHERE tpep_dropoff_datetime > '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';

-- 26.84 MB
SELECT DISTINCT VendorID
FROM `nyc_yellow_taxi_trips.nyc_tripdata_partitioned_and_clustered`
WHERE tpep_dropoff_datetime > '2024-03-01' AND tpep_dropoff_datetime <= '2024-03-15';


-- Question 9. Understanding table scans
SELECT COUNT(*)
FROM `nyc_yellow_taxi_trips.nyc_tripdata_nonpartitioned`;
