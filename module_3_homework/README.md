# Data-Engineering-Zoomcamp-2025-Homework

## BigQuery setup

>[Load script](load_script.py)


> Create external table
```sql
CREATE OR REPLACE EXTERNAL TABLE module_3_hw.external_yellow_tripdata
OPTIONS(
  format = 'PARQUET',
  uris = ['gs://dezc_md3_hw/yellow_tripdata_2024-*.parquet']
)
```

> Create physical table
```sql
CREATE OR REPLACE TABLE module_3_hw.yellow_tripdata AS
SELECT * FROM module_3_hw.external_yellow_tripdata
```

## Question 1
```sql
SELECT COUNT(*)
FROM module_3_hw.yellow_tripdata
```
> There are 20,332,093 rows for the 2024 Yellow Taxi Data

## Question 2
```sql
-- count on the external table
SELECT COUNT(DISTINCT(PULocationID))
FROM module_3_hw.external_yellow_tripdata

-- count on the physical table
SELECT COUNT(DISTINCT(PULocationID))
FROM module_3_hw.yellow_tripdata
```
> The ```COUNT``` query will consume 0B for the External Table and 155.12 MB for the Physical Table

## Question 3
> BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

## Quesion 4
```sql
SELECT COUNT(*)
FROM module_3_hw.yellow_tripdata
WHERE fare_amount = 0
```
> There are 8333 records with fare_amount of 0

## Question 5
> Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
CREATE OR REPLACE TABLE module_3_hw.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS (
  SELECT *
  FROM module_3_hw.yellow_tripdata
)
```

## Question 6
```sql
-- query on physical table
SELECT DISTINCT(VendorID)
FROM module_3_hw.yellow_tripdata
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- query on partitioned and clustered table
SELECT DISTINCT(VendorID)
FROM module_3_hw.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' AND '2024-03-15'
```
> 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

## Question 7
> The data in the External Table is stored in GCP Bucket

## Question 8
> False, we don't need to cluster data on small data (less than 1GB)

## Quesion 9
```sql
SELECT COUNT(*)
FROM module_3_hw.yellow_tripdata
```
> This query consumes 0B since the number of rows is in the metadata and BigQuery doesn't have to read all the table.