# Data-Engineering-Zoomcamp-2025-Homework

## Question 1
```docker
    FROM python:3.12.8
    ENTRYPOINT [ "bash" ]
```

```bash
    docker build -t homework_q1:m1 .
    docker run -it homework_q1:m1
    python
    import pip
    pip.__version__
```
-> The answer is 24.3.1

## Question 2
    
The hostname is db and the port is 5432, the answer is db:5432

## Question 3
> Prepare postgres

```bash
docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="homework_m1" \
-v "F:\Data Courses\Data Engineering Zoomcamp\Module 1\Homework\Question 3 Trip Segmentation Count\mount_folder":/var/lib/postgresql/data \
# Create a new empty folder on Windows to mount 
-p 5434:5432 \
postgres:13
```

``` yaml
services: 
    pgdatabase:
        image: postgres:13
        environment:
        - POSTGRES_USER=root 
        - POSTGRES_PASSWORD=root 
        - POSTGRES_DB=homework_m1
        volumes:
        - "F:/Data Courses/Data Engineering Zoomcamp/Module 1/Homework/Question 3 Trip Segmentation Count/mount_folder:/var/lib/postgresql/data:rw"
        ports:
        - "5434:5432"

    pgadmin:
        image: dpage/pgadmin4
        environment:
        - PGADMIN_DEFAULT_EMAIL=admin@admin.com
        - PGADMIN_DEFAULT_PASSWORD=root 
        ports:
        - "8081:80"
```

```bash
docker-compose up
```

```python
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("postgresql://root:root@localhost:5434/homework_m1")
engine.connect()
green_df_iter = pd.read_csv("green_tripdata_2019-10.csv", na_values=["N"], low_memory=False, iterator=True, chunksize=100000) # Turn off warning for mixed types
green_df = next(green_df_iter)
green_df["lpep_pickup_datetime"] = pd.to_datetime(green_df["lpep_pickup_datetime"])
green_df["lpep_dropoff_datetime"] = pd.to_datetime(green_df["lpep_dropoff_datetime"])

green_df.head(0).to_sql("green_tripdata", con=engine, if_exists="replace")
green_df.to_sql("green_tripdata", con=engine, if_exists="append")

# Push green trip data to the database
while True:
    try:
        green_df = next(green_df_iter)
        green_df["lpep_pickup_datetime"] = pd.to_datetime(green_df["lpep_pickup_datetime"])
        green_df["lpep_dropoff_datetime"] = pd.to_datetime(green_df["lpep_dropoff_datetime"])
        green_df.to_sql("green_tripdata", con=engine, if_exists="append")
        print("Inserted another chunk of green trip data")
    except StopIteration:
        print("Done inserting all chunks.")
        break


# Pushing the zones table
zones = pd.read_csv("taxi_zone_lookup.csv", na_values=["NV","Unknown","N/A"], low_memory=False)
zones.to_sql(name="zones", con=engine, if_exists="replace")
```
> SQL Queries

```sql
/* Trips up to 1 mile */
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00' AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
AND trip_distance <=1
-- 104802
```

```sql
/* In between 1 (exclusive) and 3 miles (inclusive) */
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00' AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
AND 1< trip_distance AND trip_distance <=3; 
-- 198924
```

```sql
/* In between 3 (exclusive) and 7 miles (inclusive) */
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00' AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
AND 3< trip_distance AND trip_distance <=7
-- 109603
```

```sql
/* In between 7 (exclusive) and 10 miles (inclusive) */
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00' AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
AND 7< trip_distance AND trip_distance <=10
-- 27678
```

```sql
/* Over 10 miles */
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01 00:00:00' AND lpep_dropoff_datetime < '2019-11-01 00:00:00'
AND trip_distance >10
-- 35189
```

## Question 4
```sql
SELECT CAST(lpep_pickup_datetime AS DATE), MAX(trip_distance)
FROM green_tripdata
WHERE CAST(lpep_pickup_datetime AS DATE) IN ('2019-10-11','2019-10-24','2019-10-26','2019-10-31')
GROUP BY 1
ORDER BY 2 DESC 
LIMIT 1
-- "2019-10-31"
```
The answer is 2019-10-31

## Question 5
```sql
SELECT z."Zone"
FROM green_tripdata g
JOIN zones z ON g."PULocationID" = z."LocationID"
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-10-18' 
GROUP BY 1
HAVING SUM(total_amount) > 13000
ORDER BY SUM(total_amount) DESC
LIMIT 3
```
![](q5.png)

## Question 6
```sql
SELECT z."Zone"
FROM green_tripdata g
JOIN zones z ON g."DOLocationID" = z."LocationID"
WHERE g."PULocationID" =74
AND CAST(g.lpep_pickup_datetime AS DATE) BETWEEN '2019-10-01' AND '2019-10-31'
ORDER BY g.tip_amount DESC
LIMIT 1
-- "JFK Airport"
```
The answer is JFK Airport

## Question 7
The answer is
```bash 
terraform init
terraform apply -auto-approve
terraform destroy
```

