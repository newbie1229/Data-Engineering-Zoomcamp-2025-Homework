# Data-Engineering-Zoomcamp-2025-Homework

## Question 1
```python
    import dlt
    print(dlt.__version__)
```
> The version is 1.13.0

## Question 2
```python
    import dlt
    from dlt.sources.helpers.rest_client import RESTClient 
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
    import duckdb

    @dlt.resource(name="taxi_rides_hw") # table name
    def ny_taxi():
        client = RESTClient(
            base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net",
            paginator = PageNumberPaginator(
                base_page = 1,
                total_path = None
            )
        )
        for page in client.paginate("data_engineering_zoomcamp_api"):
            yield page

    pipeline = dlt.pipeline(pipeline_name="ny_taxi_pipeline",destination="duckdb",dataset_name = "ny_taxi_data")
    load_info = pipeline.run(ny_taxi, write_disposition = "replace")
    # print(load_info)
    # print(pipeline.dataset(dataset_type="default").taxi_rides_hw.df().head())

    # connect to the database
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    # set search path to the dataset
    conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

    print(conn.sql("DESCRIBE").df())
```
![](q2.png)

> There are 4 tables created

## Question 3
```python
    df = pipeline.dataset(dataset_type = "default").taxi_rides_hw.df()
    print(len(df))
```
> There are 10000 records in total

## Question 4
```python
    with pipeline.sql_client() as client:
    res = client.execute_sql(
        """
        SELECT AVG(DATE_DIFF('minute', trip_pickup_date_time, trip_dropoff_date_time))
        FROM taxi_rides_hw;
        """
    )
    print(res)
```
> Average trip duration is 12.3049 minutes