WITH get_trip_duration AS (
    SELECT dropoff_zones, pickup_zones, dropoff_location_id, pickup_location_id, pickup_datetime,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
    PERCENTILE_CONT(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 0.90) OVER(PARTITION BY year, month, pickup_location_id, dropoff_location_id) AS p90_trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    JOIN {{ ref('dim_year') }}
    ON EXTRACT(YEAR FROM pickup_datetime) = dim_year.year
    JOIN {{ ref('dim_month') }}
    ON EXTRACT(MONTH FROM pickup_datetime) = dim_month.month
    WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019 AND EXTRACT(MONTH FROM pickup_datetime) = 11
    AND pickup_zones IN ('Newark Airport', 'SoHo', 'Yorkville East')
),
get_ranking AS (
    SELECT pickup_zones, dropoff_zones, p90_trip_duration,
    DENSE_RANK() OVER(PARTITION BY pickup_zones ORDER BY p90_trip_duration DESC) AS dr
    FROM get_trip_duration

)

SELECT DISTINCT pickup_zones, dropoff_zones, p90_trip_duration
FROM get_ranking
WHERE dr=2
ORDER BY pickup_zones
