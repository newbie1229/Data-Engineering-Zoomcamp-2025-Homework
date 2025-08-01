SELECT DISTINCT service_type,
EXTRACT(YEAR FROM pickup_datetime) AS year_cal,
EXTRACT(MONTH FROM pickup_datetime) AS month_cal,
PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) AS p97,
PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) AS p95,
PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) AS p90
FROM {{ ref('fact_trips') }}
WHERE fare_amount>0 AND trip_distance > 0 AND payment_type_description IN ('Cash', 'Credit card')
AND EXTRACT(YEAR FROM pickup_datetime)=2020 AND EXTRACT(MONTH FROM pickup_datetime) = 4