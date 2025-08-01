SELECT DISTINCT EXTRACT(MONTH FROM pickup_datetime) AS month
FROM {{ ref('dim_fhv_trips') }}
ORDER BY month