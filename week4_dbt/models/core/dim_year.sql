SELECT DISTINCT EXTRACT(YEAR FROM pickup_datetime) AS year
FROM {{ ref('dim_fhv_trips') }}
ORDER BY year