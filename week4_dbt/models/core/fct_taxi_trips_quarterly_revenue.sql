{{
    config(
        materialized='table'
    )
}}


WITH sub_1 AS (
    SELECT
        CASE
            WHEN EXTRACT(MONTH FROM pickup_datetime) BETWEEN 1 AND 3 THEN '1'
            WHEN EXTRACT(MONTH FROM pickup_datetime) BETWEEN 4 AND 6 THEN '2'
            WHEN EXTRACT(MONTH FROM pickup_datetime) BETWEEN 7 AND 9 THEN '3'
            ELSE '4'
        END AS quarter,
        EXTRACT(YEAR FROM pickup_datetime) AS year_cal,
        service_type,
        ROUND(SUM(total_amount), 2) AS revenue
    FROM {{ ref('fact_trips') }}
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020)
    GROUP BY 1,2,3
),

get_green_yoy AS (
    {{ get_yoy_growth('quarter', 'year_cal','revenue','service_type','Green','sub_1')}}
),

get_yellow_yoy AS (
    {{ get_yoy_growth('quarter', 'year_cal','revenue','service_type','Yellow','sub_1')}}
)

SELECT g.quarter, g.year_cal, g.yoy_growth AS green_yoy_growth, y.yoy_growth AS yellow_yoy_growth
FROM get_green_yoy AS g
JOIN get_yellow_yoy AS y
USING (quarter)
WHERE g.yoy_growth IS NOT NULL AND y.yoy_growth IS NOT NULL
ORDER BY g.quarter, g.year_cal