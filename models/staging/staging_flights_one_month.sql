{{ config(materialized='view') }}

WITH flights_one_month AS (
    SELECT *
    FROM {{ref('flights_filtered')}}
)
SELECT * FROM flights_one_month