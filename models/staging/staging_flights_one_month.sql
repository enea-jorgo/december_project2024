{{ config(materialized='view') }}

WITH flights_one_month AS (
    SELECT *
    FROM {{ref('s_eneajorgo.flights_filtered')}}
)
SELECT * FROM flights_one_month