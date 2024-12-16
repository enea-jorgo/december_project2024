{{ config(materialized='view') }}

SELECT *
FROM s_eneajorgo.flights_filtered