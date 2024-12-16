{{ config(materialized='view') }}

SELECT *
FROM {{ref('s_eneajorgo.flights_filtered')}}