WITH stats AS (
SELECT
    origin AS faa_o
    ,dest AS faa_d
    ,count(*) AS total_flights
    ,count(DISTINCT tail_number) AS nunique_tail_nr
    ,count(DISTINCT airline) AS nunique_airline
    ,avg(actual_elapsed_time) AS avg_elapsed_time
    ,avg(arr_delay) AS avg_delay
    ,max(arr_delay) AS max_delay
    ,min(arr_delay) AS min_delay
    ,sum(cancelled) AS cancelled_tot
    ,sum(diverted) AS diverted_tot
FROM {{ref('prep_flights')}} pf
GROUP BY origin, dest
)
select
    s.faa_o AS origin
    ,pa_o.city AS origin_city
    ,pa_o.name AS origin_airport
    ,s.faa_d AS dest
    ,pa_d.city as dest_city
    ,pa_d.name AS dest_airport
    ,s.total_flights
    ,s.nunique_tail_nr
    ,s.nunique_airline
    ,s.avg_elapsed_time
    ,s.avg_delay
    ,s.max_delay
    ,s.min_delay
    ,s.cancelled_tot
    ,s.diverted_tot
FROM stats s
JOIN {{ref('prep_airports')}} pa_o
  ON faa_o = pa_o.faa
JOIN {{ref('prep_airports')}} pa_d
  ON faa_d = pa_d.faa
WHERE s.faa_o IN ('MSY', 'ATL', 'JFK') OR s.faa_d IN ('MSY', 'ATL', 'JFK')