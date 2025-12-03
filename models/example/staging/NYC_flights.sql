WITH flights_union AS (

    -- departures
    SELECT
        origin AS airport_code,
        dest AS connection_code,
        cancelled,
        diverted
    FROM {{ ref('nyc_flights') }}

    UNION ALL

    -- arrivals
    SELECT
        dest AS airport_code,
        origin AS connection_code,
        cancelled,
        diverted
    FROM {{ ref('nyc_flights') }}

)

SELECT
    airport_code,
    COUNT(DISTINCT connection_code) AS unique_connections,
    COUNT(*) AS flight_count,
    SUM(cancelled) AS cancelled_total,
    SUM(diverted) AS diverted_total
FROM flights_union
GROUP BY airport_code
