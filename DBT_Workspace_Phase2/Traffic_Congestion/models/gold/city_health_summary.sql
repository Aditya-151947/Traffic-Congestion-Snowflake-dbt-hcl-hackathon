SELECT
    location,

    DATE_TRUNC('HOUR', event_time) AS reporting_hour,

    AVG(avg_speed) AS hourly_avg_speed,

    MAX(pm25_level) AS peak_pm25,

    CASE
        WHEN AVG(avg_speed) < 15
         AND MAX(pm25_level) > 100
        THEN 'CRITICAL'
        ELSE 'NORMAL'
    END AS traffic_status

FROM {{ ref('clean_telemetry') }}
GROUP BY
    location,
    DATE_TRUNC('HOUR', event_time)