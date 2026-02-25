SELECT
    SENSOR_ID,
    EVENT_TIME,
    AVG_SPEED,

    AVG_SPEED -
    LAG(AVG_SPEED) OVER (
        PARTITION BY SENSOR_ID
        ORDER BY EVENT_TIME
    ) AS SPEED_DELTA

FROM {{ ref('clean_telemetry') }}