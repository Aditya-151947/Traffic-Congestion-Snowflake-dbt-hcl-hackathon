SELECT *
FROM (
    SELECT
        LOCATION,
        EVENT_TIME,
        AVG_SPEED,

        COUNT(*) OVER (
            PARTITION BY LOCATION
            ORDER BY EVENT_TIME
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS LOW_SPEED_COUNT

    FROM {{ ref('clean_telemetry') }}
    WHERE AVG_SPEED < 10
)
WHERE LOW_SPEED_COUNT >= 3