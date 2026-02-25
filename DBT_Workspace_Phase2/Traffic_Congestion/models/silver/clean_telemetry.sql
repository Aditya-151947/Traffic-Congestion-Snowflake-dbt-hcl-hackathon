WITH flattened AS (
    SELECT
        -- keep raw sensor_id (mask using policy later)
        f.value:sensor_id::STRING AS sensor_id,

        f.value:location::STRING AS location,

        -- cast timestamp properly
        CAST(
            f.value:timestamp::STRING
            AS TIMESTAMP_NTZ
        ) AS event_time,

        -- clean avg speed (0 → NULL)
        CASE
            WHEN f.value:avg_speed_kmph::FLOAT = 0 THEN NULL
            ELSE f.value:avg_speed_kmph::FLOAT
        END AS avg_speed,

        
        COALESCE(
            f.value:pm25::FLOAT,
            0
        ) AS pm25_level,

        f.value:vehicle_count::INTEGER AS vehicle_count,

        
        ROW_NUMBER() OVER (
            PARTITION BY
                f.value:sensor_id::STRING,
                f.value:timestamp::STRING
            ORDER BY
                INGESTED_AT DESC
        ) AS rn

    FROM {{ source('BRONZE', 'IOT_RAW') }},
        LATERAL FLATTEN(input => JSON_CONTENT) f
)

SELECT
    sensor_id,
    location,
    event_time,
    avg_speed,
    pm25_level,
    vehicle_count
FROM flattened
WHERE rn = 1