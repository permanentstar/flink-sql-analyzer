CREATE TABLE MyTable (
     `user_id` BIGINT,
     `name` STRING,
     `timestamp` TIMESTAMP_LTZ(3) METADATA
) WITH (
    'connector' = 'kafka'
)