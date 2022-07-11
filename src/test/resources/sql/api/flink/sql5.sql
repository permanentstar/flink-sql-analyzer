CREATE TEMPORARY TABLE datagen_table_1 (
   f_sequence INT,
   f_random INT,
   f_random_str STRING,
   ts AS localtimestamp,
   WATERMARK FOR ts AS ts
) WITH (
    'connector' = 'datagen',
    'rows-per-second'='5',
    'fields.f_sequence.kind'='sequence',
    'fields.f_sequence.start'='1',
    'fields.f_sequence.end'='10',
    'fields.f_random.min'='1',
    'fields.f_random.max'='10000',
    'fields.f_random_str.length'='10'
)
----------
WITH item AS (
    SELECT f_sequence, f_random
    FROM datagen_table_1
)
SELECT * FROM item