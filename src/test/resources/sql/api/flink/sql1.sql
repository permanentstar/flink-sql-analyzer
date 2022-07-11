USE CATALOG test_catalog
----------
USE test_catalog.default_database
----------
CREATE TEMPORARY TABLE datagen_table (
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
CREATE TEMPORARY TABLE blackhole_table (
   f_sequence INT,
   f_random INT,
   f_random_str STRING,
   ts TIMESTAMP(3)
) WITH (
    'connector' = 'blackhole'
)
----------
EXECUTE STATEMENT SET BEGIN
INSERT INTO blackhole_table
    SELECT * FROM datagen_table;
END