USE CATALOG test_catalog
----------
DROP FUNCTION IF EXISTS default_database.addNum
----------
CREATE FUNCTION default_database.addNum as 'com.daasyyds.flink.sql.analyzer.function.TestUDF'
----------
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
SELECT addNum(f_sequence, f_random) FROM datagen_table_1
----------
DROP TEMPORARY TABLE default_database.datagen_table_1
----------
DROP FUNCTION default_database.addNum
----------


USE CATALOG default_catalog
----------
DROP TEMPORARY SYSTEM FUNCTION IF EXISTS addNum
----------
CREATE TEMPORARY SYSTEM FUNCTION addNum as 'com.daasyyds.flink.sql.analyzer.function.TestUDF'
----------
DROP TEMPORARY TABLE IF EXISTS default_database.datagen_table_2
----------
CREATE TEMPORARY TABLE datagen_table_2 (
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
SELECT addNum(f_sequence, f_random) FROM datagen_table_2
----------
CREATE VIEW datagen_view AS SELECT * FROM datagen_table_2
----------
SELECT * FROM datagen_view
----------
DROP TEMPORARY SYSTEM FUNCTION addNum
