EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN
SELECT
    `count`,
    word
FROM
    MyTable1
WHERE
    word LIKE 'F%'
UNION ALL
SELECT
    `count`,
    word
FROM
    MyT0able2
----------
EXPLAIN INSERT INTO a.b.c
SELECT cnt FROM
(
    SELECT count(*) as cnt, key FROM d.e.f GROUP BY key
)
----------
SHOW TABLES
----------
EXPLAIN STATEMENT SET BEGIN
INSERT INTO students
VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END
----------
EXPLAIN PLAN FOR STATEMENT SET BEGIN
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
INSERT INTO students
    VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
END