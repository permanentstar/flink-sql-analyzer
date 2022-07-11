INSERT INTO
    abc
SELECT
    window_start,
    window_end,
    adspaceId,
    avg(pressTime) AS avg_time
FROM TABLE(
    TUMBLE(TABLE table_source_3, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE))
GROUP BY
    window_start,
    window_end,
    adspaceId