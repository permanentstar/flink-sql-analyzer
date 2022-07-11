insert into test_catalog.test_database.table_sink_1
SELECT
    row_num as topn,
    dspId,
    adspaceId,
    avg_time
FROM(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY dspId
            ORDER BY
                avg_time DESC
            ) AS row_num
    FROM
        (
            select
                dspId,
                adspaceId,
                avg(pressTime) AS avg_time
            from
                table_source_1
            GROUP BY
                dspId,
                adspaceId
        )
    )
WHERE
    row_num < 5