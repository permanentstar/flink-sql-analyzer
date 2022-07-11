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
                dwd_kfk_shbt2_test3_deal_behave_0401
            GROUP BY
                dspId,
                adspaceId
        )
    )
WHERE
      row_num < 5