CREATE TABLE `dwd_user_behave` (
   `rowtime` TIMESTAMP(3),
   `bidId` STRING,
   `dspId` INT,
   `impressionId` BIGINT,
   `dealType` STRING,
   `dealId` BIGINT,
   `adspaceId` INT,
   `pressTime` BIGINT,
   WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '5' MINUTE)
WITH (
    'connector'='kafka',
    'format'='json',
    'properties.group.id'='test_group',
    'properties.security.protocol'='SASL_PLAINTEXT',
    'properties.sasl.mechanism'='SCRAM-SHA-256',
    'properties.sasl.jaas.config'='org.apache.kafka.common.security.scram.ScramLoginModule required username="name" password="passwd";',
    'topic'='user_behave',
    'properties.bootstrap.servers'='localhost:9092')
----------
CREATE TABLE `dwa_behave_agg` (
    `topn` BIGINT,
    `dspId` INT,
    `adspaceId` INT,
    `avg_time` BIGINT,
    PRIMARY KEY (dspId,adspaceId) NOT ENFORCED)
WITH (
    'connector'='jdbc',
    'url'='jdbc:mysql://localhost:3301/db_demo',
    'table-name'='behave_agg')
----------
INSERT INTO default_catalog.default_database.dwa_behave_agg
SELECT
    row_num AS topn,
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
                dwd_user_behave
            GROUP BY
                dspId,
                adspaceId
        )
    )
WHERE
    row_num < 5