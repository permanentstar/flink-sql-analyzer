INSERT INTO
    default_catalog.default_database.table_sink_2
SELECT
    rowTime,
    info.exchangeBidId AS bidId,
    info.impressionInfo.dspShowInfo.dspId AS dspId,
    info.impressionInfo.dspShowInfo.impressionId AS impressionId,
    info.impressionInfo.dspShowInfo.adspaceSlot AS adspaceSlot,
    info.impressionInfo.dspShowInfo.dealInfo.dealType AS dealType,
    info.impressionInfo.dspShowInfo.dealInfo.dealId AS dealId,
    info.impressionInfo.adSlotInfo.adspaceId AS adspaceId,
    (userBehavior.mouseReleaseTime - userBehavior.mousePressTime) AS pressTime
FROM
    default_catalog.default_database.table_source_2
WHERE
    userBehavior.mouseReleaseTime - userBehavior.mousePressTime BETWEEN 50 AND 2000
  AND dealType IS NOT NULL