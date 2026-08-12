-- Rebuild source once, prune orphans, then MERGE.
-- Snowflake does not support MERGE ... WHEN NOT MATCHED BY SOURCE.
CREATE OR REPLACE TEMPORARY TABLE tmp_marketshare_map_isrcs_source AS
SELECT
    mr.mr_id,
    mr.isrc,
    'US' AS country_code,
    -- Hardcoded U.S. only
    r.bu_id AS owner_bu_id,
    l.level_1_distributor,
    l.level_1_distributor_bu_id,
    l.level_2_distributor,
    l.level_2_distributor_bu_id,
    l.level_3_distributor,
    l.level_3_distributor_bu_id,
    mr.first_stream_date AS release_date,
    CASE
        WHEN DATEADD(MONTH, 18, mr.first_stream_date) >= CURRENT_DATE() THEN TRUE
        ELSE FALSE
    END AS is_current,
    SUM(r.share * 100) AS percent_owned -- Combine split label shares (e.g. AMG 10% then 5% later summed to 15%)
FROM
    luminate_prod_wmgonly.extract_s.vw_musical_right_ds r
    JOIN luminate_prod.extract_s.vw_musical_recording_ds mr ON mr.mr_id = r.entity_id
        AND r.entity_type = 'MR'
    LEFT JOIN current_dev.data.marketshare_map_label_hierarchy l ON l.bu_id = r.bu_id
WHERE
    r.right_type = 'VALID'
    AND (
        r.end_date IS NULL
        OR r.end_date > CURRENT_DATE()
    )
    AND r.bu_role = 'OWNER'
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
;

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM tmp_marketshare_map_isrcs_source AS source
    WHERE target.mr_id = source.mr_id
      AND target.isrc = source.isrc
      AND target.country_code = source.country_code
      AND target.owner_bu_id IS NOT DISTINCT FROM source.owner_bu_id
);

MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS AS target
USING tmp_marketshare_map_isrcs_source AS source
    ON target.mr_id = source.mr_id
    AND target.isrc = source.isrc
    AND target.country_code = source.country_code
    AND target.owner_bu_id IS NOT DISTINCT FROM source.owner_bu_id

WHEN MATCHED -- Update rows with changed metadata or hierarchy path
    AND (
        target.release_date IS DISTINCT FROM source.release_date
        OR target.is_current IS DISTINCT FROM source.is_current
        OR target.percent_owned IS DISTINCT FROM source.percent_owned
        OR target.level_1_distributor IS DISTINCT FROM source.level_1_distributor
        OR target.level_1_distributor_bu_id IS DISTINCT FROM source.level_1_distributor_bu_id
        OR target.level_2_distributor IS DISTINCT FROM source.level_2_distributor
        OR target.level_2_distributor_bu_id IS DISTINCT FROM source.level_2_distributor_bu_id
        OR target.level_3_distributor IS DISTINCT FROM source.level_3_distributor
        OR target.level_3_distributor_bu_id IS DISTINCT FROM source.level_3_distributor_bu_id
    ) THEN
    UPDATE SET
        target.release_date = source.release_date,
        target.is_current = source.is_current,
        target.percent_owned = source.percent_owned,
        target.level_1_distributor = source.level_1_distributor,
        target.level_1_distributor_bu_id = source.level_1_distributor_bu_id,
        target.level_2_distributor = source.level_2_distributor,
        target.level_2_distributor_bu_id = source.level_2_distributor_bu_id,
        target.level_3_distributor = source.level_3_distributor,
        target.level_3_distributor_bu_id = source.level_3_distributor_bu_id

WHEN NOT MATCHED THEN -- Create new rows for new songs / ownership
    INSERT (
        mr_id,
        isrc,
        country_code,
        owner_bu_id,
        level_1_distributor,
        level_1_distributor_bu_id,
        level_2_distributor,
        level_2_distributor_bu_id,
        level_3_distributor,
        level_3_distributor_bu_id,
        release_date,
        is_current,
        percent_owned
    )
    VALUES (
        source.mr_id,
        source.isrc,
        source.country_code,
        source.owner_bu_id,
        source.level_1_distributor,
        source.level_1_distributor_bu_id,
        source.level_2_distributor,
        source.level_2_distributor_bu_id,
        source.level_3_distributor,
        source.level_3_distributor_bu_id,
        source.release_date,
        source.is_current,
        source.percent_owned
    );
