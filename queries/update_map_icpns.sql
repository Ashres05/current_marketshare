-- Rebuild source once, prune orphans, then MERGE.
-- Snowflake does not support MERGE ... WHEN NOT MATCHED BY SOURCE.
-- Grain is mp_id x icpn x country x owner_bu_id x right window (adjacent windows stay separate).
CREATE OR REPLACE TEMPORARY TABLE tmp_marketshare_map_icpns_source AS
SELECT
    mp.mp_id,
    mp.external_ids:ICPN [0]::VARCHAR AS icpn,
    'US' AS country_code,
    -- Hardcoded U.S. only
    r.bu_id AS owner_bu_id,
    l.level_1_distributor,
    l.level_1_distributor_bu_id,
    l.level_2_distributor,
    l.level_2_distributor_bu_id,
    l.level_3_distributor,
    l.level_3_distributor_bu_id,
    mp.release_date,
    CASE
        WHEN DATEADD(MONTH, 18, mp.release_date) >= CURRENT_DATE() THEN TRUE
        ELSE FALSE
    END AS is_current,
    SUM(r.share * 100) AS percent_owned, -- Combine split label shares inside each window
    r.start_date AS right_start_date,
    r.end_date AS right_end_date
FROM
    luminate_prod_wmgonly.extract_s.vw_musical_right_ds r
    JOIN luminate_prod.extract_s.vw_musical_product_ds mp ON mp.mp_id = r.entity_id
        AND r.entity_type = 'MP'
    LEFT JOIN current_dev.data.marketshare_map_label_hierarchy l ON l.bu_id = r.bu_id
WHERE
    r.right_type = 'VALID'
    AND r.bu_role = 'OWNER'
    AND r.start_date <= CURRENT_DATE()
    AND (
        r.end_date IS NULL
        OR r.end_date >= DATE '2023-12-29'
    )
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15
;

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM tmp_marketshare_map_icpns_source AS source
    WHERE target.mp_id = source.mp_id
      AND target.icpn = source.icpn
      AND target.country_code = source.country_code
      AND target.owner_bu_id IS NOT DISTINCT FROM source.owner_bu_id
      AND target.right_start_date IS NOT DISTINCT FROM source.right_start_date
      AND target.right_end_date IS NOT DISTINCT FROM source.right_end_date
);

MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS AS target
USING tmp_marketshare_map_icpns_source AS source
    ON target.mp_id = source.mp_id
    AND target.icpn = source.icpn
    AND target.country_code = source.country_code
    AND target.owner_bu_id IS NOT DISTINCT FROM source.owner_bu_id
    AND target.right_start_date IS NOT DISTINCT FROM source.right_start_date
    AND target.right_end_date IS NOT DISTINCT FROM source.right_end_date

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

WHEN NOT MATCHED THEN -- Create new rows for new songs / ownership windows
    INSERT (
        mp_id,
        icpn,
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
        percent_owned,
        right_start_date,
        right_end_date
    )
    VALUES (
        source.mp_id,
        source.icpn,
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
        source.percent_owned,
        source.right_start_date,
        source.right_end_date
    );
