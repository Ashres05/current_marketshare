MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS AS target USING (
    SELECT
        mr.mr_id,
        mr.isrc,
        'US' AS country_code,
        -- Hardcoded U.S. only
        l.level_1_distributor,
        l.level_2_distributor,
        l.level_3_distributor,
        mr.first_stream_date AS release_date,
        CASE
            WHEN DATEADD(MONTH, 18, mr.first_stream_date) >= CURRENT_DATE() THEN TRUE
            ELSE FALSE
        END AS is_current,
        SUM(r.share * 100) AS percent_owned -- Combine split label shares (i.e. AMG buys 10% then 5% later; gets summed to 15%)
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
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
) AS source ON target.mr_id = source.mr_id
AND target.isrc = source.isrc
AND target.level_1_distributor IS NOT DISTINCT
FROM
    source.level_1_distributor
    AND target.level_2_distributor IS NOT DISTINCT
FROM
    source.level_2_distributor
    AND target.level_3_distributor IS NOT DISTINCT
FROM
    source.level_3_distributor
    WHEN MATCHED -- Update rows with changed metadata
    AND (
        target.release_date IS DISTINCT
        FROM
            source.release_date
            OR target.is_current IS DISTINCT
        FROM
            source.is_current
            OR target.percent_owned IS DISTINCT
        FROM
            source.percent_owned
    ) THEN
UPDATE
SET
    target.release_date = source.release_date,
    target.is_current = source.is_current,
    target.percent_owned = source.percent_owned
    WHEN NOT MATCHED THEN -- Create new rows for new songs
INSERT
    (
        mr_id,
        isrc,
        country_code,
        level_1_distributor,
        level_2_distributor,
        level_3_distributor,
        release_date,
        is_current,
        percent_owned
    )
VALUES
    (
        source.mr_id,
        source.isrc,
        source.country_code,
        source.level_1_distributor,
        source.level_2_distributor,
        source.level_3_distributor,
        source.release_date,
        source.is_current,
        source.percent_owned
    );
