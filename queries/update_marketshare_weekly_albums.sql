-- Query for Marketshare Release historicals and marketshare
-- Each MRELG ID (non Multi Artist comilation) with over 10 weekly units is mapped to ONE label via COUNT() of the
-- musical products that belong to it and selecting the label that came up most frequently with MAX()
-- Streaming and song sales aggregated at the MR level, mapping a MR to a MRELG
-- Product sales aggregated at the MP level, mapping a MP to a MRELG
-- MP maps to MRELG and MR maps to MRELG are one-to-one, done so by selecting the MRELG with the highest pure sales this week, and if there is a tie, the oldest release date. Both ties force the pick of one using the smallest MRELG ID.
-- NOTE: This implementation does not deal with precise share percentages

MERGE INTO current_dev.data.marketshare_weekly_albums AS tgt
USING (
    WITH mrelg_filtered AS (
        SELECT
            m.mrelg_id
        FROM
            luminate_prod.extract_s.vw_musical_release_group_ds m
            JOIN luminate_prod.extract_s.vw_daily_fact_mrelg_summary_ds s ON s.mrelg_id = m.mrelg_id
            AND s.country_code = 'US'
            AND s.report_date BETWEEN DATEADD(DAY, -9, CURRENT_DATE())
            AND DATEADD(DAY, -2, CURRENT_DATE())
        WHERE
            m.compilation_type <> 'Multi Artist'
            AND m.release_type = 'Album'
        GROUP BY
            1
        HAVING
            SUM(s.equivalent_quantity) >= 10 -- Filter to only albums moving greater than 10 album equivalents this week for time
    ),
    mr_to_mrelg AS (
        SELECT
            DISTINCT m.mr_id,
            f.mrelg_id
        FROM
            luminate_prod.extract_s.vw_mr_mp_map_ds m
            JOIN luminate_prod.extract_s.vw_mp_mrel_map_ds mm ON mm.mp_id = m.mp_id
            JOIN luminate_prod.extract_s.vw_mrel_mrelg_map_ds mmm ON mmm.mrel_id = mm.mrel_id
            JOIN mrelg_filtered f ON f.mrelg_id = mmm.mrelg_id
    ),
    mrelg_us_product_fact AS (
        SELECT
            f.mrelg_id,
            COALESCE(SUM(s.quantity), 0) AS product_sales
        FROM
            mrelg_filtered f -- Left join in case there are no product sales
            LEFT JOIN luminate_prod.extract_s.vw_daily_fact_mrelg_summary_ds s ON s.mrelg_id = f.mrelg_id
            AND s.country_code = 'US'
            AND s.metric_category = 'ProductSales'
            AND s.report_date BETWEEN DATEADD(DAY, -9, CURRENT_DATE())
            AND DATEADD(DAY, -2, CURRENT_DATE())
        GROUP BY
            1
    ),
    mr_to_mrelg_distinct AS (
        -- Unique MR to MRELG map to prevent double counting.
        -- Ranks by product sales quantity first, then release date/first sale date second (if tie)
        SELECT
            DISTINCT m.mr_id,
            m.mrelg_id
        FROM
            mr_to_mrelg m
            JOIN luminate_prod.extract_s.vw_musical_release_group_ds mrelg ON mrelg.mrelg_id = m.mrelg_id -- Left join in case there are no product sales
            LEFT JOIN mrelg_us_product_fact pf ON pf.mrelg_id = m.mrelg_id QUALIFY ROW_NUMBER() OVER (
                PARTITION BY m.mr_id
                ORDER BY
                    pf.product_sales DESC,
                    COALESCE(
                        mrelg.first_sale_date,
                        mrelg.release_date,
                        '1900-01-01'
                    ) ASC,
                    mrelg.mrelg_id ASC
            ) = 1
    ),
    mp_to_mrelg AS (
        SELECT
            DISTINCT m.mp_id,
            f.mrelg_id
        FROM
            luminate_prod.extract_s.vw_mp_mrel_map_ds m
            JOIN luminate_prod.extract_s.vw_mrel_mrelg_map_ds mm ON mm.mrel_id = m.mrel_id
            JOIN mrelg_filtered f ON f.mrelg_id = mm.mrelg_id
    ),
    mp_to_mrelg_distinct AS (
        -- Unique MP to MRELG map to prevent double counting.
        -- Ranks by product sales quantity first, then release date/first sale date second (if tie)
        SELECT
            DISTINCT m.mp_id,
            m.mrelg_id
        FROM
            mp_to_mrelg m
            JOIN luminate_prod.extract_s.vw_musical_release_group_ds mrelg ON mrelg.mrelg_id = m.mrelg_id -- Left join in case there are no product sales
            LEFT JOIN mrelg_us_product_fact pf ON pf.mrelg_id = m.mrelg_id QUALIFY ROW_NUMBER() OVER (
                PARTITION BY m.mp_id
                ORDER BY
                    pf.product_sales DESC,
                    COALESCE(
                        mrelg.first_sale_date,
                        mrelg.release_date,
                        '1900-01-01'
                    ) ASC,
                    mrelg.mrelg_id ASC
            ) = 1
    ),
    mp_label_map AS (
        SELECT
            DISTINCT i.mp_id,
            COALESCE(i.level_1_distributor,    'N/A') AS level_1_distributor,
            COALESCE(i.level_1_distributor_bu_id, 'N/A') AS level_1_distributor_bu_id,
            COALESCE(i.level_2_distributor,    'N/A') AS level_2_distributor,
            COALESCE(i.level_2_distributor_bu_id, 'N/A') AS level_2_distributor_bu_id,
            COALESCE(i.level_3_distributor,    'N/A') AS level_3_distributor,
            COALESCE(i.level_3_distributor_bu_id, 'N/A') AS level_3_distributor_bu_id
            -- COALESCE() to remove nulls which will cause JOIN issues later in the pipeline
            -- IS_CURRENT flag not imported because the album release age and report date will be determining that
        FROM
            current_dev.data.marketshare_map_icpns i
        WHERE
            i.country_code = 'US'
    ),
    mrelg_label_map_pre_agg AS (
        SELECT
            mm.mrelg_id,
            m.level_1_distributor,
            m.level_1_distributor_bu_id,
            m.level_2_distributor,
            m.level_2_distributor_bu_id,
            m.level_3_distributor,
            m.level_3_distributor_bu_id,
            COUNT(DISTINCT m.mp_id) OVER (
                PARTITION BY mm.mrelg_id,
                m.level_1_distributor_bu_id,
                m.level_2_distributor_bu_id,
                m.level_3_distributor_bu_id
            ) AS id_count
        FROM
            mp_label_map m
            JOIN mp_to_mrelg_distinct mm ON mm.mp_id = m.mp_id
    ),
    mrelg_label_map AS (
        -- Pick the label (by BU_ID) that owns the most MPs for this release group
        SELECT
            mrelg_id,
            level_1_distributor,
            level_1_distributor_bu_id,
            level_2_distributor,
            level_2_distributor_bu_id,
            level_3_distributor,
            level_3_distributor_bu_id
        FROM
            mrelg_label_map_pre_agg QUALIFY ROW_NUMBER() OVER (
                PARTITION BY mrelg_id
                ORDER BY
                    id_count DESC
            ) = 1
    ),
    mr_fact AS (
        -- Recordings limited to streaming and song sale facts
        -- Facts hardcoded US
        -- Any type of equivalent is in album units (not singles)
        SELECT
            r.mr_id,
            s.country_code,
            da.week_end_date,
            SUM(
                IFF(
                    s.metric_category = 'Streams',
                    s.quantity,
                    0
                )
            ) AS total_streams,
            SUM(
                IFF(
                    s.metric_category = 'RecordingSales',
                    s.equivalent_quantity,
                    0
                )
            ) AS song_sale_equivalent,
            SUM(
                IFF(
                    s.metric_category = 'Streams',
                    s.equivalent_quantity,
                    0
                )
            ) AS streaming_equivalent,
            CASE
                WHEN DATEADD(
                    MONTH,
                    18,
                    COALESCE(
                        r.first_sale_date,
                        r.first_stream_date,
                        '1900-01-01' -- If first sale date and stream date are null, this will mark the release as catalog
                    )
                ) >= s.report_date THEN TRUE
                ELSE FALSE
            END AS is_current
        FROM
            luminate_prod.extract_s.vw_musical_recording_ds r
            JOIN luminate_prod.extract_s.vw_daily_fact_mr_summary_ds s ON s.mr_id = r.mr_id
            AND s.country_code = 'US'
            JOIN luminate_prod.extract_s.vw_date_ds da ON da.datename = s.report_date
            AND da.yearid >= 2024 -- Restrict to 2024 since that is the earlies marketshare data
            AND da.week_end_date >= DATE '{checkpoint_date}' -- Restrict to checkpoint date since that is the last updated date
            AND da.week_end_date < DATEADD(DAY, 2, CURRENT_DATE()) -- Ignore unfinished building data for the current week
        GROUP BY
            ALL
    ),
    mp_fact AS (
        -- Musical products limited to recording sale facts
        -- Facts hardcoded US
        -- Any type of equivalent is in album units (not singles)
        SELECT
            p.mp_id,
            s.country_code,
            da.week_end_date,
            SUM(
                IFF(
                    s.metric_category = 'ProductSales',
                    s.equivalent_quantity,
                    0
                )
            ) AS product_sales,
            CASE
                WHEN DATEADD(
                    MONTH,
                    18,
                    COALESCE(
                        p.first_sale_date,
                        p.release_date,
                        '1900-01-01' -- If first sale date and stream date are null, this will mark the release as catalog
                    )
                ) >= s.report_date THEN TRUE
                ELSE FALSE
            END AS is_current
        FROM
            luminate_prod.extract_s.vw_musical_product_ds p
            JOIN luminate_prod.extract_s.vw_daily_fact_mp_summary_ds s ON s.mp_id = p.mp_id
            AND s.country_code = 'US'
            JOIN luminate_prod.extract_s.vw_date_ds da ON da.datename = s.report_date
            AND da.yearid >= 2024 -- Restrict to 2024 since that is the earlies marketshare data
            AND da.week_end_date >= DATE '{checkpoint_date}' -- Restrict to checkpoint date since that is the last updated date
            AND da.week_end_date < DATEADD(DAY, 2, CURRENT_DATE()) -- Ignore unfinished building data for the current week
        GROUP BY
            ALL
    ),
    mrelg_fact_mp_agg AS (
        SELECT
            m.mrelg_id,
            f.country_code,
            f.week_end_date,
            ROUND(
                SUM(f.product_sales),
                0
            ) AS product_sales,
            f.is_current
        FROM
            mp_to_mrelg_distinct m
            JOIN mp_fact f ON f.mp_id = m.mp_id
        GROUP BY
            ALL
    ),
    mrelg_fact_mr_agg AS (
        SELECT
            m.mrelg_id,
            f.country_code,
            f.week_end_date,
            ROUND(
                SUM(f.total_streams),
                0
            ) AS total_streams,
            ROUND(
                SUM(f.song_sale_equivalent),
                0
            ) AS song_sale_equivalent,
            ROUND(
                SUM(f.streaming_equivalent),
                0
            ) AS streaming_equivalent,
            f.is_current
        FROM
            mr_to_mrelg_distinct m
            JOIN mr_fact f ON f.mr_id = m.mr_id
        GROUP BY
            ALL
    )
    SELECT
        p.mrelg_id,
        GET(m.artists, 0):ARTIST_ID::VARCHAR AS primary_artist_id,
        m.title,
        m.display_artist,
        COALESCE(m.first_sale_date, m.release_date, '1900-01-01') AS release_date,
        p.country_code,
        p.week_end_date AS week_ending_date,
        r.is_current,
        r.total_streams AS streaming_total,
        p.product_sales + r.song_sale_equivalent + r.streaming_equivalent AS album_equivalent,
        p.product_sales,
        r.song_sale_equivalent,
        r.streaming_equivalent,
        l.level_1_distributor,
        l.level_1_distributor_bu_id,
        l.level_2_distributor,
        l.level_2_distributor_bu_id,
        l.level_3_distributor,
        l.level_3_distributor_bu_id
    FROM
        mrelg_fact_mp_agg p
        JOIN mrelg_fact_mr_agg r ON r.mrelg_id = p.mrelg_id
        AND r.country_code = p.country_code
        AND r.week_end_date = p.week_end_date
        AND r.is_current = p.is_current
        JOIN mrelg_label_map l ON l.mrelg_id = r.mrelg_id
        JOIN luminate_prod.extract_s.vw_musical_release_group_ds m ON m.mrelg_id = p.mrelg_id
) AS src
ON  tgt.mrelg_id = src.mrelg_id
AND tgt.country_code = src.country_code
AND tgt.week_ending_date = src.week_ending_date
AND tgt.is_current = src.is_current
WHEN MATCHED THEN UPDATE SET
    tgt.primary_artist_id        = src.primary_artist_id,
    tgt.title                    = src.title,
    tgt.display_artist           = src.display_artist,
    tgt.release_date             = src.release_date,
    tgt.streaming_total          = src.streaming_total,
    tgt.album_equivalent         = src.album_equivalent,
    tgt.product_sales            = src.product_sales,
    tgt.song_sale_equivalent     = src.song_sale_equivalent,
    tgt.streaming_equivalent     = src.streaming_equivalent,
    tgt.level_1_distributor      = src.level_1_distributor,
    tgt.level_1_distributor_bu_id = src.level_1_distributor_bu_id,
    tgt.level_2_distributor      = src.level_2_distributor,
    tgt.level_2_distributor_bu_id = src.level_2_distributor_bu_id,
    tgt.level_3_distributor      = src.level_3_distributor,
    tgt.level_3_distributor_bu_id = src.level_3_distributor_bu_id
WHEN NOT MATCHED THEN INSERT (
    mrelg_id,
    primary_artist_id,
    title,
    display_artist,
    release_date,
    country_code,
    week_ending_date,
    is_current,
    streaming_total,
    album_equivalent,
    product_sales,
    song_sale_equivalent,
    streaming_equivalent,
    level_1_distributor,
    level_1_distributor_bu_id,
    level_2_distributor,
    level_2_distributor_bu_id,
    level_3_distributor,
    level_3_distributor_bu_id
) VALUES (
    src.mrelg_id,
    src.primary_artist_id,
    src.title,
    src.display_artist,
    src.release_date,
    src.country_code,
    src.week_ending_date,
    src.is_current,
    src.streaming_total,
    src.album_equivalent,
    src.product_sales,
    src.song_sale_equivalent,
    src.streaming_equivalent,
    src.level_1_distributor,
    src.level_1_distributor_bu_id,
    src.level_2_distributor,
    src.level_2_distributor_bu_id,
    src.level_3_distributor,
    src.level_3_distributor_bu_id
);
