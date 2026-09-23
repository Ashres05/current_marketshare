-- Weekly artists from US MR/MP daily facts x current ICPN/ISRC maps (prorated ownership).
-- Grain is artist x country x week x is_current x L1/L2/L3 path BU_IDs.
-- OWNER_BU_ID is an attribute only, not MERGE identity (out of scope vs other fact tables).
-- Not an albums rollup. Checkpointed fact MERGE then display-only name remap from current maps.
-- Do not orphan-delete against the checkpointed fact source (that would wipe pre-checkpoint history).
-- Snowflake does not support MERGE ... WHEN NOT MATCHED BY SOURCE.

-- Collapse extra rows that share the path PK (e.g. leftover owner-grain rows) so MERGE can match 1:1.
CREATE OR REPLACE TEMPORARY TABLE tmp_artist_path_dup_delete AS
SELECT
    artist_id,
    country_code,
    week_ending_date,
    is_current,
    owner_bu_id,
    level_1_distributor_bu_id,
    level_2_distributor_bu_id,
    level_3_distributor_bu_id,
    streaming_total,
    album_equivalent,
    product_sales,
    song_sale_equivalent,
    streaming_equivalent
FROM current_dev.data.marketshare_weekly_artists
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        artist_id,
        country_code,
        week_ending_date,
        is_current,
        level_1_distributor_bu_id,
        level_2_distributor_bu_id,
        level_3_distributor_bu_id
    ORDER BY album_equivalent DESC NULLS LAST, streaming_total DESC NULLS LAST
) > 1
;

DELETE FROM current_dev.data.marketshare_weekly_artists AS a
USING tmp_artist_path_dup_delete AS d
WHERE a.artist_id = d.artist_id
  AND a.country_code = d.country_code
  AND a.week_ending_date = d.week_ending_date
  AND a.is_current = d.is_current
  AND a.owner_bu_id IS NOT DISTINCT FROM d.owner_bu_id
  AND a.level_1_distributor_bu_id IS NOT DISTINCT FROM d.level_1_distributor_bu_id
  AND a.level_2_distributor_bu_id IS NOT DISTINCT FROM d.level_2_distributor_bu_id
  AND a.level_3_distributor_bu_id IS NOT DISTINCT FROM d.level_3_distributor_bu_id
  AND a.streaming_total IS NOT DISTINCT FROM d.streaming_total
  AND a.album_equivalent IS NOT DISTINCT FROM d.album_equivalent
  AND a.product_sales IS NOT DISTINCT FROM d.product_sales
  AND a.song_sale_equivalent IS NOT DISTINCT FROM d.song_sale_equivalent
  AND a.streaming_equivalent IS NOT DISTINCT FROM d.streaming_equivalent
;

MERGE INTO current_dev.data.marketshare_weekly_artists AS tgt
USING (
    WITH mr_to_artist AS (
        SELECT DISTINCT
            GET(mr.artists, 0):ARTIST_ID::TEXT AS primary_artist_id,
            mr.mr_id
        FROM luminate_prod.extract_s.vw_musical_recording_ds mr
        WHERE GET(mr.artists, 0):ARTIST_ID::TEXT IS NOT NULL
    ),
    mp_to_artist AS (
        SELECT DISTINCT
            GET(mp.artists, 0):ARTIST_ID::TEXT AS primary_artist_id,
            mp.mp_id
        FROM luminate_prod.extract_s.vw_musical_product_ds mp
        WHERE GET(mp.artists, 0):ARTIST_ID::TEXT IS NOT NULL
    ),
    isrc_owner_map AS (
        SELECT
            mr_id,
            country_code,
            owner_bu_id,
            MAX(percent_owned) / 100.0 AS ownership_share,
            MAX(COALESCE(level_1_distributor, 'N/A')) AS level_1_distributor,
            MAX(COALESCE(level_1_distributor_bu_id, 'N/A')) AS level_1_distributor_bu_id,
            MAX(COALESCE(level_2_distributor, 'N/A')) AS level_2_distributor,
            MAX(COALESCE(level_2_distributor_bu_id, 'N/A')) AS level_2_distributor_bu_id,
            MAX(COALESCE(level_3_distributor, 'N/A')) AS level_3_distributor,
            MAX(COALESCE(level_3_distributor_bu_id, 'N/A')) AS level_3_distributor_bu_id
        FROM current_dev.data.marketshare_map_isrcs
        WHERE owner_bu_id IS NOT NULL
          AND country_code = 'US'
        GROUP BY mr_id, country_code, owner_bu_id
    ),
    icpn_owner_map AS (
        SELECT
            mp_id,
            country_code,
            owner_bu_id,
            MAX(percent_owned) / 100.0 AS ownership_share,
            MAX(COALESCE(level_1_distributor, 'N/A')) AS level_1_distributor,
            MAX(COALESCE(level_1_distributor_bu_id, 'N/A')) AS level_1_distributor_bu_id,
            MAX(COALESCE(level_2_distributor, 'N/A')) AS level_2_distributor,
            MAX(COALESCE(level_2_distributor_bu_id, 'N/A')) AS level_2_distributor_bu_id,
            MAX(COALESCE(level_3_distributor, 'N/A')) AS level_3_distributor,
            MAX(COALESCE(level_3_distributor_bu_id, 'N/A')) AS level_3_distributor_bu_id
        FROM current_dev.data.marketshare_map_icpns
        WHERE owner_bu_id IS NOT NULL
          AND country_code = 'US'
        GROUP BY mp_id, country_code, owner_bu_id
    ),
    mr_fact AS (
        SELECT
            r.mr_id,
            s.country_code,
            da.week_end_date,
            SUM(IFF(s.metric_category = 'Streams', s.quantity, 0)) AS total_streams,
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
            DATEADD(
                MONTH,
                18,
                COALESCE(
                    r.first_sale_date,
                    r.first_stream_date,
                    '1900-01-01'
                )
            ) >= s.report_date AS is_current
        FROM
            luminate_prod.extract_s.vw_musical_recording_ds r
            JOIN luminate_prod.extract_s.vw_daily_fact_mr_summary_ds s
                ON s.mr_id = r.mr_id
                AND s.country_code = 'US'
            JOIN luminate_prod.extract_s.vw_date_ds da
                ON da.datename = s.report_date
                AND da.yearid >= 2024
                AND da.week_end_date >= DATE '{checkpoint_date}'
                AND da.week_end_date < DATEADD(DAY, -2, CURRENT_DATE())
        GROUP BY ALL
    ),
    mp_fact AS (
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
            DATEADD(
                MONTH,
                18,
                COALESCE(
                    p.first_sale_date,
                    p.release_date,
                    '1900-01-01'
                )
            ) >= s.report_date AS is_current
        FROM
            luminate_prod.extract_s.vw_musical_product_ds p
            JOIN luminate_prod.extract_s.vw_daily_fact_mp_summary_ds s
                ON s.mp_id = p.mp_id
                AND s.country_code = 'US'
            JOIN luminate_prod.extract_s.vw_date_ds da
                ON da.datename = s.report_date
                AND da.yearid >= 2024
                AND da.week_end_date >= DATE '{checkpoint_date}'
                AND da.week_end_date < DATEADD(DAY, -2, CURRENT_DATE())
        GROUP BY ALL
    ),
    artist_fact_mp_agg AS (
        SELECT
            m.primary_artist_id,
            f.country_code,
            f.week_end_date,
            f.is_current,
            i.owner_bu_id,
            ROUND(SUM(f.product_sales * i.ownership_share), 0) AS product_sales,
            MAX(i.level_1_distributor) AS level_1_distributor,
            MAX(i.level_1_distributor_bu_id) AS level_1_distributor_bu_id,
            MAX(i.level_2_distributor) AS level_2_distributor,
            MAX(i.level_2_distributor_bu_id) AS level_2_distributor_bu_id,
            MAX(i.level_3_distributor) AS level_3_distributor,
            MAX(i.level_3_distributor_bu_id) AS level_3_distributor_bu_id
        FROM
            mp_to_artist m
            JOIN mp_fact f ON f.mp_id = m.mp_id
            JOIN icpn_owner_map i
                ON i.mp_id = f.mp_id
                AND i.country_code = f.country_code
        GROUP BY
            m.primary_artist_id,
            f.country_code,
            f.week_end_date,
            f.is_current,
            i.owner_bu_id
    ),
    artist_fact_mr_agg AS (
        SELECT
            m.primary_artist_id,
            f.country_code,
            f.week_end_date,
            f.is_current,
            i.owner_bu_id,
            ROUND(SUM(f.total_streams * i.ownership_share), 0) AS total_streams,
            ROUND(SUM(f.song_sale_equivalent * i.ownership_share), 0)
                AS song_sale_equivalent,
            ROUND(SUM(f.streaming_equivalent * i.ownership_share), 0)
                AS streaming_equivalent,
            MAX(i.level_1_distributor) AS level_1_distributor,
            MAX(i.level_1_distributor_bu_id) AS level_1_distributor_bu_id,
            MAX(i.level_2_distributor) AS level_2_distributor,
            MAX(i.level_2_distributor_bu_id) AS level_2_distributor_bu_id,
            MAX(i.level_3_distributor) AS level_3_distributor,
            MAX(i.level_3_distributor_bu_id) AS level_3_distributor_bu_id
        FROM
            mr_to_artist m
            JOIN mr_fact f ON f.mr_id = m.mr_id
            JOIN isrc_owner_map i
                ON i.mr_id = f.mr_id
                AND i.country_code = f.country_code
        GROUP BY
            m.primary_artist_id,
            f.country_code,
            f.week_end_date,
            f.is_current,
            i.owner_bu_id
    ),
    combined AS (
        SELECT
            COALESCE(p.primary_artist_id, r.primary_artist_id) AS artist_id,
            COALESCE(p.country_code, r.country_code) AS country_code,
            COALESCE(p.week_end_date, r.week_end_date) AS week_ending_date,
            COALESCE(p.is_current, r.is_current) AS is_current,
            COALESCE(p.owner_bu_id, r.owner_bu_id) AS owner_bu_id,
            COALESCE(r.total_streams, 0) AS streaming_total,
            COALESCE(p.product_sales, 0) AS product_sales,
            COALESCE(r.song_sale_equivalent, 0) AS song_sale_equivalent,
            COALESCE(r.streaming_equivalent, 0) AS streaming_equivalent,
            COALESCE(p.level_1_distributor, r.level_1_distributor, 'N/A')
                AS level_1_distributor,
            COALESCE(p.level_1_distributor_bu_id, r.level_1_distributor_bu_id, 'N/A')
                AS level_1_distributor_bu_id,
            COALESCE(p.level_2_distributor, r.level_2_distributor, 'N/A')
                AS level_2_distributor,
            COALESCE(p.level_2_distributor_bu_id, r.level_2_distributor_bu_id, 'N/A')
                AS level_2_distributor_bu_id,
            COALESCE(p.level_3_distributor, r.level_3_distributor, 'N/A')
                AS level_3_distributor,
            COALESCE(p.level_3_distributor_bu_id, r.level_3_distributor_bu_id, 'N/A')
                AS level_3_distributor_bu_id
        FROM artist_fact_mp_agg p
        FULL OUTER JOIN artist_fact_mr_agg r
            ON p.primary_artist_id = r.primary_artist_id
            AND p.country_code = r.country_code
            AND p.week_end_date = r.week_end_date
            AND p.is_current = r.is_current
            AND p.owner_bu_id = r.owner_bu_id
    ),
    path_grain AS (
        SELECT
            c.artist_id,
            c.country_code,
            c.week_ending_date,
            c.is_current,
            MAX(c.owner_bu_id) AS owner_bu_id,
            ROUND(SUM(c.streaming_total), 0) AS streaming_total,
            ROUND(SUM(c.product_sales), 0) AS product_sales,
            ROUND(SUM(c.song_sale_equivalent), 0) AS song_sale_equivalent,
            ROUND(SUM(c.streaming_equivalent), 0) AS streaming_equivalent,
            MAX(c.level_1_distributor) AS level_1_distributor,
            c.level_1_distributor_bu_id,
            MAX(c.level_2_distributor) AS level_2_distributor,
            c.level_2_distributor_bu_id,
            MAX(c.level_3_distributor) AS level_3_distributor,
            c.level_3_distributor_bu_id
        FROM combined c
        GROUP BY
            c.artist_id,
            c.country_code,
            c.week_ending_date,
            c.is_current,
            c.level_1_distributor_bu_id,
            c.level_2_distributor_bu_id,
            c.level_3_distributor_bu_id
    ),
    artists_named AS (
        SELECT
            a.artist_id,
            MAX(a.artist_name) AS artist_name
        FROM luminate_prod.extract_s.vw_artist_ds a
        GROUP BY a.artist_id
    )
    SELECT
        g.artist_id,
        n.artist_name,
        g.owner_bu_id,
        g.is_current,
        g.country_code,
        g.week_ending_date,
        g.streaming_total,
        g.product_sales + g.song_sale_equivalent + g.streaming_equivalent
            AS album_equivalent,
        g.product_sales,
        g.song_sale_equivalent,
        g.streaming_equivalent,
        g.level_1_distributor,
        g.level_1_distributor_bu_id,
        g.level_2_distributor,
        g.level_2_distributor_bu_id,
        g.level_3_distributor,
        g.level_3_distributor_bu_id
    FROM path_grain g
    JOIN artists_named n
        ON n.artist_id = g.artist_id
) AS src
ON tgt.artist_id = src.artist_id
AND tgt.country_code = src.country_code
AND tgt.week_ending_date = src.week_ending_date
AND tgt.is_current = src.is_current
AND tgt.level_1_distributor_bu_id IS NOT DISTINCT FROM src.level_1_distributor_bu_id
AND tgt.level_2_distributor_bu_id IS NOT DISTINCT FROM src.level_2_distributor_bu_id
AND tgt.level_3_distributor_bu_id IS NOT DISTINCT FROM src.level_3_distributor_bu_id
WHEN MATCHED THEN UPDATE SET
    tgt.artist_name = src.artist_name,
    tgt.owner_bu_id = src.owner_bu_id,
    tgt.streaming_total = src.streaming_total,
    tgt.album_equivalent = src.album_equivalent,
    tgt.product_sales = src.product_sales,
    tgt.song_sale_equivalent = src.song_sale_equivalent,
    tgt.streaming_equivalent = src.streaming_equivalent,
    tgt.level_1_distributor = src.level_1_distributor,
    tgt.level_2_distributor = src.level_2_distributor,
    tgt.level_3_distributor = src.level_3_distributor
WHEN NOT MATCHED THEN INSERT (
    artist_id,
    artist_name,
    country_code,
    week_ending_date,
    is_current,
    owner_bu_id,
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
    src.artist_id,
    src.artist_name,
    src.country_code,
    src.week_ending_date,
    src.is_current,
    src.owner_bu_id,
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

-- Display-only remap: refresh L1/L2/L3 names from current maps on the existing path BU_IDs.
-- Do not rewrite path BU_ID key columns (that would mint a new PK). Owner-path identity is future work.
MERGE INTO current_dev.data.marketshare_weekly_artists AS tgt
USING (
    SELECT
        level_1_distributor_bu_id,
        level_2_distributor_bu_id,
        level_3_distributor_bu_id,
        MAX(level_1_distributor) AS level_1_distributor,
        MAX(level_2_distributor) AS level_2_distributor,
        MAX(level_3_distributor) AS level_3_distributor
    FROM (
        SELECT
            COALESCE(level_1_distributor, 'N/A') AS level_1_distributor,
            COALESCE(level_1_distributor_bu_id, 'N/A') AS level_1_distributor_bu_id,
            COALESCE(level_2_distributor, 'N/A') AS level_2_distributor,
            COALESCE(level_2_distributor_bu_id, 'N/A') AS level_2_distributor_bu_id,
            COALESCE(level_3_distributor, 'N/A') AS level_3_distributor,
            COALESCE(level_3_distributor_bu_id, 'N/A') AS level_3_distributor_bu_id
        FROM current_dev.data.marketshare_map_icpns
        WHERE country_code = 'US'
        UNION ALL
        SELECT
            COALESCE(level_1_distributor, 'N/A') AS level_1_distributor,
            COALESCE(level_1_distributor_bu_id, 'N/A') AS level_1_distributor_bu_id,
            COALESCE(level_2_distributor, 'N/A') AS level_2_distributor,
            COALESCE(level_2_distributor_bu_id, 'N/A') AS level_2_distributor_bu_id,
            COALESCE(level_3_distributor, 'N/A') AS level_3_distributor,
            COALESCE(level_3_distributor_bu_id, 'N/A') AS level_3_distributor_bu_id
        FROM current_dev.data.marketshare_map_isrcs
        WHERE country_code = 'US'
    )
    GROUP BY
        level_1_distributor_bu_id,
        level_2_distributor_bu_id,
        level_3_distributor_bu_id
) AS src
ON tgt.level_1_distributor_bu_id IS NOT DISTINCT FROM src.level_1_distributor_bu_id
AND tgt.level_2_distributor_bu_id IS NOT DISTINCT FROM src.level_2_distributor_bu_id
AND tgt.level_3_distributor_bu_id IS NOT DISTINCT FROM src.level_3_distributor_bu_id
WHEN MATCHED AND (
    tgt.level_1_distributor IS DISTINCT FROM src.level_1_distributor
    OR tgt.level_2_distributor IS DISTINCT FROM src.level_2_distributor
    OR tgt.level_3_distributor IS DISTINCT FROM src.level_3_distributor
) THEN UPDATE SET
    tgt.level_1_distributor = src.level_1_distributor,
    tgt.level_2_distributor = src.level_2_distributor,
    tgt.level_3_distributor = src.level_3_distributor
;

-- Collapse extra rows that share the path PK (duplicate-only, never empties the table).
CREATE OR REPLACE TEMPORARY TABLE tmp_artist_path_dup_delete_post AS
SELECT
    artist_id,
    country_code,
    week_ending_date,
    is_current,
    owner_bu_id,
    level_1_distributor_bu_id,
    level_2_distributor_bu_id,
    level_3_distributor_bu_id,
    streaming_total,
    album_equivalent,
    product_sales,
    song_sale_equivalent,
    streaming_equivalent
FROM current_dev.data.marketshare_weekly_artists
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        artist_id,
        country_code,
        week_ending_date,
        is_current,
        level_1_distributor_bu_id,
        level_2_distributor_bu_id,
        level_3_distributor_bu_id
    ORDER BY album_equivalent DESC NULLS LAST, streaming_total DESC NULLS LAST
) > 1
;

DELETE FROM current_dev.data.marketshare_weekly_artists AS a
USING tmp_artist_path_dup_delete_post AS d
WHERE a.artist_id = d.artist_id
  AND a.country_code = d.country_code
  AND a.week_ending_date = d.week_ending_date
  AND a.is_current = d.is_current
  AND a.owner_bu_id IS NOT DISTINCT FROM d.owner_bu_id
  AND a.level_1_distributor_bu_id IS NOT DISTINCT FROM d.level_1_distributor_bu_id
  AND a.level_2_distributor_bu_id IS NOT DISTINCT FROM d.level_2_distributor_bu_id
  AND a.level_3_distributor_bu_id IS NOT DISTINCT FROM d.level_3_distributor_bu_id
  AND a.streaming_total IS NOT DISTINCT FROM d.streaming_total
  AND a.album_equivalent IS NOT DISTINCT FROM d.album_equivalent
  AND a.product_sales IS NOT DISTINCT FROM d.product_sales
  AND a.song_sale_equivalent IS NOT DISTINCT FROM d.song_sale_equivalent
  AND a.streaming_equivalent IS NOT DISTINCT FROM d.streaming_equivalent
;
