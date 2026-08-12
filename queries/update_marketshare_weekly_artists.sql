-- Merge weekly artist marketshare aggregates into MARKETSHARE_WEEKLY_ARTISTS, updating matches on the composite PK or inserting new rows.
-- Grain is artist x country x week x is_current x distributor BU_ID path.
-- Distributor names are display-only (ANY_VALUE) so name variants with the same BU_IDs do not create duplicate MERGE source rows.
MERGE INTO current_dev.data.marketshare_weekly_artists AS tgt USING (
    WITH luminate_agg AS (
        SELECT
            f.week_ending_date,
            a.artist_id,
            MAX(a.artist_name) AS artist_name,
            f.country_code,
            f.is_current,
            SUM(f.streaming_total)      AS streaming_total,
            SUM(f.album_equivalent)     AS album_equivalent,
            SUM(f.product_sales)        AS product_sales,
            SUM(f.song_sale_equivalent) AS song_sale_equivalent,
            SUM(f.streaming_equivalent) AS streaming_equivalent,
            MAX(COALESCE(f.level_1_distributor, 'N/A')) AS level_1_distributor,
            COALESCE(f.level_1_distributor_bu_id, 'N/A') AS level_1_distributor_bu_id,
            MAX(COALESCE(f.level_2_distributor, 'N/A')) AS level_2_distributor,
            COALESCE(f.level_2_distributor_bu_id, 'N/A') AS level_2_distributor_bu_id,
            MAX(COALESCE(f.level_3_distributor, 'N/A')) AS level_3_distributor,
            COALESCE(f.level_3_distributor_bu_id, 'N/A') AS level_3_distributor_bu_id
        FROM
            current_dev.data.marketshare_weekly_albums f
            JOIN luminate_prod.extract_s.vw_artist_ds a ON f.primary_artist_id = a.artist_id -- NOTE: Will drop albums without an artist ID.
        WHERE
            f.week_ending_date < DATEADD(DAY, -2, CURRENT_DATE()) -- Ignore unfinished building data for the current week
        GROUP BY
            f.week_ending_date,
            a.artist_id,
            f.country_code,
            f.is_current,
            COALESCE(f.level_1_distributor_bu_id, 'N/A'),
            COALESCE(f.level_2_distributor_bu_id, 'N/A'),
            COALESCE(f.level_3_distributor_bu_id, 'N/A')
    )
    SELECT l.*
    FROM luminate_agg l
) AS src
    ON  tgt.artist_id              = src.artist_id
    AND tgt.country_code           = src.country_code
    AND tgt.week_ending_date       = src.week_ending_date
    AND tgt.is_current             = src.is_current
    AND tgt.level_1_distributor_bu_id IS NOT DISTINCT FROM src.level_1_distributor_bu_id
    AND tgt.level_2_distributor_bu_id IS NOT DISTINCT FROM src.level_2_distributor_bu_id
    AND tgt.level_3_distributor_bu_id IS NOT DISTINCT FROM src.level_3_distributor_bu_id
WHEN MATCHED THEN UPDATE SET
    tgt.artist_name              = src.artist_name,
    tgt.streaming_total          = src.streaming_total,
    tgt.album_equivalent         = src.album_equivalent,
    tgt.product_sales            = src.product_sales,
    tgt.song_sale_equivalent     = src.song_sale_equivalent,
    tgt.streaming_equivalent     = src.streaming_equivalent,
    tgt.level_1_distributor      = src.level_1_distributor,
    tgt.level_2_distributor      = src.level_2_distributor,
    tgt.level_3_distributor      = src.level_3_distributor
WHEN NOT MATCHED THEN INSERT (
    artist_id,
    artist_name,
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
    src.artist_id,
    src.artist_name,
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
