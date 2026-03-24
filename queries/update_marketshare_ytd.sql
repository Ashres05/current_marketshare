MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_YTD AS target
USING (
    WITH marketshare_volume AS (
        SELECT
            w.week_ending_date,
            da.yearid AS year,
            da.weeknum AS week_num,
            w.country_code,
            w.release_age,
            w.label_name,
            SUM(w.streaming_total) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS streaming_total,
            SUM(w.album_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS album_equivalent,
            SUM(w.product_sales) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS product_sales,
            SUM(w.song_sale_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS song_sale_equivalent,
            SUM(w.streaming_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS streaming_equivalent
        FROM current_dev.data.marketshare_weekly w
        JOIN luminate_prod.extract_s.vw_date_ds da ON da.datename = w.week_ending_date
    ),
    
    total_universe AS (
        SELECT
            w.week_ending_date,
            da.yearid AS year,
            da.weeknum AS week_num,
            w.country_code,
            w.release_age,
            SUM(w.streaming_total) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS streaming_total,
            SUM(w.album_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS album_equivalent,
            SUM(w.product_sales) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS product_sales,
            SUM(w.song_sale_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS song_sale_equivalent,
            SUM(w.streaming_equivalent) OVER (
                PARTITION BY da.yearid, w.country_code, w.release_age, w.label_name 
                ORDER BY da.weeknum
            ) AS streaming_equivalent
        FROM current_dev.data.marketshare_weekly w
        JOIN luminate_prod.extract_s.vw_date_ds da ON da.datename = w.week_ending_date
        WHERE
            w.label_name = 'Total Universe'
    )
    
    SELECT
        v.week_ending_date,
        v.year,
        v.week_num,
        v.country_code,
        v.release_age,
        v.label_name,
        COALESCE(v.streaming_total, 0) AS streaming_total,
        COALESCE(v.album_equivalent, 0) AS album_equivalent,
        GREATEST(COALESCE(v.product_sales, 0), 0) AS product_sales,
        COALESCE(v.song_sale_equivalent, 0) AS song_sale_equivalent,
        COALESCE(v.streaming_equivalent, 0) AS streaming_equivalent,
        COALESCE(ROUND((v.album_equivalent / NULLIF(u.album_equivalent, 0)) * 100, 4), 0) AS album_equivalent_share,
        COALESCE(ROUND((v.product_sales / NULLIF(u.product_sales, 0)) * 100, 4), 0) AS product_sales_share,
        COALESCE(ROUND((v.song_sale_equivalent / NULLIF(u.song_sale_equivalent, 0)) * 100, 4), 0) AS song_sale_equivalent_share,
        COALESCE(ROUND((v.streaming_equivalent / NULLIF(u.streaming_equivalent, 0)) * 100, 4), 0) AS streaming_equivalent_share
    FROM marketshare_volume v
        LEFT JOIN total_universe u ON u.week_ending_date = v.week_ending_date
        AND u.country_code = v.country_code
        AND u.release_age = v.release_age
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY v.week_ending_date, v.country_code, v.release_age, v.label_name 
        ORDER BY v.week_ending_date 
    ) = 1

) AS source
    ON target.week_ending_date = source.week_ending_date
    AND target.country_code = source.country_code
    AND target.release_age = source.release_age
    AND target.label_name = source.label_name
    AND target.year = source.year
    AND target.week_num = source.week_num

WHEN MATCHED AND (
    target.streaming_total != source.streaming_total
    OR target.album_equivalent != source.album_equivalent
    OR target.product_sales != source.product_sales
    OR target.song_sale_equivalent != source.song_sale_equivalent
    OR target.streaming_equivalent != source.streaming_equivalent
    OR target.album_equivalent_share != source.album_equivalent_share
    OR target.product_sales_share != source.product_sales_share
    OR target.song_sale_equivalent_share != source.song_sale_equivalent_share
    OR target.streaming_equivalent_share != source.streaming_equivalent_share
) THEN
    UPDATE SET
        target.album_equivalent = source.album_equivalent,
        target.album_equivalent_share = source.album_equivalent_share,
        target.product_sales = source.product_sales,
        target.product_sales_share = source.product_sales_share,
        target.song_sale_equivalent = source.song_sale_equivalent,
        target.song_sale_equivalent_share = source.song_sale_equivalent_share,
        target.streaming_equivalent = source.streaming_equivalent,
        target.streaming_equivalent_share = source.streaming_equivalent_share,
        target.streaming_total = source.streaming_total

WHEN NOT MATCHED THEN
    INSERT (
        album_equivalent,
        album_equivalent_share,
        country_code,
        label_name,
        product_sales,
        product_sales_share,
        release_age,
        song_sale_equivalent,
        song_sale_equivalent_share,
        streaming_equivalent,
        streaming_equivalent_share,
        streaming_total,
        week_ending_date,
        week_num,
        year
    )
    VALUES (
        source.album_equivalent,
        source.album_equivalent_share,
        source.country_code,
        source.label_name,
        source.product_sales,
        source.product_sales_share,
        source.release_age,
        source.song_sale_equivalent,
        source.song_sale_equivalent_share,
        source.streaming_equivalent,
        source.streaming_equivalent_share,
        source.streaming_total,
        source.week_ending_date,
        source.week_num,
        source.year
    );
