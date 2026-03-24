MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_FORECASTS AS target
USING (
    WITH dates AS (
        SELECT
            DISTINCT da.week_end_date AS week_ending_date,
            da.weeknum AS week_num,
            da.yearid AS year
        FROM
            luminate_prod.extract_s.vw_date_ds da
        WHERE
            da.yearid = (
                SELECT MAX(year)
                FROM current_dev.data.marketshare_ytd
            )
    ),
    building_marketshare AS (
        SELECT
            *,
            FALSE AS forecast
        FROM
            current_dev.data.marketshare_ytd y
    ),
    weekly_avg AS (
        SELECT
            w.country_code,
            w.label_name,
            w.release_age,
            AVG(w.streaming_total) OVER (
                PARTITION BY w.country_code, w.label_name, w.release_age
                ORDER BY w.week_ending_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            ) AS streaming_total,
            AVG(w.product_sales) OVER (
                PARTITION BY w.country_code, w.label_name, w.release_age
                ORDER BY w.week_ending_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            ) AS product_sales,
            AVG(w.song_sale_equivalent) OVER (
                PARTITION BY w.country_code, w.label_name, w.release_age
                ORDER BY w.week_ending_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            ) AS song_sale_equivalent,
            AVG(w.streaming_equivalent) OVER (
                PARTITION BY w.country_code, w.label_name, w.release_age
                ORDER BY w.week_ending_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
            ) AS streaming_equivalent
        FROM
            current_dev.data.marketshare_weekly w 
        QUALIFY w.week_ending_date = MAX(w.week_ending_date) OVER ()
    ),
    linear_forecast AS (
        SELECT
            da.week_ending_date,
            da.week_num,
            da.year,
            y.country_code,
            y.label_name,
            y.release_age,
            GREATEST(COALESCE(y.streaming_total + (a.streaming_total * (da.week_num - y.week_num)), 0), 0) AS streaming_total,
            GREATEST(COALESCE(y.product_sales + (a.product_sales * (da.week_num - y.week_num)), 0), 0) AS product_sales,
            GREATEST(COALESCE(y.song_sale_equivalent + (a.song_sale_equivalent * (da.week_num - y.week_num)), 0), 0) AS song_sale_equivalent,
            GREATEST(COALESCE(y.streaming_equivalent + (a.streaming_equivalent * (da.week_num - y.week_num)), 0), 0) AS streaming_equivalent,
            TRUE AS forecast
        FROM
            dates da
            JOIN current_dev.data.marketshare_ytd y ON 1 = 1
            AND y.week_ending_date = (
                SELECT MAX(week_ending_date) FROM current_dev.data.marketshare_ytd
            )
            LEFT JOIN weekly_avg a ON a.country_code = y.country_code
            AND a.label_name = y.label_name
            AND a.release_age = y.release_age
        WHERE
            da.week_ending_date > (
                SELECT MAX(week_ending_date) FROM current_dev.data.marketshare_ytd
            )
    ),
    forecast_marketshare AS (
        SELECT
            f1.*,
            f1.product_sales + f1.song_sale_equivalent + f1.streaming_equivalent AS album_equivalent,
            COALESCE(ROUND(((f1.product_sales + f1.song_sale_equivalent + f1.streaming_equivalent) / NULLIF(f2.product_sales + f2.song_sale_equivalent + f2.streaming_equivalent, 0)) * 100, 4), 0) AS album_equivalent_share,
            COALESCE(ROUND((f1.product_sales / NULLIF(f2.product_sales, 0)) * 100, 4), 0) AS product_sales_share,
            COALESCE(ROUND((f1.song_sale_equivalent / NULLIF(f2.song_sale_equivalent, 0)) * 100, 4), 0) AS song_sale_equivalent_share,
            COALESCE(ROUND((f1.streaming_equivalent / NULLIF(f2.streaming_equivalent, 0)) * 100, 4), 0) AS streaming_equivalent_share
        FROM
            linear_forecast f1
            LEFT JOIN linear_forecast f2 ON f2.week_ending_date = f1.week_ending_date
            AND f2.release_age = f1.release_age
            AND f2.country_code = f1.country_code
            AND f2.label_name = 'Total Universe'
    ),
    entire_market_year AS (
        SELECT
            week_ending_date, year, week_num, country_code, release_age, label_name,
            streaming_total, album_equivalent, product_sales, song_sale_equivalent, streaming_equivalent,
            album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share, forecast
        FROM building_marketshare
        
        UNION ALL
        
        SELECT
            week_ending_date, year, week_num, country_code, release_age, label_name,
            streaming_total, album_equivalent, product_sales, song_sale_equivalent, streaming_equivalent,
            album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share, forecast
        FROM forecast_marketshare
    )
    SELECT *
    FROM entire_market_year 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY week_ending_date, country_code, release_age, label_name ORDER BY forecast DESC
    ) = 1
    
) AS source
ON target.week_ending_date = source.week_ending_date
AND target.country_code = source.country_code
AND target.release_age = source.release_age
AND target.label_name = source.label_name

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
    OR target.forecast != source.forecast
) THEN
UPDATE SET
    target.year = source.year,
    target.week_num = source.week_num,
    target.streaming_total = source.streaming_total,
    target.album_equivalent = source.album_equivalent,
    target.product_sales = source.product_sales,
    target.song_sale_equivalent = source.song_sale_equivalent,
    target.streaming_equivalent = source.streaming_equivalent,
    target.album_equivalent_share = source.album_equivalent_share,
    target.product_sales_share = source.product_sales_share,
    target.song_sale_equivalent_share = source.song_sale_equivalent_share,
    target.streaming_equivalent_share = source.streaming_equivalent_share,
    target.forecast = source.forecast
    
WHEN NOT MATCHED THEN
INSERT (
    week_ending_date, year, week_num, country_code, release_age, label_name,
    streaming_total, album_equivalent, product_sales, song_sale_equivalent, streaming_equivalent,
    album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share, forecast
)
VALUES (
    source.week_ending_date, source.year, source.week_num, source.country_code, source.release_age, source.label_name,
    source.streaming_total, source.album_equivalent, source.product_sales, source.song_sale_equivalent, source.streaming_equivalent,
    source.album_equivalent_share, source.product_sales_share, source.song_sale_equivalent_share, source.streaming_equivalent_share, source.forecast
);
