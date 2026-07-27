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
    rolling_avg AS (
        SELECT
            w.week_ending_date,
            w.year,
            w.week_num,
            w.country_code,
            w.bu_id,
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
    ),
    weekly_avg AS (
        SELECT *
        FROM rolling_avg
        QUALIFY week_ending_date = MAX(week_ending_date) OVER ()
    ),
    last_year_avg AS (
        SELECT *
        FROM rolling_avg
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_code, label_name, release_age, year, week_num
            ORDER BY week_ending_date DESC
        ) = 1
    ),
    anchor AS (
        SELECT DISTINCT
            w.year AS anchor_year,
            w.week_num AS anchor_week_num
        FROM
            current_dev.data.marketshare_weekly w
        WHERE
            w.week_ending_date = (
                SELECT MAX(week_ending_date) FROM current_dev.data.marketshare_weekly
            )
    ),
    -- Last year's 8-week average at the same week number as the latest actual week.
    -- Used as the denominator so last year contributes seasonal shape, not its level.
    last_year_anchor AS (
        SELECT
            ly.country_code,
            ly.label_name,
            ly.release_age,
            ly.streaming_total AS anchor_streaming_total,
            ly.product_sales AS anchor_product_sales,
            ly.song_sale_equivalent AS anchor_song_sale_equivalent,
            ly.streaming_equivalent AS anchor_streaming_equivalent
        FROM
            last_year_avg ly
            JOIN anchor an ON ly.year = an.anchor_year - 1
            AND ly.week_num = an.anchor_week_num
    ),
    weekly_blend AS (
        SELECT
            da.week_ending_date,
            da.week_num,
            da.year,
            y.country_code,
            y.bu_id,
            y.label_name,
            y.release_age,
            y.streaming_total AS ytd_streaming_total,
            y.product_sales AS ytd_product_sales,
            y.song_sale_equivalent AS ytd_song_sale_equivalent,
            y.streaming_equivalent AS ytd_streaming_equivalent,
            -- Seasonal index: this year's level scaled by last year's shape.
            -- When last year's anchor week is missing or non-positive, or the resulting
            -- ratio lands outside [0.25, 4.0], last year's shape is untrustworthy
            -- (e.g. a label that ramped up mid-year), so no seasonal adjustment is applied.
            GREATEST(
                CASE
                    WHEN da.week_num = 53
                         OR ly.streaming_total IS NULL
                         OR lya.anchor_streaming_total IS NULL
                         OR lya.anchor_streaming_total <= 0
                         OR ly.streaming_total / lya.anchor_streaming_total NOT BETWEEN 0.25 AND 4.0
                        THEN COALESCE(a.streaming_total, 0)
                    ELSE COALESCE(a.streaming_total, 0)
                         * (0.7 + 0.3 * (ly.streaming_total / lya.anchor_streaming_total))
                END, 0) AS streaming_total,
            GREATEST(
                CASE
                    WHEN da.week_num = 53
                         OR ly.product_sales IS NULL
                         OR lya.anchor_product_sales IS NULL
                         OR lya.anchor_product_sales <= 0
                         OR ly.product_sales / lya.anchor_product_sales NOT BETWEEN 0.25 AND 4.0
                        THEN COALESCE(a.product_sales, 0)
                    ELSE COALESCE(a.product_sales, 0)
                         * (0.7 + 0.3 * (ly.product_sales / lya.anchor_product_sales))
                END, 0) AS product_sales,
            GREATEST(
                CASE
                    WHEN da.week_num = 53
                         OR ly.song_sale_equivalent IS NULL
                         OR lya.anchor_song_sale_equivalent IS NULL
                         OR lya.anchor_song_sale_equivalent <= 0
                         OR ly.song_sale_equivalent / lya.anchor_song_sale_equivalent NOT BETWEEN 0.25 AND 4.0
                        THEN COALESCE(a.song_sale_equivalent, 0)
                    ELSE COALESCE(a.song_sale_equivalent, 0)
                         * (0.7 + 0.3 * (ly.song_sale_equivalent / lya.anchor_song_sale_equivalent))
                END, 0) AS song_sale_equivalent,
            GREATEST(
                CASE
                    WHEN da.week_num = 53
                         OR ly.streaming_equivalent IS NULL
                         OR lya.anchor_streaming_equivalent IS NULL
                         OR lya.anchor_streaming_equivalent <= 0
                         OR ly.streaming_equivalent / lya.anchor_streaming_equivalent NOT BETWEEN 0.25 AND 4.0
                        THEN COALESCE(a.streaming_equivalent, 0)
                    ELSE COALESCE(a.streaming_equivalent, 0)
                         * (0.7 + 0.3 * (ly.streaming_equivalent / lya.anchor_streaming_equivalent))
                END, 0) AS streaming_equivalent
        FROM
            dates da
            JOIN current_dev.data.marketshare_ytd y ON 1 = 1
            AND y.week_ending_date = (
                SELECT MAX(week_ending_date) FROM current_dev.data.marketshare_ytd
            )
            LEFT JOIN weekly_avg a ON a.country_code = y.country_code
            AND a.label_name = y.label_name
            AND a.release_age = y.release_age
            LEFT JOIN last_year_avg ly ON ly.country_code = y.country_code
            AND ly.label_name = y.label_name
            AND ly.release_age = y.release_age
            AND ly.year = da.year - 1
            AND ly.week_num = da.week_num
            LEFT JOIN last_year_anchor lya ON lya.country_code = y.country_code
            AND lya.label_name = y.label_name
            AND lya.release_age = y.release_age
        WHERE
            da.week_ending_date > (
                SELECT MAX(week_ending_date) FROM current_dev.data.marketshare_ytd
            )
    ),
    cumulative_blend AS (
        SELECT
            *,
            SUM(streaming_total) OVER (
                PARTITION BY country_code, label_name, release_age
                ORDER BY week_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_streaming_total,
            SUM(product_sales) OVER (
                PARTITION BY country_code, label_name, release_age
                ORDER BY week_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_product_sales,
            SUM(song_sale_equivalent) OVER (
                PARTITION BY country_code, label_name, release_age
                ORDER BY week_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_song_sale_equivalent,
            SUM(streaming_equivalent) OVER (
                PARTITION BY country_code, label_name, release_age
                ORDER BY week_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_streaming_equivalent
        FROM weekly_blend
    ),
    linear_forecast AS (
        SELECT
            week_ending_date,
            week_num,
            year,
            country_code,
            bu_id,
            label_name,
            release_age,
            GREATEST(COALESCE(ytd_streaming_total + cum_streaming_total, 0), 0) AS streaming_total,
            GREATEST(COALESCE(ytd_product_sales + cum_product_sales, 0), 0) AS product_sales,
            GREATEST(COALESCE(ytd_song_sale_equivalent + cum_song_sale_equivalent, 0), 0) AS song_sale_equivalent,
            GREATEST(COALESCE(ytd_streaming_equivalent + cum_streaming_equivalent, 0), 0) AS streaming_equivalent,
            TRUE AS forecast
        FROM cumulative_blend
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
            week_ending_date, year, week_num, country_code, release_age, bu_id, label_name,
            streaming_total, album_equivalent, product_sales, song_sale_equivalent, streaming_equivalent,
            album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share, forecast
        FROM building_marketshare
        
        UNION ALL
        
        SELECT
            week_ending_date, year, week_num, country_code, release_age, bu_id, label_name,
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
    OR target.bu_id IS DISTINCT FROM source.bu_id
) THEN
UPDATE SET
    target.year = source.year,
    target.week_num = source.week_num,
    target.bu_id = source.bu_id,
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
    week_ending_date, year, week_num, country_code, release_age, bu_id, label_name,
    streaming_total, album_equivalent, product_sales, song_sale_equivalent, streaming_equivalent,
    album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share, forecast
)
VALUES (
    source.week_ending_date, source.year, source.week_num, source.country_code, source.release_age, source.bu_id, source.label_name,
    source.streaming_total, source.album_equivalent, source.product_sales, source.song_sale_equivalent, source.streaming_equivalent,
    source.album_equivalent_share, source.product_sales_share, source.song_sale_equivalent_share, source.streaming_equivalent_share, source.forecast
);
