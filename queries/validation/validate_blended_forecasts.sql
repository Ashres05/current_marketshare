-- Ad hoc validation queries for the seasonal blended forecast change.
-- Run these manually against prod BEFORE treating the pipeline run as successful.
-- Do NOT move this file to queries/migrations/ — that folder is auto-executed on every pipeline run.

-- ============================================================
-- 1. Row-count parity
--    The new USING block must produce the same number of rows as the old one.
--    A mismatch means last_year_avg fanned out a join.
--    Run the new update_marketshare_forecasts_weekly.sql USING block as a SELECT
--    and compare to the row count currently in the table for forecast=TRUE rows.
-- ============================================================

-- Expected count from current table (forecast rows only):
SELECT COUNT(*) AS current_forecast_row_count
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS_WEEKLY
WHERE FORECAST = TRUE;

-- Expected count from current table (forecast rows only, cumulative):
SELECT COUNT(*) AS current_forecast_row_count
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS
WHERE FORECAST = TRUE;

-- ============================================================
-- 2. Seasonal shape spot-check
--    For Total Universe, streaming_equivalent should now vary week to week
--    (reflecting last year's seasonality) instead of being constant.
--    Pick a country_code and release_age that has last-year data.
-- ============================================================

SELECT
    week_ending_date,
    week_num,
    country_code,
    release_age,
    label_name,
    streaming_equivalent,
    product_sales,
    song_sale_equivalent
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS_WEEKLY
WHERE FORECAST = TRUE
  AND label_name = 'Total Universe'
  AND country_code = 'US'          -- adjust to a country present in your data
  AND release_age = 'TOTAL'        -- adjust to a release_age present in your data
ORDER BY week_num;

-- ============================================================
-- 3. FORECASTS = YTD baseline + SUM(FORECASTS_WEEKLY) reconciliation
--    For each (country_code, label_name, release_age), the cumulative
--    MARKETSHARE_FORECASTS value at the last forecast week must equal
--    the YTD baseline (from the last actual marketshare_ytd row)
--    plus the sum of per-week streaming_equivalent from MARKETSHARE_FORECASTS_WEEKLY
--    for that same group's forecast weeks.
-- ============================================================

WITH ytd_baseline AS (
    SELECT
        country_code,
        release_age,
        label_name,
        streaming_equivalent AS baseline_streaming_equivalent,
        product_sales        AS baseline_product_sales,
        song_sale_equivalent AS baseline_song_sale_equivalent,
        streaming_total      AS baseline_streaming_total
    FROM CURRENT_DEV.DATA.MARKETSHARE_YTD
    WHERE week_ending_date = (SELECT MAX(week_ending_date) FROM CURRENT_DEV.DATA.MARKETSHARE_YTD)
),
weekly_sum AS (
    SELECT
        country_code,
        release_age,
        label_name,
        SUM(streaming_equivalent) AS sum_weekly_streaming_equivalent,
        SUM(product_sales)        AS sum_weekly_product_sales,
        SUM(song_sale_equivalent) AS sum_weekly_song_sale_equivalent
    FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS_WEEKLY
    WHERE FORECAST = TRUE
    GROUP BY country_code, release_age, label_name
),
cumulative_last_week AS (
    SELECT
        country_code,
        release_age,
        label_name,
        streaming_equivalent AS cum_streaming_equivalent,
        product_sales        AS cum_product_sales,
        song_sale_equivalent AS cum_song_sale_equivalent
    FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS
    WHERE FORECAST = TRUE
      AND week_ending_date = (
          SELECT MAX(week_ending_date)
          FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS
          WHERE FORECAST = TRUE
      )
)
SELECT
    b.country_code,
    b.release_age,
    b.label_name,
    b.baseline_streaming_equivalent + w.sum_weekly_streaming_equivalent AS expected_cum_streaming,
    c.cum_streaming_equivalent                                           AS actual_cum_streaming,
    ABS((b.baseline_streaming_equivalent + w.sum_weekly_streaming_equivalent) - c.cum_streaming_equivalent) AS delta_streaming,
    b.baseline_product_sales + w.sum_weekly_product_sales               AS expected_cum_product_sales,
    c.cum_product_sales                                                  AS actual_cum_product_sales
FROM ytd_baseline b
JOIN weekly_sum w
    ON w.country_code = b.country_code
   AND w.release_age  = b.release_age
   AND w.label_name   = b.label_name
JOIN cumulative_last_week c
    ON c.country_code = b.country_code
   AND c.release_age  = b.release_age
   AND c.label_name   = b.label_name
-- Any row here with delta_streaming > 0.01 indicates a reconciliation failure:
HAVING ABS((b.baseline_streaming_equivalent + w.sum_weekly_streaming_equivalent) - c.cum_streaming_equivalent) > 0.01
ORDER BY delta_streaming DESC;

-- ============================================================
-- 4. No negative metrics, no week-over-week decreases in cumulative table
-- ============================================================

-- 4a. Negative metric check (both tables):
SELECT 'FORECASTS_WEEKLY' AS tbl, week_ending_date, country_code, release_age, label_name,
       streaming_equivalent, product_sales, song_sale_equivalent, streaming_total
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS_WEEKLY
WHERE FORECAST = TRUE
  AND (streaming_equivalent < 0 OR product_sales < 0 OR song_sale_equivalent < 0 OR streaming_total < 0)

UNION ALL

SELECT 'FORECASTS' AS tbl, week_ending_date, country_code, release_age, label_name,
       streaming_equivalent, product_sales, song_sale_equivalent, streaming_total
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS
WHERE FORECAST = TRUE
  AND (streaming_equivalent < 0 OR product_sales < 0 OR song_sale_equivalent < 0 OR streaming_total < 0);

-- 4b. Week-over-week non-decreasing check for the cumulative table:
--     Any row returned here is a week where the YTD value dropped vs the prior week.
SELECT
    curr.week_ending_date,
    curr.week_num,
    curr.country_code,
    curr.release_age,
    curr.label_name,
    prev.streaming_equivalent AS prev_streaming_equivalent,
    curr.streaming_equivalent AS curr_streaming_equivalent,
    curr.streaming_equivalent - prev.streaming_equivalent AS delta
FROM CURRENT_DEV.DATA.MARKETSHARE_FORECASTS curr
JOIN CURRENT_DEV.DATA.MARKETSHARE_FORECASTS prev
    ON prev.country_code     = curr.country_code
   AND prev.release_age      = curr.release_age
   AND prev.label_name       = curr.label_name
   AND prev.week_num         = curr.week_num - 1
   AND prev.year             = curr.year
WHERE curr.FORECAST = TRUE
  AND prev.FORECAST = TRUE
  AND curr.streaming_equivalent < prev.streaming_equivalent
ORDER BY delta ASC;
