-- Idempotent migration: remove chart weeks that are not yet eligible for load.
-- Luminate availability lags about 2 days, so any week_ending_date after
-- DATEADD(DAY, -2, CURRENT_DATE()) is still building and must not stay in fact tables.
-- Safe to re-run: once that week becomes official the predicate no longer matches it.
-- Do not apply this filter to forecast tables (they intentionally store future weeks).
-- run_migrations() splits this file on the statement separator character,
-- so that character must never appear inside a comment.

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_WEEKLY_ALBUMS
WHERE week_ending_date > DATEADD(DAY, -2, CURRENT_DATE());

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_WEEKLY_ARTISTS
WHERE week_ending_date > DATEADD(DAY, -2, CURRENT_DATE());

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_WEEKLY
WHERE week_ending_date > DATEADD(DAY, -2, CURRENT_DATE());

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_YTD
WHERE week_ending_date > DATEADD(DAY, -2, CURRENT_DATE());
