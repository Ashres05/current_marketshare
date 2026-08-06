-- Idempotent migration: fix artist duplicates caused by NULL or colliding distributor BU_ID MERGE keys.
-- NULL = NULL never matched in the artist MERGE, so each pipeline run inserted another copy.
-- Rows that share N/A BU_IDs but differ only by distributor name also collide on the MERGE key
-- and get over-updated when the source is collapsed to one row per BU_ID path.
-- Backfill album NULL BU_IDs to N/A, then wipe artists so the normal (non-checkpointed)
-- artist update MERGE rebuilds a single row per artist x week x BU_ID path.
-- run_migrations() splits this file on the statement separator character,
-- so that character must never appear inside a comment.

UPDATE CURRENT_DEV.DATA.MARKETSHARE_WEEKLY_ALBUMS
SET
    level_1_distributor = COALESCE(level_1_distributor, 'N/A'),
    level_1_distributor_bu_id = COALESCE(level_1_distributor_bu_id, 'N/A'),
    level_2_distributor = COALESCE(level_2_distributor, 'N/A'),
    level_2_distributor_bu_id = COALESCE(level_2_distributor_bu_id, 'N/A'),
    level_3_distributor = COALESCE(level_3_distributor, 'N/A'),
    level_3_distributor_bu_id = COALESCE(level_3_distributor_bu_id, 'N/A')
WHERE level_1_distributor_bu_id IS NULL
   OR level_2_distributor_bu_id IS NULL
   OR level_3_distributor_bu_id IS NULL;

TRUNCATE TABLE CURRENT_DEV.DATA.MARKETSHARE_WEEKLY_ARTISTS;
