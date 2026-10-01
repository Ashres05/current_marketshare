-- Idempotent: add inclusive musical-right windows so ICPN/ISRC maps keep
-- every VALID OWNER that overlaps the fact horizon, not only the open right.
-- Existing rows stay. Null dates will not match the new MERGE key and are
-- replaced by windowed rows on the next map update.
-- run_migration() splits this file on the statement separator character,
-- so that character must never appear inside a comment.

ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS ADD COLUMN IF NOT EXISTS RIGHT_START_DATE DATE;
ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS ADD COLUMN IF NOT EXISTS RIGHT_END_DATE DATE;
ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS ADD COLUMN IF NOT EXISTS RIGHT_START_DATE DATE;
ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS ADD COLUMN IF NOT EXISTS RIGHT_END_DATE DATE;
