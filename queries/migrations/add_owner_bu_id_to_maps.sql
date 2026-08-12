-- Idempotent: add OWNER_BU_ID so ICPN/ISRC MERGEs key on the rights BU, not the distributor path.
-- Path columns remain display/rollup fields and can change when Luminate reorgs hierarchy.
-- Backfill is best-effort. Rows with all-null path BU_IDs stay NULL and are removed by
-- the DELETE ... WHERE NOT EXISTS step in update_map_icpns / update_map_isrcs.

ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS ADD COLUMN IF NOT EXISTS OWNER_BU_ID STRING;
ALTER TABLE CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS ADD COLUMN IF NOT EXISTS OWNER_BU_ID STRING;

UPDATE CURRENT_DEV.DATA.MARKETSHARE_MAP_ICPNS
SET owner_bu_id = COALESCE(level_3_distributor_bu_id, level_2_distributor_bu_id, level_1_distributor_bu_id)
WHERE owner_bu_id IS NULL;

UPDATE CURRENT_DEV.DATA.MARKETSHARE_MAP_ISRCS
SET owner_bu_id = COALESCE(level_3_distributor_bu_id, level_2_distributor_bu_id, level_1_distributor_bu_id)
WHERE owner_bu_id IS NULL;
