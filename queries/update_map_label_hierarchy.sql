-- Rebuild source once, prune orphans, then MERGE.
-- Snowflake does not support MERGE ... WHEN NOT MATCHED BY SOURCE.
CREATE OR REPLACE TEMPORARY TABLE tmp_marketshare_map_label_hierarchy_source AS
SELECT
    children_id AS bu_id,
    children AS label_name,
    start_date,
    end_date,
    MAX(CASE WHEN distributor_level = 1 THEN parent END)        AS level_1_distributor,
    MAX(CASE WHEN distributor_level = 1 THEN parent_bu_id END)  AS level_1_distributor_bu_id,
    MAX(CASE WHEN distributor_level = 2 THEN parent END)        AS level_2_distributor,
    MAX(CASE WHEN distributor_level = 2 THEN parent_bu_id END)  AS level_2_distributor_bu_id,
    MAX(CASE WHEN distributor_level = 3 THEN parent END)        AS level_3_distributor,
    MAX(CASE WHEN distributor_level = 3 THEN parent_bu_id END)  AS level_3_distributor_bu_id
FROM (
    SELECT
        children.bu_id AS children_id,
        children.name AS children,
        children.start_date,
        children.end_date,
        parent.bu_id AS parent_bu_id,
        parent.name AS parent,
        ROW_NUMBER() OVER (PARTITION BY children.bu_id ORDER BY h.level DESC) AS distributor_level
    FROM luminate_prod_wmgonly.extract_s.vw_business_unit_hierarchy_ds h
        JOIN luminate_prod_wmgonly.extract_s.vw_business_unit_ds children ON children.bu_id = h.child_bu_id
        JOIN luminate_prod_wmgonly.extract_s.vw_business_unit_ds parent ON parent.bu_id = h.parent_bu_id
    WHERE
        h.end_date > CURRENT_DATE()
        AND h.start_date <= CURRENT_DATE()
)
GROUP BY
    1, 2, 3, 4
QUALIFY ROW_NUMBER() OVER (PARTITION BY bu_id ORDER BY start_date DESC) = 1
;

DELETE FROM CURRENT_DEV.DATA.MARKETSHARE_MAP_LABEL_HIERARCHY AS target
WHERE NOT EXISTS (
    SELECT 1
    FROM tmp_marketshare_map_label_hierarchy_source AS source
    WHERE target.bu_id = source.bu_id
);

MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_MAP_LABEL_HIERARCHY AS target
USING tmp_marketshare_map_label_hierarchy_source AS source
    ON target.bu_id = source.bu_id

WHEN MATCHED AND (
    target.label_name IS DISTINCT FROM source.label_name
    OR target.start_date IS DISTINCT FROM source.start_date
    OR target.end_date IS DISTINCT FROM source.end_date
    OR target.level_1_distributor IS DISTINCT FROM source.level_1_distributor
    OR target.level_1_distributor_bu_id IS DISTINCT FROM source.level_1_distributor_bu_id
    OR target.level_2_distributor IS DISTINCT FROM source.level_2_distributor
    OR target.level_2_distributor_bu_id IS DISTINCT FROM source.level_2_distributor_bu_id
    OR target.level_3_distributor IS DISTINCT FROM source.level_3_distributor
    OR target.level_3_distributor_bu_id IS DISTINCT FROM source.level_3_distributor_bu_id
) THEN
    UPDATE SET
        target.label_name = source.label_name,
        target.start_date = source.start_date,
        target.end_date = source.end_date,
        target.level_1_distributor = source.level_1_distributor,
        target.level_1_distributor_bu_id = source.level_1_distributor_bu_id,
        target.level_2_distributor = source.level_2_distributor,
        target.level_2_distributor_bu_id = source.level_2_distributor_bu_id,
        target.level_3_distributor = source.level_3_distributor,
        target.level_3_distributor_bu_id = source.level_3_distributor_bu_id

WHEN NOT MATCHED THEN
    INSERT (
        bu_id,
        label_name,
        start_date,
        end_date,
        level_1_distributor,
        level_1_distributor_bu_id,
        level_2_distributor,
        level_2_distributor_bu_id,
        level_3_distributor,
        level_3_distributor_bu_id
    )
    VALUES (
        source.bu_id,
        source.label_name,
        source.start_date,
        source.end_date,
        source.level_1_distributor,
        source.level_1_distributor_bu_id,
        source.level_2_distributor,
        source.level_2_distributor_bu_id,
        source.level_3_distributor,
        source.level_3_distributor_bu_id
    );
