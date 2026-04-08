MERGE INTO CURRENT_DEV.DATA.MARKETSHARE_WEEKLY AS target 
USING (
    WITH date_filter AS (
        SELECT
            DATEADD('DAY', -2, CURRENT_DATE) AS availability_date,
            b.week_end_date AS week_id_end_date
        FROM
            LUMINATE_PROD.EXTRACT_S.vw_date_ds AS a
            INNER JOIN LUMINATE_PROD.EXTRACT_S.vw_date_ds AS b ON a.previousweekid = b.weekid
        WHERE
            a.datename = availability_date
        LIMIT 1
    ), 
    
    unit_hierarchy AS (
        SELECT
            h.child_bu_id,
            MAX(h.level) AS max_level,
            h.child_bu_id AS l4_bu_id,
            MAX(CASE WHEN h.level = 1 THEN h.parent_bu_id END) AS p1,
            MAX(CASE WHEN h.level = 2 THEN h.parent_bu_id END) AS p2,
            MAX(CASE WHEN h.level = 3 THEN h.parent_bu_id END) AS p3,
            MAX(CASE WHEN h.level = 4 THEN h.parent_bu_id END) AS p4,
            CASE
                WHEN MAX(h.level) = 4 THEN p2
                WHEN MAX(h.level) = 3 THEN p1
                WHEN MAX(h.level) = 2 THEN p1
                WHEN MAX(h.level) = 1 THEN h.child_bu_id
            END AS l3_bu_id,
            CASE
                WHEN MAX(h.level) = 4 THEN p3
                WHEN MAX(h.level) = 3 THEN p2
                WHEN MAX(h.level) = 2 THEN p1
                WHEN MAX(h.level) = 1 THEN h.child_bu_id
            END AS l2_bu_id,
            CASE
                WHEN MAX(h.level) = 4 THEN p4
                WHEN MAX(h.level) = 3 THEN p3
                WHEN MAX(h.level) = 2 THEN p2
                WHEN MAX(h.level) = 1 THEN h.child_bu_id
            END AS l1_bu_id
        FROM
            LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_hierarchy_ds h
            CROSS JOIN date_filter df
        WHERE
            df.week_id_end_date BETWEEN h.start_date AND h.end_date
        GROUP BY ALL
    ),
    
    unit_lookup AS (
        SELECT
            u.child_bu_id AS bu_id,
            u.max_level AS level,
            bu4.name AS l4_name,
            bu3.name AS l3_name,
            bu2.name AS l2_name,
            bu1.name AS l1_name,
            ROW_NUMBER() OVER (
                ORDER BY
                    l1_bu_id, CASE WHEN max_level = 1 THEN 1 ELSE 2 END,
                    l2_bu_id, CASE WHEN max_level = 2 THEN 1 ELSE 2 END,
                    l3_bu_id, CASE WHEN max_level = 3 THEN 1 ELSE 2 END ASC
            ) AS bu4_order
        FROM
            unit_hierarchy u
            CROSS JOIN date_filter df
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS bu4 ON u.l4_bu_id = bu4.bu_id AND df.week_id_end_date BETWEEN bu4.start_date AND bu4.end_date
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS bu3 ON u.l3_bu_id = bu3.bu_id AND df.week_id_end_date BETWEEN bu3.start_date AND bu3.end_date
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS bu2 ON u.l2_bu_id = bu2.bu_id AND df.week_id_end_date BETWEEN bu2.start_date AND bu2.end_date
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS bu1 ON u.l1_bu_id = bu1.bu_id AND df.week_id_end_date BETWEEN bu1.start_date AND bu1.end_date
    ),
    
    bu_combined_base AS (
        SELECT
            bh.parent_bu_id,
            f.country_code,
            c.name AS market_share_group,
            u.level,
            u.bu4_order AS bu_order,
            'Volume' AS metric_type,
            'Weekly' AS period,
            f.week_id,
            fd.week_end_date AS week_id_end_date,
            CASE WHEN release_age < 18 THEN 'Current' ELSE 'Catalog' END AS release_age_type,
            ROUND(SUM(IFF(service_type <> 'Interactive', equivalent_quantity, 0))) AS album_equivalent,
            ROUND(SUM(IFF(metric_category = 'Streams' AND service_type <> 'Interactive', quantity, 0))) AS streams,
            ROUND(SUM(IFF(metric_category = 'RecordingSales', quantity, 0))) AS song_sales,
            ROUND(SUM(IFF(metric_category = 'ProductSales', quantity, 0))) AS product_sales
        FROM
            LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_weekly_fact_market_share_legacy_ds AS f
            INNER JOIN (SELECT DISTINCT weekid, week_end_date FROM LUMINATE_PROD.EXTRACT_S.vw_date_ds) AS fd ON f.week_id = fd.weekid
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS b ON f.owner_bu_id = b.bu_id AND b.country_code = f.country_code
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_hierarchy_ds AS bh ON f.owner_bu_id = bh.child_bu_id
            INNER JOIN LUMINATE_PROD_WMGONLY.EXTRACT_S.vw_business_unit_ds AS c ON bh.parent_bu_id = c.bu_id
            LEFT JOIN unit_lookup AS u ON u.bu_id = bh.parent_bu_id
            CROSS JOIN date_filter AS df
        WHERE
            f.market_id = -1
            AND f.genre_client_domain = 'Billboard'
            AND df.week_id_end_date BETWEEN b.start_date AND b.end_date
            AND df.week_id_end_date BETWEEN bh.start_date AND bh.end_date
        GROUP BY ALL
    ),
    
    combined_volumes AS (
        SELECT * FROM bu_combined_base
        UNION ALL
        SELECT
            parent_bu_id, country_code, market_share_group, level, bu_order, metric_type, period, week_id, week_id_end_date,
            'Overall', SUM(album_equivalent), SUM(streams), SUM(song_sales), SUM(product_sales)
        FROM bu_combined_base
        GROUP BY ALL
    ),
    
    uni_metrics AS (
        SELECT
            NULL AS parent_bu_id,
            country_code,
            t.label AS market_share_group,
            0 AS level,
            0 AS bu_order,
            metric_type,
            period,
            week_id,
            week_id_end_date,
            release_age_type,
            SUM(IFF(t.label = 'Total Universe (Excl Under Review)' AND f.market_share_group = 'Under Review', 0, album_equivalent)) AS album_equivalent,
            SUM(IFF(t.label = 'Total Universe (Excl Under Review)' AND f.market_share_group = 'Under Review', 0, streams)) AS streams,
            SUM(IFF(t.label = 'Total Universe (Excl Under Review)' AND f.market_share_group = 'Under Review', 0, song_sales)) AS song_sales,
            SUM(IFF(t.label = 'Total Universe (Excl Under Review)' AND f.market_share_group = 'Under Review', 0, product_sales)) AS product_sales
        FROM
            combined_volumes f
            CROSS JOIN (SELECT 'Total Universe' AS label UNION ALL SELECT 'Total Universe (Excl Under Review)' AS label) t
        WHERE
            level = 1
        GROUP BY ALL
    ),
    
    final_volume_pool AS (
        SELECT * FROM combined_volumes
        UNION ALL
        SELECT * FROM uni_metrics
    ),
    
    share_calculation AS (
        SELECT
            parent_bu_id, country_code, market_share_group, level, bu_order, 'Share (%)' AS metric_type, period, week_id, week_id_end_date, release_age_type,
            COALESCE(ROUND((album_equivalent / NULLIF(MAX(IFF(market_share_group = 'Total Universe (Excl Under Review)', album_equivalent, NULL)) OVER (PARTITION BY period, release_age_type, week_id), 0)) * 100, 4), 0) AS album_equivalent,
            COALESCE(ROUND((streams / NULLIF(MAX(IFF(market_share_group = 'Total Universe (Excl Under Review)', streams, NULL)) OVER (PARTITION BY period, release_age_type, week_id), 0)) * 100, 4), 0) AS streams,
            COALESCE(ROUND((song_sales / NULLIF(MAX(IFF(market_share_group = 'Total Universe (Excl Under Review)', song_sales, NULL)) OVER (PARTITION BY period, release_age_type, week_id), 0)) * 100, 4), 0) AS song_sales,
            COALESCE(ROUND((product_sales / NULLIF(MAX(IFF(market_share_group = 'Total Universe (Excl Under Review)', product_sales, NULL)) OVER (PARTITION BY period, release_age_type, week_id), 0)) * 100, 4), 0) AS product_sales
        FROM
            final_volume_pool
    ),
    
    weekly_marketshare AS (
        SELECT
            v.period, v.week_id, v.week_id_end_date, v.country_code, v.release_age_type, v.parent_bu_id, v.level, v.market_share_group, v.metric_type,
            v.album_equivalent, v.streams, v.song_sales, v.product_sales
        FROM (
            SELECT * FROM final_volume_pool WHERE market_share_group <> 'Total Universe (Excl Under Review)'
            UNION ALL
            SELECT * FROM share_calculation WHERE market_share_group NOT IN ('Total Universe', 'Total Universe (Excl Under Review)')
        ) v
        WHERE v.period = 'Weekly'
    ),
    
    current_dev_marketshare AS (
        SELECT
            m.week_id_end_date AS week_ending_date,
            da.yearid AS year,
            da.weeknum AS week_num,
            m.country_code,
            m.release_age_type AS release_age,
            m.market_share_group AS label_name,
            COALESCE(ROUND(MAX(IFF(m.metric_type = 'Volume', m.streams, 0)), 0), 0) AS streaming_total,
            COALESCE(MAX(IFF(m.metric_type = 'Volume', m.album_equivalent, 0)), 0) AS album_equivalent,
            GREATEST(COALESCE(MAX(IFF(m.metric_type = 'Volume', m.product_sales, 0)), 0), 0) AS product_sales,
            COALESCE(MAX(IFF(m.metric_type = 'Volume', m.song_sales / 10, 0)), 0) AS song_sale_equivalent,
            COALESCE(MAX(IFF(m.metric_type = 'Volume', m.album_equivalent - m.product_sales - (m.song_sales / 10), 0)), 0) AS streaming_equivalent,
            COALESCE(MAX(IFF(m.metric_type = 'Share (%)', m.album_equivalent, 0)), 0) AS album_equivalent_share,
            COALESCE(MAX(IFF(m.metric_type = 'Share (%)', m.product_sales, 0)), 0) AS product_sales_share,
            COALESCE(MAX(IFF(m.metric_type = 'Share (%)', m.song_sales, 0)), 0) AS song_sale_equivalent_share,
            COALESCE(MAX(IFF(m.metric_type = 'Share (%)', m.streams, 0)), 0) AS streaming_equivalent_share
        FROM
            weekly_marketshare m
            JOIN LUMINATE_PROD.EXTRACT_S.vw_date_ds da ON da.datename = m.week_id_end_date AND da.week_end_date < DATEADD(DAY, -2, CURRENT_DATE())
        GROUP BY ALL
    )
    
    SELECT 
        week_ending_date, year, week_num, country_code, release_age, label_name, streaming_total, album_equivalent, product_sales, song_sale_equivalent,
        streaming_equivalent, album_equivalent_share, product_sales_share, song_sale_equivalent_share, streaming_equivalent_share
    FROM current_dev_marketshare
    
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY week_ending_date, year, week_num, country_code, release_age, label_name 
        ORDER BY week_ending_date
    ) = 1

) AS source 
    ON target.WEEK_ENDING_DATE = source.week_ending_date
    AND target.COUNTRY_CODE = source.country_code
    AND target.RELEASE_AGE = source.release_age
    AND target.LABEL_NAME = source.label_name
    AND target.YEAR = source.year
    AND target.WEEK_NUM = source.week_num
    
WHEN MATCHED AND (
    target.STREAMING_TOTAL != source.streaming_total
    OR target.ALBUM_EQUIVALENT != source.album_equivalent
    OR target.PRODUCT_SALES != source.product_sales
    OR target.SONG_SALE_EQUIVALENT != source.song_sale_equivalent
    OR target.STREAMING_EQUIVALENT != source.streaming_equivalent
    OR target.ALBUM_EQUIVALENT_SHARE != source.album_equivalent_share
    OR target.PRODUCT_SALES_SHARE != source.product_sales_share
    OR target.SONG_SALE_EQUIVALENT_SHARE != source.song_sale_equivalent_share
    OR target.STREAMING_EQUIVALENT_SHARE != source.streaming_equivalent_share
    OR target.YEAR != source.year
    OR target.WEEK_NUM != source.week_num
) THEN
UPDATE SET
    target.STREAMING_TOTAL = source.streaming_total,
    target.ALBUM_EQUIVALENT = source.album_equivalent,
    target.PRODUCT_SALES = source.product_sales,
    target.SONG_SALE_EQUIVALENT = source.song_sale_equivalent,
    target.STREAMING_EQUIVALENT = source.streaming_equivalent,
    target.ALBUM_EQUIVALENT_SHARE = source.album_equivalent_share,
    target.PRODUCT_SALES_SHARE = source.product_sales_share,
    target.SONG_SALE_EQUIVALENT_SHARE = source.song_sale_equivalent_share,
    target.STREAMING_EQUIVALENT_SHARE = source.streaming_equivalent_share,
    target.YEAR = source.year,
    target.WEEK_NUM = source.week_num
    
WHEN NOT MATCHED THEN
INSERT (
    WEEK_ENDING_DATE, COUNTRY_CODE, RELEASE_AGE, LABEL_NAME, STREAMING_TOTAL, ALBUM_EQUIVALENT, PRODUCT_SALES, 
    SONG_SALE_EQUIVALENT, STREAMING_EQUIVALENT, ALBUM_EQUIVALENT_SHARE, PRODUCT_SALES_SHARE, SONG_SALE_EQUIVALENT_SHARE, 
    STREAMING_EQUIVALENT_SHARE, YEAR, WEEK_NUM
) VALUES (
    source.week_ending_date, source.country_code, source.release_age, source.label_name, source.streaming_total, source.album_equivalent, source.product_sales, 
    source.song_sale_equivalent, source.streaming_equivalent, source.album_equivalent_share, source.product_sales_share, source.song_sale_equivalent_share, 
    source.streaming_equivalent_share, source.year, source.week_num
);
