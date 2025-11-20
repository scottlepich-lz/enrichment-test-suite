-- Query to extract Order Completed events for enrichment testing
--
-- This query retrieves events from the Segment Order Completed fact table
-- with the required columns for the enrichment test suite.
--
-- Output columns:
--   EVENT_ID: Unique Segment event identifier
--   FULL_EVENT_PAYLOAD: Complete JSON event payload
--
-- Usage:
--   1. Run this query in Snowflake
--   2. Export results as CSV
--   3. Use the CSV as input for enrich_csv_with_test.py
--
-- Default: Last 7 days, limited to 1000 events
-- Modify date range and limit as needed for your test

SELECT
    fso.segment_event_id AS EVENT_ID,
    fso.full_event_payload AS FULL_EVENT_PAYLOAD
FROM analytics.product.fct_segment_order_completed_event AS fso
WHERE fso.occurred_date_pt >= CURRENT_DATE - 7
LIMIT 1000;

-- Alternative queries for different test scenarios:

-- Last 30 days, 5000 events:
-- WHERE fso.occurred_date_pt >= CURRENT_DATE - 30
-- LIMIT 5000;

-- Specific date range:
-- WHERE fso.occurred_date_pt BETWEEN '2025-11-01' AND '2025-11-19'
-- LIMIT 1000;

-- Filter by specific products (example):
-- WHERE fso.occurred_date_pt >= CURRENT_DATE - 7
--   AND fso.full_event_payload:properties:products[0]:product_id IN ('8868', '8869')
-- LIMIT 1000;

-- Include only events with specific topline product group:
-- WHERE fso.occurred_date_pt >= CURRENT_DATE - 7
--   AND fso.full_event_payload:properties:topline_product_group = 'LLC'
-- LIMIT 1000;
