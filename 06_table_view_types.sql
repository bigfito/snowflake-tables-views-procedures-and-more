-- ============================================================
-- SNOWFLAKE INTELLIGENCE DEMO: PIZZERIA BELLA NAPOLI
-- Script 6: Table Types & View Types Showcase
-- ============================================================
-- This script demonstrates ALL Snowflake table and view types
-- in a real-world pizza restaurant context
-- ============================================================

USE DATABASE PIZZERIA_DEMO;
USE SCHEMA BELLA_NAPOLI;

-- ============================================================
-- PART 1: TABLE TYPES
-- ============================================================

-- --------------------------------------------------------
-- 1.1 STANDARD (PERMANENT) TABLES
-- Already created in 01_ddl_schema.sql
-- Features: Time Travel, Fail-safe, Cloning
-- --------------------------------------------------------

-- Example: Our core tables (DIM_CUSTOMER, FACT_ORDER, etc.) are permanent
-- Show Time Travel capability
SELECT * FROM FACT_ORDER AT(OFFSET => -60*5);  -- 5 minutes ago
SELECT * FROM FACT_ORDER BEFORE(STATEMENT => '<query_id>');  -- Before specific query

-- Clone a table for testing (zero-copy)
CREATE OR REPLACE TABLE FACT_ORDER_CLONE CLONE FACT_ORDER;


-- --------------------------------------------------------
-- 1.2 TRANSIENT TABLES
-- No Fail-safe period (reduced storage costs)
-- Good for: ETL staging, intermediate processing
-- --------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE STG_ONLINE_ORDERS (
    raw_order_id VARCHAR(50),
    raw_payload VARIANT,
    source_system VARCHAR(20),
    received_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_flag BOOLEAN DEFAULT FALSE
)
COMMENT = 'Staging table for incoming online orders - transient to reduce storage costs';

-- Insert sample staging data
INSERT INTO STG_ONLINE_ORDERS (raw_order_id, raw_payload, source_system)
SELECT 
    'ONLINE-' || UUID_STRING() AS raw_order_id,
    OBJECT_CONSTRUCT(
        'customer_email', c.email,
        'items', ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('item', 'Pepperoni', 'size', 'Large', 'qty', 1),
            OBJECT_CONSTRUCT('item', 'Garlic Knots', 'size', 'N/A', 'qty', 2)
        ),
        'total', 28.99,
        'delivery_address', c.address,
        'order_source', 'WEBSITE'
    ) AS raw_payload,
    'WEB_APP' AS source_system
FROM DIM_CUSTOMER c
WHERE c.customer_id <= 5;


-- --------------------------------------------------------
-- 1.3 TEMPORARY TABLES
-- Session-scoped, automatically dropped when session ends
-- Good for: Complex query intermediate results, user-specific temp data
-- --------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE TEMP_DAILY_METRICS AS
SELECT 
    DATE(order_timestamp) AS metric_date,
    location_id,
    COUNT(*) AS order_count,
    SUM(total_amount) AS daily_revenue,
    AVG(total_amount) AS avg_order_value
FROM FACT_ORDER
WHERE order_timestamp >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY DATE(order_timestamp), location_id;

-- Use in subsequent queries within this session
SELECT * FROM TEMP_DAILY_METRICS ORDER BY metric_date DESC LIMIT 10;


-- --------------------------------------------------------
-- 1.4 EXTERNAL TABLES
-- Query data in external cloud storage (S3, Azure, GCS)
-- Data stays in place - no ingestion required
-- --------------------------------------------------------

-- First, create an external stage pointing to cloud storage
CREATE OR REPLACE STAGE EXT_DELIVERY_DATA
    URL = 's3://bella-napoli-data/deliveries/'  -- Replace with actual bucket
    -- STORAGE_INTEGRATION = my_s3_integration  -- Uncomment with real integration
    FILE_FORMAT = (TYPE = 'PARQUET');

-- Create external table over delivery partner data
CREATE OR REPLACE EXTERNAL TABLE EXT_DOORDASH_ORDERS (
    order_id VARCHAR AS (VALUE:order_id::VARCHAR),
    restaurant_id VARCHAR AS (VALUE:restaurant_id::VARCHAR),
    customer_name VARCHAR AS (VALUE:customer_name::VARCHAR),
    order_total DECIMAL(10,2) AS (VALUE:order_total::DECIMAL(10,2)),
    delivery_fee DECIMAL(8,2) AS (VALUE:delivery_fee::DECIMAL(8,2)),
    tip_amount DECIMAL(8,2) AS (VALUE:tip_amount::DECIMAL(8,2)),
    order_placed_at TIMESTAMP_NTZ AS (VALUE:order_placed_at::TIMESTAMP_NTZ),
    delivered_at TIMESTAMP_NTZ AS (VALUE:delivered_at::TIMESTAMP_NTZ),
    driver_rating INT AS (VALUE:driver_rating::INT)
)
WITH LOCATION = @EXT_DELIVERY_DATA/doordash/
FILE_FORMAT = (TYPE = 'PARQUET')
COMMENT = 'External table for DoorDash delivery data - data remains in S3';

-- Similarly for UberEats
CREATE OR REPLACE EXTERNAL TABLE EXT_UBEREATS_ORDERS (
    uber_order_id VARCHAR AS (VALUE:uber_order_id::VARCHAR),
    merchant_id VARCHAR AS (VALUE:merchant_id::VARCHAR),
    items ARRAY AS (VALUE:items::ARRAY),
    subtotal DECIMAL(10,2) AS (VALUE:subtotal::DECIMAL(10,2)),
    service_fee DECIMAL(8,2) AS (VALUE:service_fee::DECIMAL(8,2)),
    delivery_fee DECIMAL(8,2) AS (VALUE:delivery_fee::DECIMAL(8,2)),
    created_at TIMESTAMP_NTZ AS (VALUE:created_at::TIMESTAMP_NTZ)
)
WITH LOCATION = @EXT_DELIVERY_DATA/ubereats/
FILE_FORMAT = (TYPE = 'PARQUET')
COMMENT = 'External table for UberEats order data';


-- --------------------------------------------------------
-- 1.5 ICEBERG TABLES
-- Open table format for interoperability
-- Can be read by Spark, Trino, Flink, etc.
-- --------------------------------------------------------

-- Create external volume for Iceberg (required)
-- Note: Replace with your actual cloud storage details
/*
CREATE OR REPLACE EXTERNAL VOLUME iceberg_ext_vol
    STORAGE_LOCATIONS = (
        (
            NAME = 'my-iceberg-storage'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://bella-napoli-iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789:role/snowflake-iceberg-role'
        )
    );
*/

-- Iceberg table for order analytics (shareable with Spark jobs)
CREATE OR REPLACE ICEBERG TABLE ICE_ORDER_ANALYTICS (
    order_date DATE,
    location_id INT,
    location_name VARCHAR(100),
    total_orders INT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(8,2),
    total_pizzas INT,
    delivery_orders INT,
    pickup_orders INT,
    dine_in_orders INT,
    unique_customers INT,
    new_customers INT,
    repeat_customers INT,
    avg_prep_time_minutes DECIMAL(6,2),
    partition_month VARCHAR(7)
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_ext_vol'  -- Reference your external volume
BASE_LOCATION = 'order_analytics/'
COMMENT = 'Iceberg table for order analytics - accessible by Spark/Trino/external tools';

-- Populate Iceberg table
INSERT INTO ICE_ORDER_ANALYTICS
SELECT 
    DATE(o.order_timestamp) AS order_date,
    o.location_id,
    l.location_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    SUM(CASE WHEN oi.item_id BETWEEN 1 AND 14 THEN oi.quantity ELSE 0 END) AS total_pizzas,
    SUM(CASE WHEN o.order_type = 'DELIVERY' THEN 1 ELSE 0 END) AS delivery_orders,
    SUM(CASE WHEN o.order_type = 'PICKUP' THEN 1 ELSE 0 END) AS pickup_orders,
    SUM(CASE WHEN o.order_type = 'DINE_IN' THEN 1 ELSE 0 END) AS dine_in_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    COUNT(DISTINCT CASE WHEN c.registration_date = DATE(o.order_timestamp) THEN c.customer_id END) AS new_customers,
    COUNT(DISTINCT o.customer_id) - COUNT(DISTINCT CASE WHEN c.registration_date = DATE(o.order_timestamp) THEN c.customer_id END) AS repeat_customers,
    AVG(TIMESTAMPDIFF(MINUTE, o.order_timestamp, o.actual_ready_time)) AS avg_prep_time_minutes,
    TO_VARCHAR(DATE(o.order_timestamp), 'YYYY-MM') AS partition_month
FROM FACT_ORDER o
JOIN DIM_LOCATION l ON o.location_id = l.location_id
LEFT JOIN FACT_ORDER_ITEM oi ON o.order_id = oi.order_id
LEFT JOIN DIM_CUSTOMER c ON o.customer_id = c.customer_id
GROUP BY DATE(o.order_timestamp), o.location_id, l.location_name;

-- Iceberg table for customer 360 (share with data science team using Spark)
CREATE OR REPLACE ICEBERG TABLE ICE_CUSTOMER_360 (
    customer_id INT,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(2),
    registration_date DATE,
    first_order_date DATE,
    last_order_date DATE,
    days_since_last_order INT,
    total_orders INT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(8,2),
    favorite_pizza VARCHAR(100),
    preferred_order_type VARCHAR(20),
    avg_rating_given DECIMAL(3,2),
    loyalty_points INT,
    customer_segment VARCHAR(50),
    lifetime_days INT,
    orders_per_month DECIMAL(6,2),
    last_updated TIMESTAMP_NTZ
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'iceberg_ext_vol'
BASE_LOCATION = 'customer_360/'
COMMENT = 'Iceberg table for Customer 360 view - used by ML pipelines in Spark';


-- --------------------------------------------------------
-- 1.6 HYBRID TABLES
-- HTAP: Transactional (row-based) + Analytical capabilities
-- Good for: Operational workloads requiring fast single-row lookups
-- --------------------------------------------------------

-- Real-time order status tracking (needs fast point lookups)
CREATE OR REPLACE HYBRID TABLE HYB_ORDER_STATUS (
    order_id INT PRIMARY KEY,
    location_id INT,
    order_status VARCHAR(20),
    current_station VARCHAR(50),  -- QUEUE, PREP, OVEN, BOXING, READY, OUT_FOR_DELIVERY
    assigned_chef_id INT,
    assigned_driver_id INT,
    estimated_ready_time TIMESTAMP_NTZ,
    actual_ready_time TIMESTAMP_NTZ,
    estimated_delivery_time TIMESTAMP_NTZ,
    actual_delivery_time TIMESTAMP_NTZ,
    last_status_update TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    customer_notified BOOLEAN DEFAULT FALSE,
    INDEX idx_location_status (location_id, order_status),
    INDEX idx_station (current_station)
)
COMMENT = 'Hybrid table for real-time order tracking - fast point lookups for kitchen displays';

-- Populate with today's orders
INSERT INTO HYB_ORDER_STATUS (order_id, location_id, order_status, current_station, 
    assigned_chef_id, estimated_ready_time, actual_ready_time)
SELECT 
    o.order_id,
    o.location_id,
    o.order_status,
    CASE o.order_status
        WHEN 'PENDING' THEN 'QUEUE'
        WHEN 'PREPARING' THEN 'PREP'
        WHEN 'READY' THEN 'READY'
        WHEN 'COMPLETED' THEN 'COMPLETED'
        ELSE 'UNKNOWN'
    END AS current_station,
    o.employee_id AS assigned_chef_id,
    o.estimated_ready_time,
    o.actual_ready_time
FROM FACT_ORDER o
WHERE DATE(o.order_timestamp) >= DATEADD(DAY, -1, CURRENT_DATE());

-- Fast point lookup (what Hybrid tables excel at)
SELECT * FROM HYB_ORDER_STATUS WHERE order_id = 12345;

-- Kitchen display query (filtered by location + status)
SELECT * FROM HYB_ORDER_STATUS 
WHERE location_id = 1 
AND order_status IN ('PENDING', 'PREPARING')
ORDER BY estimated_ready_time;


-- ============================================================
-- PART 2: VIEW TYPES
-- ============================================================

-- --------------------------------------------------------
-- 2.1 STANDARD VIEWS
-- Virtual tables - query executed at runtime
-- Already created in 01_ddl_schema.sql (V_ORDER_SUMMARY, etc.)
-- --------------------------------------------------------

-- Additional standard view: Menu profitability
CREATE OR REPLACE VIEW V_MENU_PROFITABILITY AS
SELECT 
    m.item_id,
    m.item_name,
    c.category_name,
    m.base_price,
    m.cost_to_make,
    m.base_price - m.cost_to_make AS profit_per_item,
    ROUND((m.base_price - m.cost_to_make) / m.base_price * 100, 1) AS profit_margin_pct,
    COUNT(DISTINCT oi.order_id) AS times_ordered,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.line_total) AS total_revenue,
    SUM(oi.quantity * m.cost_to_make) AS total_cost,
    SUM(oi.line_total) - SUM(oi.quantity * m.cost_to_make) AS total_profit
FROM DIM_MENU_ITEM m
JOIN DIM_CATEGORY c ON m.category_id = c.category_id
LEFT JOIN FACT_ORDER_ITEM oi ON m.item_id = oi.item_id
GROUP BY m.item_id, m.item_name, c.category_name, m.base_price, m.cost_to_make;


-- --------------------------------------------------------
-- 2.2 SECURE VIEWS
-- Definition hidden from users without ownership
-- Good for: Data sharing, row-level security, sensitive logic
-- --------------------------------------------------------

-- Secure view for franchise partners (hides business logic)
CREATE OR REPLACE SECURE VIEW V_FRANCHISE_PERFORMANCE AS
SELECT 
    l.location_name,
    DATE_TRUNC('WEEK', o.order_timestamp) AS week_starting,
    COUNT(DISTINCT o.order_id) AS weekly_orders,
    ROUND(SUM(o.total_amount), 2) AS weekly_revenue,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    ROUND(AVG(r.overall_rating), 2) AS avg_rating,
    -- Royalty calculation (hidden from partners)
    ROUND(SUM(o.total_amount) * 0.06, 2) AS franchise_royalty
FROM FACT_ORDER o
JOIN DIM_LOCATION l ON o.location_id = l.location_id
LEFT JOIN FACT_REVIEW r ON o.order_id = r.order_id
GROUP BY l.location_name, DATE_TRUNC('WEEK', o.order_timestamp);

-- Secure view with row-level security for multi-tenant access
CREATE OR REPLACE SECURE VIEW V_LOCATION_ORDERS_SECURE AS
SELECT 
    o.order_id,
    o.order_timestamp,
    o.order_type,
    o.total_amount,
    o.order_status,
    l.location_name
FROM FACT_ORDER o
JOIN DIM_LOCATION l ON o.location_id = l.location_id
WHERE l.location_id = CURRENT_SETTING('app.current_location_id')::INT
   OR CURRENT_ROLE() IN ('ADMIN', 'CORPORATE');


-- --------------------------------------------------------
-- 2.3 MATERIALIZED VIEWS
-- Pre-computed results stored physically
-- Auto-refreshed by Snowflake when base tables change
-- Good for: Expensive aggregations, frequent queries
-- --------------------------------------------------------

-- Materialized view for hourly sales dashboard (frequently queried)
CREATE OR REPLACE MATERIALIZED VIEW MV_HOURLY_SALES AS
SELECT 
    DATE(order_timestamp) AS order_date,
    HOUR(order_timestamp) AS order_hour,
    location_id,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    SUM(CASE WHEN order_type = 'DELIVERY' THEN 1 ELSE 0 END) AS delivery_count,
    SUM(CASE WHEN order_type = 'PICKUP' THEN 1 ELSE 0 END) AS pickup_count,
    SUM(CASE WHEN order_type = 'DINE_IN' THEN 1 ELSE 0 END) AS dine_in_count
FROM FACT_ORDER
GROUP BY DATE(order_timestamp), HOUR(order_timestamp), location_id;

-- Materialized view for customer metrics (expensive to compute)
CREATE OR REPLACE MATERIALIZED VIEW MV_CUSTOMER_METRICS AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.city,
    c.loyalty_points,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS lifetime_value,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.order_timestamp) AS first_order_date,
    MAX(o.order_timestamp) AS last_order_date,
    DATEDIFF(DAY, MAX(o.order_timestamp), CURRENT_TIMESTAMP()) AS days_since_last_order
FROM DIM_CUSTOMER c
LEFT JOIN FACT_ORDER o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.loyalty_points;

-- Query the MV (fast, pre-computed)
SELECT * FROM MV_CUSTOMER_METRICS ORDER BY lifetime_value DESC LIMIT 10;

-- Check MV refresh status
SHOW MATERIALIZED VIEWS LIKE 'MV_%';


-- ============================================================
-- PART 3: SUMMARY - TABLE & VIEW TYPE COMPARISON
-- ============================================================

/*
+-------------------+------------------+----------------------------------+----------------------------+
| TABLE TYPE        | TIME TRAVEL      | USE CASE                         | PIZZA DEMO EXAMPLE         |
+-------------------+------------------+----------------------------------+----------------------------+
| Permanent         | Yes (up to 90d)  | Core business data               | FACT_ORDER, DIM_CUSTOMER   |
| Transient         | Yes (0-1 day)    | ETL staging, temp processing     | STG_ONLINE_ORDERS          |
| Temporary         | No               | Session-scoped calculations      | TEMP_DAILY_METRICS         |
| External          | No               | Query external cloud storage     | EXT_DOORDASH_ORDERS        |
| Iceberg           | Yes              | Open format, multi-engine access | ICE_ORDER_ANALYTICS        |
| Hybrid            | Yes              | HTAP, fast point lookups         | HYB_ORDER_STATUS           |
+-------------------+------------------+----------------------------------+----------------------------+

+-------------------+------------------+----------------------------------+----------------------------+
| VIEW TYPE         | STORED RESULTS   | USE CASE                         | PIZZA DEMO EXAMPLE         |
+-------------------+------------------+----------------------------------+----------------------------+
| Standard          | No               | Simplify queries, abstraction    | V_ORDER_SUMMARY            |
| Secure            | No               | Hide logic, data sharing, RLS    | V_FRANCHISE_PERFORMANCE    |
| Materialized      | Yes              | Pre-computed aggregations        | MV_HOURLY_SALES            |
+-------------------+------------------+----------------------------------+----------------------------+
*/

COMMIT;
