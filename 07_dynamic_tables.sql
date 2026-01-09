-- ============================================================
-- SNOWFLAKE INTELLIGENCE DEMO: PIZZERIA BELLA NAPOLI
-- Script 7: Dynamic Tables
-- ============================================================
-- Dynamic Tables automatically refresh based on a defined lag
-- Perfect for declarative data pipelines without managing Tasks
-- ============================================================

USE DATABASE PIZZERIA_DEMO;
USE SCHEMA BELLA_NAPOLI;

-- ============================================================
-- PART 1: BRONZE → SILVER → GOLD PIPELINE WITH DYNAMIC TABLES
-- ============================================================

-- --------------------------------------------------------
-- BRONZE LAYER: Raw incoming order events
-- Simulates real-time order stream from POS systems
-- --------------------------------------------------------

-- Source table (raw events - simulating streaming input)
CREATE OR REPLACE TABLE BRONZE_ORDER_EVENTS (
    event_id VARCHAR(50) DEFAULT UUID_STRING(),
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    event_type VARCHAR(20),  -- ORDER_PLACED, ORDER_UPDATED, ORDER_COMPLETED
    location_code VARCHAR(10),
    payload VARIANT,
    source_system VARCHAR(20),
    processed BOOLEAN DEFAULT FALSE
);

-- Insert sample raw events
INSERT INTO BRONZE_ORDER_EVENTS (event_type, location_code, payload, source_system)
SELECT 
    'ORDER_PLACED' AS event_type,
    CASE o.location_id 
        WHEN 1 THEN 'DT-001' 
        WHEN 2 THEN 'WL-002' 
        ELSE 'RR-003' 
    END AS location_code,
    OBJECT_CONSTRUCT(
        'order_id', o.order_id,
        'customer_id', o.customer_id,
        'order_type', o.order_type,
        'subtotal', o.subtotal,
        'tax', o.tax_amount,
        'tip', o.tip_amount,
        'total', o.total_amount,
        'payment_method', o.payment_method,
        'items', (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'item_id', oi.item_id,
                'item_name', m.item_name,
                'quantity', oi.quantity,
                'price', oi.unit_price
            ))
            FROM FACT_ORDER_ITEM oi
            JOIN DIM_MENU_ITEM m ON oi.item_id = m.item_id
            WHERE oi.order_id = o.order_id
        )
    ) AS payload,
    CASE MOD(o.order_id, 3)
        WHEN 0 THEN 'POS_TERMINAL'
        WHEN 1 THEN 'WEB_APP'
        ELSE 'MOBILE_APP'
    END AS source_system
FROM FACT_ORDER o
WHERE o.order_timestamp >= DATEADD(DAY, -7, CURRENT_DATE())
LIMIT 500;


-- --------------------------------------------------------
-- SILVER LAYER: Cleaned and normalized order data
-- Dynamic Table auto-refreshes every 1 minute
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_SILVER_ORDERS
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH  -- Replace with your warehouse
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    -- Event metadata
    e.event_id,
    e.event_timestamp,
    e.event_type,
    e.source_system,
    
    -- Extract and type-cast from payload
    e.payload:order_id::INT AS order_id,
    e.payload:customer_id::INT AS customer_id,
    
    -- Map location code to location_id
    CASE e.location_code
        WHEN 'DT-001' THEN 1
        WHEN 'WL-002' THEN 2
        WHEN 'RR-003' THEN 3
    END AS location_id,
    
    -- Order details
    e.payload:order_type::VARCHAR AS order_type,
    e.payload:subtotal::DECIMAL(10,2) AS subtotal,
    e.payload:tax::DECIMAL(10,2) AS tax_amount,
    e.payload:tip::DECIMAL(10,2) AS tip_amount,
    e.payload:total::DECIMAL(10,2) AS total_amount,
    e.payload:payment_method::VARCHAR AS payment_method,
    
    -- Flatten items array for item-level analysis
    e.payload:items AS order_items,
    ARRAY_SIZE(e.payload:items) AS item_count,
    
    -- Calculated fields
    DATE(e.event_timestamp) AS order_date,
    HOUR(e.event_timestamp) AS order_hour,
    DAYNAME(e.event_timestamp) AS day_of_week,
    
    -- Data quality flag
    CASE 
        WHEN e.payload:order_id IS NULL THEN FALSE
        WHEN e.payload:total::DECIMAL(10,2) <= 0 THEN FALSE
        ELSE TRUE
    END AS is_valid_order

FROM BRONZE_ORDER_EVENTS e
WHERE e.event_type = 'ORDER_PLACED';


-- --------------------------------------------------------
-- SILVER LAYER: Flattened order items
-- Dynamic Table for item-level analysis
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_SILVER_ORDER_ITEMS
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    o.event_id,
    o.order_id,
    o.order_date,
    o.location_id,
    
    -- Flatten items array
    item.value:item_id::INT AS item_id,
    item.value:item_name::VARCHAR AS item_name,
    item.value:quantity::INT AS quantity,
    item.value:price::DECIMAL(8,2) AS unit_price,
    item.value:quantity::INT * item.value:price::DECIMAL(8,2) AS line_total,
    item.index + 1 AS item_sequence

FROM DT_SILVER_ORDERS o,
    LATERAL FLATTEN(input => o.order_items) item
WHERE o.is_valid_order = TRUE;


-- --------------------------------------------------------
-- GOLD LAYER: Daily sales aggregations
-- Dynamic Table with 5-minute lag for near-real-time dashboards
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_GOLD_DAILY_SALES
    TARGET_LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    o.order_date,
    o.location_id,
    l.location_name,
    
    -- Order metrics
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    
    -- Order type breakdown
    SUM(CASE WHEN o.order_type = 'DELIVERY' THEN 1 ELSE 0 END) AS delivery_orders,
    SUM(CASE WHEN o.order_type = 'PICKUP' THEN 1 ELSE 0 END) AS pickup_orders,
    SUM(CASE WHEN o.order_type = 'DINE_IN' THEN 1 ELSE 0 END) AS dine_in_orders,
    
    -- Revenue by order type
    SUM(CASE WHEN o.order_type = 'DELIVERY' THEN o.total_amount ELSE 0 END) AS delivery_revenue,
    SUM(CASE WHEN o.order_type = 'PICKUP' THEN o.total_amount ELSE 0 END) AS pickup_revenue,
    SUM(CASE WHEN o.order_type = 'DINE_IN' THEN o.total_amount ELSE 0 END) AS dine_in_revenue,
    
    -- Payment method breakdown
    SUM(CASE WHEN o.payment_method = 'CREDIT' THEN 1 ELSE 0 END) AS credit_orders,
    SUM(CASE WHEN o.payment_method = 'CASH' THEN 1 ELSE 0 END) AS cash_orders,
    SUM(CASE WHEN o.payment_method = 'MOBILE' THEN 1 ELSE 0 END) AS mobile_orders,
    
    -- Source system breakdown
    SUM(CASE WHEN o.source_system = 'WEB_APP' THEN 1 ELSE 0 END) AS web_orders,
    SUM(CASE WHEN o.source_system = 'MOBILE_APP' THEN 1 ELSE 0 END) AS mobile_app_orders,
    SUM(CASE WHEN o.source_system = 'POS_TERMINAL' THEN 1 ELSE 0 END) AS pos_orders,
    
    -- Time of processing
    CURRENT_TIMESTAMP() AS last_refreshed

FROM DT_SILVER_ORDERS o
JOIN DIM_LOCATION l ON o.location_id = l.location_id
WHERE o.is_valid_order = TRUE
GROUP BY o.order_date, o.location_id, l.location_name;


-- --------------------------------------------------------
-- GOLD LAYER: Hourly sales for real-time monitoring
-- Downstream from silver, 1-minute freshness
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_GOLD_HOURLY_SALES
    TARGET_LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    o.order_date,
    o.order_hour,
    o.location_id,
    l.location_name,
    o.day_of_week,
    
    COUNT(DISTINCT o.order_id) AS orders,
    SUM(o.total_amount) AS revenue,
    AVG(o.total_amount) AS avg_order_value,
    AVG(o.item_count) AS avg_items_per_order,
    
    -- Peak hour indicator
    CASE 
        WHEN o.order_hour BETWEEN 11 AND 13 THEN 'LUNCH_RUSH'
        WHEN o.order_hour BETWEEN 17 AND 20 THEN 'DINNER_RUSH'
        ELSE 'REGULAR'
    END AS period_type

FROM DT_SILVER_ORDERS o
JOIN DIM_LOCATION l ON o.location_id = l.location_id
WHERE o.is_valid_order = TRUE
GROUP BY o.order_date, o.order_hour, o.location_id, l.location_name, o.day_of_week;


-- --------------------------------------------------------
-- GOLD LAYER: Menu item performance
-- Which items are selling best?
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_GOLD_ITEM_PERFORMANCE
    TARGET_LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    i.order_date,
    i.location_id,
    l.location_name,
    i.item_id,
    i.item_name,
    m.category_id,
    c.category_name,
    
    -- Sales metrics
    SUM(i.quantity) AS quantity_sold,
    SUM(i.line_total) AS item_revenue,
    COUNT(DISTINCT i.order_id) AS orders_containing_item,
    AVG(i.quantity) AS avg_quantity_per_order,
    
    -- Profitability (join with menu for cost)
    SUM(i.quantity * (m.base_price - m.cost_to_make)) AS estimated_profit

FROM DT_SILVER_ORDER_ITEMS i
JOIN DIM_LOCATION l ON i.location_id = l.location_id
JOIN DIM_MENU_ITEM m ON i.item_id = m.item_id
JOIN DIM_CATEGORY c ON m.category_id = c.category_id
GROUP BY i.order_date, i.location_id, l.location_name, 
         i.item_id, i.item_name, m.category_id, c.category_name;


-- --------------------------------------------------------
-- GOLD LAYER: Customer purchase patterns
-- Real-time customer analytics
-- --------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DT_GOLD_CUSTOMER_ACTIVITY
    TARGET_LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    o.customer_id,
    cust.first_name || ' ' || cust.last_name AS customer_name,
    cust.email,
    cust.city,
    
    -- Activity metrics (from streaming data)
    COUNT(DISTINCT o.order_id) AS recent_orders,
    SUM(o.total_amount) AS recent_spend,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.event_timestamp) AS first_order_in_window,
    MAX(o.event_timestamp) AS last_order_in_window,
    
    -- Preferences
    MODE(o.order_type) AS preferred_order_type,
    MODE(o.payment_method) AS preferred_payment,
    MODE(o.location_id) AS preferred_location,
    
    -- Activity level
    CASE 
        WHEN COUNT(DISTINCT o.order_id) >= 5 THEN 'HIGH'
        WHEN COUNT(DISTINCT o.order_id) >= 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS activity_level

FROM DT_SILVER_ORDERS o
JOIN DIM_CUSTOMER cust ON o.customer_id = cust.customer_id
WHERE o.is_valid_order = TRUE
GROUP BY o.customer_id, cust.first_name, cust.last_name, cust.email, cust.city;


-- ============================================================
-- PART 2: DYNAMIC TABLE MONITORING & MANAGEMENT
-- ============================================================

-- View all Dynamic Tables
SHOW DYNAMIC TABLES IN SCHEMA BELLA_NAPOLI;

-- Check refresh history
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME LIKE 'DT_%'
ORDER BY REFRESH_START_TIME DESC
LIMIT 20;

-- Check current lag status
SELECT 
    name,
    target_lag,
    refresh_mode,
    scheduling_state,
    last_refresh_time,
    next_scheduled_refresh_time,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE NAME LIKE 'DT_%';

-- Manually refresh a specific Dynamic Table (if needed)
ALTER DYNAMIC TABLE DT_GOLD_DAILY_SALES REFRESH;

-- Suspend/Resume Dynamic Tables
-- ALTER DYNAMIC TABLE DT_GOLD_DAILY_SALES SUSPEND;
-- ALTER DYNAMIC TABLE DT_GOLD_DAILY_SALES RESUME;


-- ============================================================
-- PART 3: QUERYING DYNAMIC TABLES
-- ============================================================

-- Real-time daily dashboard
SELECT 
    order_date,
    location_name,
    total_orders,
    ROUND(total_revenue, 2) AS revenue,
    ROUND(avg_order_value, 2) AS aov,
    delivery_orders,
    pickup_orders,
    dine_in_orders,
    last_refreshed
FROM DT_GOLD_DAILY_SALES
ORDER BY order_date DESC, total_revenue DESC;

-- Current hour performance (near-real-time)
SELECT 
    location_name,
    orders,
    ROUND(revenue, 2) AS revenue,
    period_type
FROM DT_GOLD_HOURLY_SALES
WHERE order_date = CURRENT_DATE()
AND order_hour = HOUR(CURRENT_TIMESTAMP())
ORDER BY revenue DESC;

-- Top selling items today
SELECT 
    item_name,
    category_name,
    SUM(quantity_sold) AS total_sold,
    ROUND(SUM(item_revenue), 2) AS revenue,
    ROUND(SUM(estimated_profit), 2) AS profit
FROM DT_GOLD_ITEM_PERFORMANCE
WHERE order_date = CURRENT_DATE()
GROUP BY item_name, category_name
ORDER BY total_sold DESC
LIMIT 10;

-- High-activity customers (potential VIPs)
SELECT 
    customer_name,
    email,
    recent_orders,
    ROUND(recent_spend, 2) AS spend,
    preferred_order_type,
    activity_level
FROM DT_GOLD_CUSTOMER_ACTIVITY
WHERE activity_level = 'HIGH'
ORDER BY recent_spend DESC;


-- ============================================================
-- PART 4: DYNAMIC TABLE PIPELINE VISUALIZATION
-- ============================================================

/*
PIPELINE ARCHITECTURE:

    ┌─────────────────────┐
    │  BRONZE_ORDER_EVENTS │  ← Raw events from POS/Web/Mobile
    │    (Source Table)    │
    └──────────┬──────────┘
               │
               ▼
    ┌─────────────────────┐
    │  DT_SILVER_ORDERS   │  ← Cleaned, normalized (1 min lag)
    │   (Dynamic Table)    │
    └──────────┬──────────┘
               │
       ┌───────┴───────┐
       │               │
       ▼               ▼
┌──────────────┐ ┌─────────────────┐
│DT_SILVER_    │ │                 │
│ORDER_ITEMS   │ │                 │
│(1 min lag)   │ │                 │
└──────┬───────┘ │                 │
       │         │                 │
       ▼         ▼                 ▼
┌──────────────┐ ┌──────────────┐ ┌────────────────┐
│DT_GOLD_ITEM_ │ │DT_GOLD_DAILY_│ │DT_GOLD_CUSTOMER│
│PERFORMANCE   │ │SALES         │ │_ACTIVITY       │
│(5 min lag)   │ │(5 min lag)   │ │(5 min lag)     │
└──────────────┘ └──────────────┘ └────────────────┘
                        │
                        ▼
                 ┌──────────────┐
                 │DT_GOLD_HOURLY│
                 │_SALES        │
                 │(1 min lag)   │
                 └──────────────┘

KEY BENEFITS:
• Declarative pipeline - just write SELECT statements
• Automatic incremental refresh
• Dependency tracking handled by Snowflake
• No Task/Stream management needed
• Perfect for medallion architecture
*/

COMMIT;
