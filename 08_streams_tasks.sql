-- ============================================================
-- SNOWFLAKE INTELLIGENCE DEMO: PIZZERIA BELLA NAPOLI
-- Script 8: Streams and Tasks
-- ============================================================
-- Streams capture change data (CDC) from tables
-- Tasks schedule and automate SQL execution
-- Together they enable real-time data pipelines
-- ============================================================

USE DATABASE PIZZERIA_DEMO;
USE SCHEMA BELLA_NAPOLI;

-- ============================================================
-- PART 1: STREAMS - Change Data Capture
-- ============================================================

-- --------------------------------------------------------
-- 1.1 STANDARD STREAM on Orders Table
-- Captures INSERT, UPDATE, DELETE operations
-- --------------------------------------------------------

-- Create stream to track changes on orders
CREATE OR REPLACE STREAM STREAM_ORDER_CHANGES 
    ON TABLE FACT_ORDER
    APPEND_ONLY = FALSE  -- Track all DML types
    SHOW_INITIAL_ROWS = FALSE  -- Don't include existing rows
    COMMENT = 'Captures all changes to order data for CDC pipeline';

-- Create append-only stream (more efficient for insert-heavy tables)
CREATE OR REPLACE STREAM STREAM_NEW_ORDERS
    ON TABLE FACT_ORDER
    APPEND_ONLY = TRUE  -- Only track INSERTs
    COMMENT = 'Captures only new orders for real-time notifications';


-- --------------------------------------------------------
-- 1.2 STREAM on Reviews for Sentiment Processing
-- --------------------------------------------------------

CREATE OR REPLACE STREAM STREAM_NEW_REVIEWS
    ON TABLE FACT_REVIEW
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new reviews for sentiment analysis pipeline';


-- --------------------------------------------------------
-- 1.3 STREAM on Customer Table for CRM Updates
-- --------------------------------------------------------

CREATE OR REPLACE STREAM STREAM_CUSTOMER_CHANGES
    ON TABLE DIM_CUSTOMER
    APPEND_ONLY = FALSE
    COMMENT = 'Captures customer profile changes for CRM sync';


-- --------------------------------------------------------
-- 1.4 STREAM on Inventory for Alert System
-- --------------------------------------------------------

CREATE OR REPLACE STREAM STREAM_INVENTORY_CHANGES
    ON TABLE FACT_INVENTORY
    APPEND_ONLY = FALSE
    COMMENT = 'Monitors inventory changes for low-stock alerts';


-- --------------------------------------------------------
-- 1.5 Test Streams by Making Changes
-- --------------------------------------------------------

-- Insert test orders to generate stream data
INSERT INTO FACT_ORDER (
    order_id, customer_id, employee_id, location_id, order_timestamp,
    order_type, subtotal, tax_amount, tip_amount, discount_amount,
    total_amount, payment_method, order_status
)
SELECT 
    (SELECT MAX(order_id) + ROW_NUMBER() OVER (ORDER BY 1) FROM FACT_ORDER) AS order_id,
    UNIFORM(1, 100, RANDOM()) AS customer_id,
    UNIFORM(1, 10, RANDOM()) AS employee_id,
    UNIFORM(1, 3, RANDOM()) AS location_id,
    CURRENT_TIMESTAMP() AS order_timestamp,
    CASE UNIFORM(1, 3, RANDOM()) 
        WHEN 1 THEN 'DELIVERY' 
        WHEN 2 THEN 'PICKUP' 
        ELSE 'DINE_IN' 
    END AS order_type,
    ROUND(UNIFORM(15, 75, RANDOM()), 2) AS subtotal,
    ROUND(UNIFORM(15, 75, RANDOM()) * 0.0825, 2) AS tax_amount,
    ROUND(UNIFORM(0, 10, RANDOM()), 2) AS tip_amount,
    0 AS discount_amount,
    ROUND(UNIFORM(15, 75, RANDOM()) * 1.0825, 2) AS total_amount,
    'CREDIT' AS payment_method,
    'PENDING' AS order_status
FROM TABLE(GENERATOR(ROWCOUNT => 5));

-- Check stream contents (before consuming)
SELECT * FROM STREAM_NEW_ORDERS LIMIT 10;

-- Stream metadata columns:
-- METADATA$ACTION: INSERT, DELETE
-- METADATA$ISUPDATE: TRUE if this is part of an UPDATE
-- METADATA$ROW_ID: Unique row identifier


-- ============================================================
-- PART 2: TARGET TABLES FOR STREAM PROCESSING
-- ============================================================

-- Processed orders log (where stream data lands)
CREATE OR REPLACE TABLE PROCESSED_ORDER_LOG (
    log_id INT AUTOINCREMENT,
    order_id INT,
    action_type VARCHAR(20),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    order_total DECIMAL(10,2),
    customer_id INT,
    location_id INT,
    notification_sent BOOLEAN DEFAULT FALSE
);

-- Sentiment analysis results
CREATE OR REPLACE TABLE REVIEW_SENTIMENT_ANALYSIS (
    analysis_id INT AUTOINCREMENT,
    review_id INT,
    review_text VARCHAR(2000),
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    topics ARRAY,
    analyzed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    requires_response BOOLEAN DEFAULT FALSE
);

-- Inventory alerts table
CREATE OR REPLACE TABLE INVENTORY_ALERTS (
    alert_id INT AUTOINCREMENT,
    location_id INT,
    ingredient_id INT,
    ingredient_name VARCHAR(100),
    current_quantity DECIMAL(10,2),
    reorder_point DECIMAL(10,2),
    alert_type VARCHAR(20),  -- LOW_STOCK, OUT_OF_STOCK, EXPIRING
    alert_status VARCHAR(20) DEFAULT 'OPEN',  -- OPEN, ACKNOWLEDGED, RESOLVED
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at TIMESTAMP_NTZ
);

-- Customer 360 change history
CREATE OR REPLACE TABLE CUSTOMER_CHANGE_HISTORY (
    change_id INT AUTOINCREMENT,
    customer_id INT,
    change_type VARCHAR(20),  -- INSERT, UPDATE, DELETE
    old_values VARIANT,
    new_values VARIANT,
    changed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ============================================================
-- PART 3: TASKS - Automated Scheduling
-- ============================================================

-- --------------------------------------------------------
-- 3.1 TASK: Process New Orders (Every Minute)
-- Consumes STREAM_NEW_ORDERS, logs and triggers notifications
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_PROCESS_NEW_ORDERS
    WAREHOUSE = COMPUTE_WH  -- Replace with your warehouse
    SCHEDULE = '1 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Process new orders from stream every minute'
WHEN
    SYSTEM$STREAM_HAS_DATA('STREAM_NEW_ORDERS')
AS
BEGIN
    -- Insert into processed log
    INSERT INTO PROCESSED_ORDER_LOG (order_id, action_type, order_total, customer_id, location_id)
    SELECT 
        order_id,
        METADATA$ACTION AS action_type,
        total_amount,
        customer_id,
        location_id
    FROM STREAM_NEW_ORDERS;
    
    -- Log the processing
    CALL SYSTEM$LOG_INFO('Processed ' || SQLROWCOUNT || ' new orders');
END;


-- --------------------------------------------------------
-- 3.2 TASK: Sentiment Analysis on New Reviews (Every 5 Min)
-- Uses Cortex LLM to analyze sentiment
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_ANALYZE_REVIEW_SENTIMENT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTES'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Analyze sentiment of new reviews using Cortex AI'
WHEN
    SYSTEM$STREAM_HAS_DATA('STREAM_NEW_REVIEWS')
AS
BEGIN
    INSERT INTO REVIEW_SENTIMENT_ANALYSIS (
        review_id, 
        review_text, 
        sentiment_score, 
        sentiment_label,
        requires_response
    )
    SELECT 
        review_id,
        review_text,
        SNOWFLAKE.CORTEX.SENTIMENT(review_text) AS sentiment_score,
        CASE 
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(review_text) >= 0.3 THEN 'POSITIVE'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(review_text) <= -0.3 THEN 'NEGATIVE'
            ELSE 'NEUTRAL'
        END AS sentiment_label,
        CASE 
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(review_text) <= -0.3 THEN TRUE
            ELSE FALSE
        END AS requires_response
    FROM STREAM_NEW_REVIEWS
    WHERE review_text IS NOT NULL;
END;


-- --------------------------------------------------------
-- 3.3 TASK: Check Inventory Levels (Every 15 Min)
-- Monitors for low stock and creates alerts
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_CHECK_INVENTORY_ALERTS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '15 MINUTES'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Check inventory levels and create alerts for low stock'
WHEN
    SYSTEM$STREAM_HAS_DATA('STREAM_INVENTORY_CHANGES')
AS
BEGIN
    -- Insert low stock alerts
    INSERT INTO INVENTORY_ALERTS (
        location_id, ingredient_id, ingredient_name, 
        current_quantity, reorder_point, alert_type
    )
    SELECT 
        inv.location_id,
        inv.ingredient_id,
        ing.ingredient_name,
        inv.quantity_on_hand,
        inv.reorder_point,
        CASE 
            WHEN inv.quantity_on_hand = 0 THEN 'OUT_OF_STOCK'
            WHEN inv.quantity_on_hand <= inv.reorder_point * 0.5 THEN 'CRITICAL'
            ELSE 'LOW_STOCK'
        END AS alert_type
    FROM STREAM_INVENTORY_CHANGES inv
    JOIN DIM_INGREDIENT ing ON inv.ingredient_id = ing.ingredient_id
    WHERE inv.quantity_on_hand <= inv.reorder_point
    AND inv.METADATA$ACTION = 'INSERT'
    AND NOT EXISTS (
        SELECT 1 FROM INVENTORY_ALERTS a 
        WHERE a.location_id = inv.location_id 
        AND a.ingredient_id = inv.ingredient_id 
        AND a.alert_status = 'OPEN'
    );
END;


-- --------------------------------------------------------
-- 3.4 TASK: Track Customer Changes (Every 10 Min)
-- Maintains change history for auditing
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_LOG_CUSTOMER_CHANGES
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTES'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
    COMMENT = 'Track all customer profile changes for audit'
WHEN
    SYSTEM$STREAM_HAS_DATA('STREAM_CUSTOMER_CHANGES')
AS
BEGIN
    INSERT INTO CUSTOMER_CHANGE_HISTORY (customer_id, change_type, new_values)
    SELECT 
        customer_id,
        CASE 
            WHEN METADATA$ISUPDATE THEN 'UPDATE'
            ELSE METADATA$ACTION
        END AS change_type,
        OBJECT_CONSTRUCT(
            'first_name', first_name,
            'last_name', last_name,
            'email', email,
            'phone', phone,
            'address', address,
            'loyalty_points', loyalty_points
        ) AS new_values
    FROM STREAM_CUSTOMER_CHANGES;
END;


-- --------------------------------------------------------
-- 3.5 TASK: Daily Sales Aggregation (Scheduled at 1 AM)
-- CRON-based scheduling for end-of-day processing
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_DAILY_SALES_AGGREGATION
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 1 * * * America/Chicago'  -- 1 AM Central
    COMMENT = 'Aggregate daily sales metrics at end of day'
AS
BEGIN
    -- Merge yesterday's data into daily summary
    MERGE INTO FACT_DAILY_SALES target
    USING (
        SELECT 
            DATE(o.order_timestamp) AS sales_date,
            o.location_id,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_revenue,
            AVG(o.total_amount) AS avg_order_value,
            SUM(CASE WHEN o.order_type = 'DINE_IN' THEN 1 ELSE 0 END) AS dine_in_orders,
            SUM(CASE WHEN o.order_type = 'PICKUP' THEN 1 ELSE 0 END) AS pickup_orders,
            SUM(CASE WHEN o.order_type = 'DELIVERY' THEN 1 ELSE 0 END) AS delivery_orders,
            SUM(oi.pizza_count) AS total_pizzas_sold,
            COUNT(DISTINCT CASE WHEN c.registration_date = DATE(o.order_timestamp) THEN c.customer_id END) AS new_customers,
            DAYOFWEEK(o.order_timestamp) IN (0, 6) AS is_weekend,
            FALSE AS is_holiday
        FROM FACT_ORDER o
        LEFT JOIN (
            SELECT order_id, SUM(CASE WHEN item_id BETWEEN 1 AND 14 THEN quantity ELSE 0 END) AS pizza_count
            FROM FACT_ORDER_ITEM GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
        LEFT JOIN DIM_CUSTOMER c ON o.customer_id = c.customer_id
        WHERE DATE(o.order_timestamp) = DATEADD(DAY, -1, CURRENT_DATE())
        GROUP BY DATE(o.order_timestamp), o.location_id
    ) source
    ON target.sales_date = source.sales_date AND target.location_id = source.location_id
    WHEN MATCHED THEN UPDATE SET
        total_orders = source.total_orders,
        total_revenue = source.total_revenue,
        avg_order_value = source.avg_order_value,
        dine_in_orders = source.dine_in_orders,
        pickup_orders = source.pickup_orders,
        delivery_orders = source.delivery_orders,
        total_pizzas_sold = source.total_pizzas_sold,
        new_customers = source.new_customers
    WHEN NOT MATCHED THEN INSERT (
        sales_date, location_id, total_orders, total_revenue, avg_order_value,
        dine_in_orders, pickup_orders, delivery_orders, total_pizzas_sold,
        new_customers, is_weekend, is_holiday
    ) VALUES (
        source.sales_date, source.location_id, source.total_orders, source.total_revenue,
        source.avg_order_value, source.dine_in_orders, source.pickup_orders, 
        source.delivery_orders, source.total_pizzas_sold, source.new_customers,
        source.is_weekend, source.is_holiday
    );
END;


-- --------------------------------------------------------
-- 3.6 TASK: Weekly Report Generation (Mondays at 6 AM)
-- --------------------------------------------------------

CREATE OR REPLACE TASK TASK_WEEKLY_REPORT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 6 * * MON America/Chicago'  -- Monday 6 AM
    COMMENT = 'Generate weekly performance summary'
AS
BEGIN
    -- Create weekly summary table
    CREATE OR REPLACE TABLE WEEKLY_PERFORMANCE_REPORT AS
    SELECT 
        DATE_TRUNC('WEEK', sales_date) AS week_starting,
        l.location_name,
        SUM(total_orders) AS weekly_orders,
        ROUND(SUM(total_revenue), 2) AS weekly_revenue,
        ROUND(AVG(avg_order_value), 2) AS avg_order_value,
        SUM(delivery_orders) AS delivery_orders,
        SUM(pickup_orders) AS pickup_orders,
        SUM(dine_in_orders) AS dine_in_orders,
        SUM(total_pizzas_sold) AS pizzas_sold,
        SUM(new_customers) AS new_customers,
        -- Week over week comparison
        LAG(SUM(total_revenue)) OVER (
            PARTITION BY l.location_name ORDER BY DATE_TRUNC('WEEK', sales_date)
        ) AS prev_week_revenue,
        ROUND(
            (SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (
                PARTITION BY l.location_name ORDER BY DATE_TRUNC('WEEK', sales_date)
            )) / NULLIF(LAG(SUM(total_revenue)) OVER (
                PARTITION BY l.location_name ORDER BY DATE_TRUNC('WEEK', sales_date)
            ), 0) * 100, 
        1) AS wow_growth_pct
    FROM FACT_DAILY_SALES ds
    JOIN DIM_LOCATION l ON ds.location_id = l.location_id
    WHERE sales_date >= DATEADD(WEEK, -8, CURRENT_DATE())
    GROUP BY DATE_TRUNC('WEEK', sales_date), l.location_name;
END;


-- --------------------------------------------------------
-- 3.7 TASK TREE: Parent-Child Dependencies
-- --------------------------------------------------------

-- Parent task that triggers child tasks
CREATE OR REPLACE TASK TASK_PARENT_ETL_ORCHESTRATOR
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTES'
    COMMENT = 'Parent task to orchestrate ETL pipeline'
AS
    SELECT 'ETL Pipeline Started at ' || CURRENT_TIMESTAMP();

-- Child task 1: Extract
CREATE OR REPLACE TASK TASK_CHILD_EXTRACT
    WAREHOUSE = COMPUTE_WH
    AFTER TASK_PARENT_ETL_ORCHESTRATOR  -- Runs after parent
    COMMENT = 'Child task - Extract phase'
AS
    SELECT 'Extract phase completed';

-- Child task 2: Transform (runs after extract)
CREATE OR REPLACE TASK TASK_CHILD_TRANSFORM
    WAREHOUSE = COMPUTE_WH
    AFTER TASK_CHILD_EXTRACT
    COMMENT = 'Child task - Transform phase'
AS
    SELECT 'Transform phase completed';

-- Child task 3: Load (runs after transform)
CREATE OR REPLACE TASK TASK_CHILD_LOAD
    WAREHOUSE = COMPUTE_WH
    AFTER TASK_CHILD_TRANSFORM
    COMMENT = 'Child task - Load phase'
AS
    SELECT 'Load phase completed';


-- ============================================================
-- PART 4: SERVERLESS TASKS
-- ============================================================

-- Serverless task (no warehouse needed - auto-scaled)
CREATE OR REPLACE TASK TASK_SERVERLESS_CLEANUP
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'  -- Serverless
    SCHEDULE = 'USING CRON 0 2 * * * America/Chicago'    -- 2 AM daily
    COMMENT = 'Serverless task for data cleanup'
AS
BEGIN
    -- Clean up old processed logs (keep 30 days)
    DELETE FROM PROCESSED_ORDER_LOG 
    WHERE processed_at < DATEADD(DAY, -30, CURRENT_DATE());
    
    -- Archive resolved alerts (keep 90 days)
    DELETE FROM INVENTORY_ALERTS 
    WHERE alert_status = 'RESOLVED' 
    AND resolved_at < DATEADD(DAY, -90, CURRENT_DATE());
END;


-- ============================================================
-- PART 5: TASK & STREAM MANAGEMENT
-- ============================================================

-- View all streams
SHOW STREAMS IN SCHEMA BELLA_NAPOLI;

-- View all tasks
SHOW TASKS IN SCHEMA BELLA_NAPOLI;

-- Check stream contents/status
SELECT SYSTEM$STREAM_HAS_DATA('STREAM_NEW_ORDERS');

-- View task run history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
ORDER BY SCHEDULED_TIME DESC;

-- Enable tasks (tasks are created in suspended state by default)
ALTER TASK TASK_CHILD_LOAD RESUME;
ALTER TASK TASK_CHILD_TRANSFORM RESUME;
ALTER TASK TASK_CHILD_EXTRACT RESUME;
ALTER TASK TASK_PARENT_ETL_ORCHESTRATOR RESUME;

ALTER TASK TASK_PROCESS_NEW_ORDERS RESUME;
ALTER TASK TASK_ANALYZE_REVIEW_SENTIMENT RESUME;
ALTER TASK TASK_CHECK_INVENTORY_ALERTS RESUME;
ALTER TASK TASK_LOG_CUSTOMER_CHANGES RESUME;
ALTER TASK TASK_DAILY_SALES_AGGREGATION RESUME;
ALTER TASK TASK_WEEKLY_REPORT RESUME;
ALTER TASK TASK_SERVERLESS_CLEANUP RESUME;

-- Suspend all tasks
-- ALTER TASK TASK_PROCESS_NEW_ORDERS SUSPEND;

-- Manually execute a task (for testing)
EXECUTE TASK TASK_PROCESS_NEW_ORDERS;

-- View task graph/dependencies
SELECT *
FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
WHERE ROOT_TASK_NAME = 'TASK_PARENT_ETL_ORCHESTRATOR';


-- ============================================================
-- PART 6: STREAM & TASK ARCHITECTURE DIAGRAM
-- ============================================================

/*
REAL-TIME DATA PIPELINE ARCHITECTURE:

    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │   POS System    │     │    Web App      │     │   Mobile App    │
    └────────┬────────┘     └────────┬────────┘     └────────┬────────┘
             │                       │                       │
             └───────────────────────┼───────────────────────┘
                                     ▼
                          ┌─────────────────────┐
                          │     FACT_ORDER      │
                          │    (Source Table)   │
                          └──────────┬──────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
           ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
           │ STREAM_ORDER_ │ │ STREAM_NEW_   │ │ STREAM_       │
           │ CHANGES       │ │ ORDERS        │ │ INVENTORY_    │
           │ (All DML)     │ │ (Append Only) │ │ CHANGES       │
           └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
                   │                 │                 │
                   ▼                 ▼                 ▼
           ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
           │ TASK_LOG_     │ │ TASK_PROCESS_ │ │ TASK_CHECK_   │
           │ CUSTOMER_     │ │ NEW_ORDERS    │ │ INVENTORY_    │
           │ CHANGES       │ │ (1 min)       │ │ ALERTS        │
           │ (10 min)      │ │               │ │ (15 min)      │
           └───────┬───────┘ └───────┬───────┘ └───────┬───────┘
                   │                 │                 │
                   ▼                 ▼                 ▼
           ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
           │ CUSTOMER_     │ │ PROCESSED_    │ │ INVENTORY_    │
           │ CHANGE_       │ │ ORDER_LOG     │ │ ALERTS        │
           │ HISTORY       │ │               │ │               │
           └───────────────┘ └───────────────┘ └───────────────┘


SCHEDULED BATCH TASKS:

    ┌─────────────────────────────────────────────────────────┐
    │                    TIME-BASED TRIGGERS                   │
    └─────────────────────────────────────────────────────────┘
                    │
        ┌───────────┼───────────┬───────────────┐
        ▼           ▼           ▼               ▼
    ┌─────────┐ ┌─────────┐ ┌─────────┐   ┌─────────────┐
    │ Daily   │ │ Weekly  │ │ Cleanup │   │ ETL Parent  │
    │ Sales   │ │ Report  │ │ Task    │   │ Task        │
    │ (1 AM)  │ │ (Mon)   │ │ (2 AM)  │   │ (30 min)    │
    └─────────┘ └─────────┘ └─────────┘   └──────┬──────┘
                                                 │
                                    ┌────────────┼────────────┐
                                    ▼            ▼            ▼
                               ┌─────────┐ ┌─────────┐ ┌─────────┐
                               │ Extract │ │Transform│ │  Load   │
                               │  Child  │→│  Child  │→│  Child  │
                               └─────────┘ └─────────┘ └─────────┘

KEY CONCEPTS:
• Streams = CDC (Change Data Capture) on tables
• Tasks = Scheduled execution of SQL/Procedures
• WHEN clause = Conditional execution based on stream data
• Task Trees = Dependency chains for complex pipelines
• Serverless Tasks = Auto-scaled, no warehouse management
*/

COMMIT;
