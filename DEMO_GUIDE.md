# Snowflake Intelligence Demo Guide
## Pizzeria Bella Napoli 🍕

---

## Overview

This demo showcases **Snowflake Intelligence** capabilities using a realistic pizza restaurant dataset. The demo covers:

- **Cortex LLM Functions**: Sentiment analysis, summarization, classification, text generation
- **Cortex Analyst**: Natural language to SQL with semantic models
- **ML Functions**: Sales forecasting and anomaly detection
- **Advanced Analytics**: Customer segmentation, market basket analysis

---

## Setup Instructions

### Step 1: Create Database Objects

Run the scripts in order:

```sql
-- 1. Create schema and tables
-- Run: 01_ddl_schema.sql

-- 2. Load dimension data (categories, menu items, customers, etc.)
-- Run: 02_sample_data_dimensions.sql

-- 3. Load fact data (orders, reviews, inventory)
-- Run: 03_sample_data_facts.sql
```

### Step 2: Upload Semantic Model

```sql
-- Create stage for semantic model
CREATE STAGE IF NOT EXISTS PIZZERIA_DEMO.BELLA_NAPOLI.SEMANTIC_MODELS;

-- Upload the YAML file (run from SnowSQL CLI)
PUT file://04_semantic_model.yaml @PIZZERIA_DEMO.BELLA_NAPOLI.SEMANTIC_MODELS AUTO_COMPRESS=FALSE;
```

### Step 3: Verify Data

```sql
USE DATABASE PIZZERIA_DEMO;
USE SCHEMA BELLA_NAPOLI;

-- Check record counts
SELECT 'Orders' AS table_name, COUNT(*) AS records FROM FACT_ORDER
UNION ALL SELECT 'Order Items', COUNT(*) FROM FACT_ORDER_ITEM
UNION ALL SELECT 'Reviews', COUNT(*) FROM FACT_REVIEW
UNION ALL SELECT 'Customers', COUNT(*) FROM DIM_CUSTOMER
UNION ALL SELECT 'Menu Items', COUNT(*) FROM DIM_MENU_ITEM;
```

---

## Demo Walkthrough

### Act 1: Setting the Scene (2 minutes)

**Narrative**: *"Meet Bella Napoli, a growing pizza chain in Austin, Texas with three locations. They've been collecting data but struggling to get insights. Let's see how Snowflake Intelligence can transform their operations."*

Show the data model:
- 3 locations (Downtown, Westlake, Round Rock)
- 40 menu items across 6 categories
- ~15,000 orders over the past year
- 100 customers with varying loyalty levels
- Thousands of customer reviews

### Act 2: Understanding Customer Sentiment (5 minutes)

**Business Question**: *"What are customers really saying about us?"*

#### Demo 2.1: Sentiment Analysis

```sql
-- Analyze sentiment of recent reviews
SELECT 
    r.review_id,
    r.overall_rating,
    LEFT(r.review_text, 100) || '...' AS review_preview,
    ROUND(SNOWFLAKE.CORTEX.SENTIMENT(r.review_text), 3) AS sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(r.review_text) >= 0.3 THEN '😊 Positive'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(r.review_text) <= -0.3 THEN '😞 Negative'
        ELSE '😐 Neutral'
    END AS sentiment
FROM FACT_REVIEW r
WHERE r.review_text IS NOT NULL
ORDER BY r.review_date DESC
LIMIT 10;
```

**Key Talking Points**:
- No ML expertise required - just a SQL function
- Works on any text column
- Score ranges from -1 (negative) to +1 (positive)
- Can process millions of reviews in seconds

#### Demo 2.2: Summarize Negative Feedback

```sql
-- Get actionable summary of complaints
WITH negative_reviews AS (
    SELECT LISTAGG(review_text, ' | ') AS all_reviews
    FROM FACT_REVIEW
    WHERE overall_rating <= 2
    LIMIT 10
)
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(all_reviews) AS complaint_summary
FROM negative_reviews;
```

**Key Talking Points**:
- Instantly synthesize hundreds of reviews into actionable insights
- No need to read through each review manually
- Great for executive summaries and weekly reports

#### Demo 2.3: Auto-Classify Review Topics

```sql
-- Classify what customers are talking about
SELECT 
    LEFT(review_text, 80) AS review_preview,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        review_text,
        ['Food Quality', 'Service', 'Delivery', 'Price', 'Cleanliness']
    ):label::STRING AS topic,
    ROUND(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        review_text,
        ['Food Quality', 'Service', 'Delivery', 'Price', 'Cleanliness']
    ):probability::FLOAT, 2) AS confidence
FROM FACT_REVIEW
WHERE review_text IS NOT NULL
LIMIT 10;
```

**Key Talking Points**:
- Zero-shot classification - no training data needed
- Custom categories defined at query time
- Confidence scores help prioritize

### Act 3: AI-Powered Content Generation (5 minutes)

**Business Question**: *"How can AI help with marketing and customer communication?"*

#### Demo 3.1: Generate Menu Descriptions

```sql
-- Create compelling menu descriptions
SELECT 
    item_name,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-3-5-sonnet',
        'Write a mouth-watering 2-sentence menu description for: ' || item_name 
        || '. Ingredients: ' || description
    ) AS ai_description
FROM DIM_MENU_ITEM
WHERE category_id = 1
LIMIT 3;
```

#### Demo 3.2: Personalized Customer Emails

```sql
-- Generate personalized loyalty emails
WITH top_customer AS (
    SELECT 
        c.first_name,
        COUNT(*) AS orders,
        ROUND(SUM(o.total_amount)) AS spent
    FROM DIM_CUSTOMER c
    JOIN FACT_ORDER o ON c.customer_id = o.customer_id
    GROUP BY c.first_name
    ORDER BY spent DESC
    LIMIT 1
)
SELECT 
    first_name,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-3-5-sonnet',
        'Write a warm, personal thank-you email (3 sentences) to ' || first_name 
        || ' who has ordered ' || orders || ' times and spent $' || spent 
        || ' at our pizza restaurant. Offer them a free dessert.'
    ) AS email_content
FROM top_customer;
```

#### Demo 3.3: Auto-Respond to Negative Reviews

```sql
-- Draft professional responses to complaints
SELECT 
    review_text,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-3-5-sonnet',
        'As a restaurant manager, write a professional, empathetic 3-sentence response to this review. Acknowledge the issue and invite them back: ' 
        || review_text
    ) AS suggested_response
FROM FACT_REVIEW
WHERE overall_rating = 1
LIMIT 1;
```

**Key Talking Points**:
- AI content generation directly in SQL
- Personalization at scale
- Human review before sending (AI assists, doesn't replace)
- Consistent brand voice across all communications

### Act 4: Cortex Analyst - Talk to Your Data (5 minutes)

**Business Question**: *"What if anyone could query our data without knowing SQL?"*

#### Demo 4.1: Natural Language Queries

Open Cortex Analyst in the Snowflake UI and demonstrate:

**Easy Questions**:
- "What were our total sales last month?"
- "Which pizza is the bestseller?"
- "How many orders did we have yesterday?"

**Medium Questions**:
- "Compare revenue between our three locations"
- "What's our average order value on weekends vs weekdays?"
- "Show me the top 10 customers by total spending"

**Complex Questions**:
- "What's the trend in daily orders over the last 30 days?"
- "Which vegetarian items have the highest profit margin?"
- "What percentage of our revenue comes from delivery orders?"

**Key Talking Points**:
- Business users can self-serve analytics
- Semantic model provides business context
- Generated SQL is visible and auditable
- Reduces burden on data team

### Act 5: Predictive Analytics with ML Functions (5 minutes)

**Business Question**: *"What will sales look like next week? How should we staff?"*

#### Demo 5.1: Sales Forecasting

```sql
-- Create and run forecast
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST pizza_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FACT_DAILY_SALES'),
    TIMESTAMP_COLNAME => 'SALES_DATE',
    TARGET_COLNAME => 'TOTAL_REVENUE',
    SERIES_COLNAME => 'LOCATION_ID'
);

CALL pizza_forecast!FORECAST(FORECASTING_PERIODS => 7);

-- Show results
SELECT 
    l.location_name,
    f.ts AS date,
    DAYNAME(f.ts) AS day,
    '$' || ROUND(f.forecast) AS predicted_revenue
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) f
JOIN DIM_LOCATION l ON f.series::INT = l.location_id
ORDER BY l.location_name, f.ts;
```

#### Demo 5.2: Staffing Recommendations

```sql
-- Order volume forecast with staffing guidance
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST order_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FACT_DAILY_SALES'),
    TIMESTAMP_COLNAME => 'SALES_DATE',
    TARGET_COLNAME => 'TOTAL_ORDERS',
    SERIES_COLNAME => 'LOCATION_ID'
);

CALL order_forecast!FORECAST(FORECASTING_PERIODS => 7);

SELECT 
    l.location_name,
    f.ts AS date,
    ROUND(f.forecast) AS expected_orders,
    CASE 
        WHEN ROUND(f.forecast) >= 70 THEN '👥👥👥 Full + Extra'
        WHEN ROUND(f.forecast) >= 50 THEN '👥👥 Full Staff'
        ELSE '👥 Regular'
    END AS staffing
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) f
JOIN DIM_LOCATION l ON f.series::INT = l.location_id
ORDER BY f.ts, l.location_name;
```

**Key Talking Points**:
- No data science expertise required
- Automated model selection and tuning
- Confidence intervals included
- Forecasts per location automatically

#### Demo 5.3: Anomaly Detection

```sql
-- Find unusual sales days
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION sales_anomalies(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FACT_DAILY_SALES'),
    TIMESTAMP_COLNAME => 'SALES_DATE',
    TARGET_COLNAME => 'TOTAL_REVENUE',
    SERIES_COLNAME => 'LOCATION_ID'
);

CALL sales_anomalies!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'FACT_DAILY_SALES'),
    TIMESTAMP_COLNAME => 'SALES_DATE',
    TARGET_COLNAME => 'TOTAL_REVENUE',
    SERIES_COLNAME => 'LOCATION_ID'
);

-- Show anomalies
SELECT 
    l.location_name,
    a.ts AS date,
    '$' || ROUND(a.y) AS actual,
    '$' || ROUND(a.forecast) AS expected,
    CASE WHEN a.y > a.forecast THEN '📈 Above' ELSE '📉 Below' END AS direction
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) a
JOIN DIM_LOCATION l ON a.series::INT = l.location_id
WHERE a.is_anomaly = TRUE
ORDER BY ABS(a.y - a.forecast) DESC
LIMIT 5;
```

**Key Talking Points**:
- Automatic detection of unusual patterns
- Great for fraud detection, operations monitoring
- Explains what "normal" looks like

### Act 6: Business Intelligence Queries (3 minutes)

#### Customer Segmentation (RFM)

```sql
-- Quick RFM segmentation
WITH rfm AS (
    SELECT 
        c.first_name || ' ' || c.last_name AS customer,
        DATEDIFF(DAY, MAX(o.order_timestamp), CURRENT_DATE()) AS recency,
        COUNT(*) AS frequency,
        SUM(o.total_amount) AS monetary
    FROM DIM_CUSTOMER c
    JOIN FACT_ORDER o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, customer
)
SELECT 
    customer,
    CASE 
        WHEN recency < 30 AND frequency >= 5 AND monetary >= 200 THEN '⭐ Champion'
        WHEN recency < 60 AND frequency >= 3 THEN '💚 Loyal'
        WHEN recency > 90 AND monetary >= 100 THEN '⚠️ At Risk'
        ELSE '👋 Regular'
    END AS segment,
    recency || ' days ago' AS last_order,
    frequency || ' orders' AS total_orders,
    '$' || ROUND(monetary) AS lifetime_value
FROM rfm
ORDER BY monetary DESC
LIMIT 10;
```

---

## Demo Tips

### Before the Demo
- [ ] Run all setup scripts and verify data
- [ ] Test each query at least once
- [ ] Have backup screenshots ready
- [ ] Know your audience's technical level

### During the Demo
- Start with the business problem, not the technology
- Use real numbers from the queries to tell a story
- Pause after key moments for questions
- Have the semantic model YAML visible to explain structure

### Common Questions & Answers

**Q: How much does this cost?**
A: Cortex LLM functions are billed per token. ML functions use serverless compute. Check current pricing at snowflake.com/pricing.

**Q: How accurate is the forecasting?**
A: Accuracy depends on data quality and patterns. The model automatically selects the best algorithm. Confidence intervals show uncertainty.

**Q: Can we use our own LLM?**
A: Currently, Cortex uses models hosted by Snowflake partners (Anthropic, Meta, Mistral). External model integration is on the roadmap.

**Q: How long does it take to set up Cortex Analyst?**
A: The semantic model YAML takes a few hours to create properly. After that, users can query immediately.

---

## Files Included

| File | Description |
|------|-------------|
| `01_ddl_schema.sql` | Database, schema, and table definitions |
| `02_sample_data_dimensions.sql` | Dimension table data (menu, customers, locations) |
| `03_sample_data_facts.sql` | Fact table data (orders, reviews, inventory) |
| `04_semantic_model.yaml` | Cortex Analyst semantic model |
| `05_demo_queries.sql` | All demo queries organized by feature |
| `06_table_view_types.sql` | **All table & view types** (Permanent, Transient, Temp, External, Iceberg, Hybrid + Standard, Secure, Materialized Views) |
| `07_dynamic_tables.sql` | **Dynamic Tables** - Declarative medallion architecture pipeline |
| `08_streams_tasks.sql` | **Streams & Tasks** - CDC and scheduled automation |
| `09_stored_procedures.sql` | **Stored Procedures** - SQL, JavaScript, Python examples |
| `streamlit_demo.py` | Interactive Streamlit app (deployable as SiS) |
| `DEMO_GUIDE.md` | This document |

---

## Additional Demo Sections

### Table Types Showcase (Script 06)

| Table Type | Time Travel | Use Case | Demo Example |
|------------|-------------|----------|--------------|
| **Permanent** | Yes (90 days) | Core business data | `FACT_ORDER`, `DIM_CUSTOMER` |
| **Transient** | Yes (0-1 day) | ETL staging, temp processing | `STG_ONLINE_ORDERS` |
| **Temporary** | No | Session-scoped calculations | `TEMP_DAILY_METRICS` |
| **External** | No | Query external cloud storage | `EXT_DOORDASH_ORDERS` |
| **Iceberg** | Yes | Open format, multi-engine | `ICE_ORDER_ANALYTICS` |
| **Hybrid** | Yes | HTAP, fast point lookups | `HYB_ORDER_STATUS` |

### View Types Showcase (Script 06)

| View Type | Stored Results | Use Case | Demo Example |
|-----------|----------------|----------|--------------|
| **Standard** | No | Simplify queries | `V_ORDER_SUMMARY` |
| **Secure** | No | Hide logic, data sharing | `V_FRANCHISE_PERFORMANCE` |
| **Materialized** | Yes | Pre-computed aggregations | `MV_HOURLY_SALES` |

### Dynamic Tables (Script 07)

Demonstrates a complete **Bronze → Silver → Gold medallion architecture**:

```
BRONZE_ORDER_EVENTS (source)
        ↓
DT_SILVER_ORDERS (1 min lag)
        ↓
    ┌───┴───┐
    ↓       ↓
DT_GOLD_DAILY_SALES    DT_GOLD_ITEM_PERFORMANCE
   (5 min lag)              (5 min lag)
```

**Key Talking Points:**
- Declarative pipelines - just write SELECT statements
- Automatic incremental refresh
- Dependency tracking handled by Snowflake
- No Task/Stream management needed

### Streams & Tasks (Script 08)

| Stream | Purpose |
|--------|---------|
| `STREAM_NEW_ORDERS` | Capture new orders for notifications |
| `STREAM_NEW_REVIEWS` | Trigger sentiment analysis |
| `STREAM_INVENTORY_CHANGES` | Monitor low stock alerts |

| Task | Schedule | Action |
|------|----------|--------|
| `TASK_PROCESS_NEW_ORDERS` | 1 min | Log orders, trigger notifications |
| `TASK_ANALYZE_REVIEW_SENTIMENT` | 5 min | Run Cortex sentiment analysis |
| `TASK_DAILY_SALES_AGGREGATION` | 1 AM daily | End-of-day aggregations |
| `TASK_WEEKLY_REPORT` | Mon 6 AM | Generate weekly summary |

**Task Tree Demo:** Shows parent-child dependencies for ETL orchestration.

### Stored Procedures (Script 09)

| Procedure | Language | Purpose |
|-----------|----------|---------|
| `SP_PROCESS_DAILY_SALES` | SQL | Daily aggregation with loops |
| `SP_UPDATE_LOYALTY_POINTS` | SQL | Calculate & update loyalty |
| `SP_VALIDATE_ORDER_JSON` | JavaScript | JSON parsing & validation |
| `SP_GENERATE_EMAIL_CONTENT` | JavaScript | Dynamic email templating |
| `SP_CALCULATE_RFM_SEGMENTS` | Python | Customer segmentation ML |
| `SP_DETECT_SALES_ANOMALIES` | Python | Statistical anomaly detection |
| `SP_RECOMMEND_ITEMS` | Python | Recommendation engine |
| `SP_RUN_DATA_QUALITY_CHECKS` | SQL | Automated DQ framework |

---

## Resources

- [Snowflake Cortex Documentation](https://docs.snowflake.com/en/guides-overview-ai-features)
- [Cortex Analyst Guide](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [ML Functions Reference](https://docs.snowflake.com/en/guides-overview-ml-functions)
- [LLM Functions Reference](https://docs.snowflake.com/en/sql-reference/functions/complete-snowflake-cortex)

---

*Demo created for Snowflake Intelligence showcase. Data is synthetic and for demonstration purposes only.*
