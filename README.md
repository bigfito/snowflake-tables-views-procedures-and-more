# Snowflake Tables, Views, Procedures and More
## Demo scripts for table and view types, dynamic tables, streams & tasks and stored procedures (Pizzeria Bella Napoli dataset)

These scripts reuse the Pizzeria Bella Napoli data model shared with [snowflake-intelligence](https://github.com/bigfito/snowflake-intelligence). That repository covers Cortex and Snowflake Intelligence (Cortex Analyst, Cortex Search, and Cortex AI/ML). This one covers platform objects: table and view types, dynamic tables, streams and tasks, and stored procedures.

Scripts `02` through `05` are not in this repository. They live in `snowflake-intelligence`: `02_sample_data_dimensions.sql`, `03_sample_data_facts.sql`, `04_semantic_model.yaml`, and `05_demo_queries.sql`. This repository keeps the shared schema (`01_ddl_schema.sql`) and continues with the platform-object scripts `06` through `09`.

## Files Included

| File | Description |
|------|-------------|
| `01_ddl_schema.sql` | Database, schema, and table definitions |
| `06_table_view_types.sql` | **All table & view types** (Permanent, Transient, Temp, External, Iceberg, Hybrid + Standard, Secure, Materialized Views) |
| `07_dynamic_tables.sql` | **Dynamic Tables** - Declarative medallion architecture pipeline |
| `08_streams_tasks.sql` | **Streams & Tasks** - CDC and scheduled automation |
| `09_stored_procedures.sql` | **Stored Procedures** - SQL, JavaScript, Python examples |

---

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
