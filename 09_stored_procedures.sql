-- ============================================================
-- SNOWFLAKE INTELLIGENCE DEMO: PIZZERIA BELLA NAPOLI
-- Script 9: Stored Procedures
-- ============================================================
-- Demonstrates all stored procedure types:
-- 1. SQL Scripting (Snowflake Scripting)
-- 2. JavaScript
-- 3. Python
-- 4. Java (Scala)
-- ============================================================

USE DATABASE PIZZERIA_DEMO;
USE SCHEMA BELLA_NAPOLI;

-- ============================================================
-- PART 1: SQL SCRIPTING STORED PROCEDURES
-- Native Snowflake procedural language
-- ============================================================

-- --------------------------------------------------------
-- 1.1 Process Daily Sales Summary
-- Demonstrates: Variables, loops, conditional logic, DML
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_PROCESS_DAILY_SALES(
    P_DATE DATE DEFAULT CURRENT_DATE()
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Calculates and inserts daily sales summary for a given date'
AS
DECLARE
    v_total_orders INT;
    v_total_revenue DECIMAL(12,2);
    v_locations_processed INT DEFAULT 0;
    v_result VARCHAR;
BEGIN
    -- Process each location
    FOR location_rec IN (SELECT location_id, location_name FROM DIM_LOCATION) DO
        
        -- Calculate metrics for this location
        SELECT 
            COUNT(DISTINCT order_id),
            COALESCE(SUM(total_amount), 0)
        INTO v_total_orders, v_total_revenue
        FROM FACT_ORDER
        WHERE DATE(order_timestamp) = :P_DATE
        AND location_id = location_rec.location_id;
        
        -- Insert or update daily sales
        MERGE INTO FACT_DAILY_SALES target
        USING (
            SELECT 
                :P_DATE AS sales_date,
                location_rec.location_id AS location_id,
                :v_total_orders AS total_orders,
                :v_total_revenue AS total_revenue,
                CASE WHEN :v_total_orders > 0 
                    THEN :v_total_revenue / :v_total_orders 
                    ELSE 0 
                END AS avg_order_value,
                DAYOFWEEK(:P_DATE) IN (0, 6) AS is_weekend
        ) source
        ON target.sales_date = source.sales_date 
        AND target.location_id = source.location_id
        WHEN MATCHED THEN UPDATE SET
            total_orders = source.total_orders,
            total_revenue = source.total_revenue,
            avg_order_value = source.avg_order_value
        WHEN NOT MATCHED THEN INSERT (
            sales_date, location_id, total_orders, total_revenue, avg_order_value, is_weekend
        ) VALUES (
            source.sales_date, source.location_id, source.total_orders, 
            source.total_revenue, source.avg_order_value, source.is_weekend
        );
        
        v_locations_processed := v_locations_processed + 1;
        
    END FOR;
    
    v_result := 'Processed ' || v_locations_processed || ' locations for ' || P_DATE;
    RETURN v_result;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error: ' || SQLERRM;
END;

-- Test the procedure
CALL SP_PROCESS_DAILY_SALES(CURRENT_DATE() - 1);


-- --------------------------------------------------------
-- 1.2 Customer Loyalty Points Calculator
-- Demonstrates: Output parameters, calculations, transactions
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_UPDATE_LOYALTY_POINTS(
    P_CUSTOMER_ID INT,
    P_ORDER_TOTAL DECIMAL(10,2),
    P_POINTS_EARNED INT DEFAULT NULL
)
RETURNS TABLE (
    customer_id INT,
    points_before INT,
    points_earned INT,
    points_after INT,
    loyalty_tier VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Updates customer loyalty points based on order total'
AS
DECLARE
    v_current_points INT;
    v_points_to_add INT;
    v_new_total INT;
    v_tier VARCHAR;
    res RESULTSET;
BEGIN
    -- Get current points
    SELECT loyalty_points INTO v_current_points
    FROM DIM_CUSTOMER
    WHERE customer_id = :P_CUSTOMER_ID;
    
    -- Calculate points (1 point per dollar, or use provided value)
    IF (P_POINTS_EARNED IS NOT NULL) THEN
        v_points_to_add := P_POINTS_EARNED;
    ELSE
        v_points_to_add := FLOOR(P_ORDER_TOTAL);
    END IF;
    
    v_new_total := v_current_points + v_points_to_add;
    
    -- Determine tier
    CASE 
        WHEN v_new_total >= 1000 THEN v_tier := 'GOLD';
        WHEN v_new_total >= 500 THEN v_tier := 'SILVER';
        WHEN v_new_total >= 100 THEN v_tier := 'BRONZE';
        ELSE v_tier := 'MEMBER';
    END CASE;
    
    -- Update customer
    UPDATE DIM_CUSTOMER
    SET loyalty_points = :v_new_total
    WHERE customer_id = :P_CUSTOMER_ID;
    
    -- Return results
    res := (
        SELECT 
            :P_CUSTOMER_ID AS customer_id,
            :v_current_points AS points_before,
            :v_points_to_add AS points_earned,
            :v_new_total AS points_after,
            :v_tier AS loyalty_tier
    );
    
    RETURN TABLE(res);
END;

-- Test the procedure
CALL SP_UPDATE_LOYALTY_POINTS(1, 45.99);


-- --------------------------------------------------------
-- 1.3 Generate Promotional Discounts
-- Demonstrates: Complex business logic, date handling
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_GENERATE_PROMOTIONS(
    P_START_DATE DATE,
    P_END_DATE DATE,
    P_DISCOUNT_PERCENT INT DEFAULT 15
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_promo_count INT DEFAULT 0;
BEGIN
    -- Create promotions table if not exists
    CREATE TABLE IF NOT EXISTS PROMOTIONS (
        promo_id INT AUTOINCREMENT,
        promo_code VARCHAR(20),
        description VARCHAR(200),
        discount_percent INT,
        start_date DATE,
        end_date DATE,
        target_segment VARCHAR(50),
        min_order_value DECIMAL(8,2),
        max_uses INT,
        current_uses INT DEFAULT 0,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    
    -- Lapsed customer promotion (no order in 60+ days)
    INSERT INTO PROMOTIONS (promo_code, description, discount_percent, start_date, end_date, target_segment, min_order_value, max_uses)
    SELECT 
        'COMEBACK' || TO_VARCHAR(:P_START_DATE, 'MMDD') AS promo_code,
        'We miss you! ' || :P_DISCOUNT_PERCENT || '% off your next order' AS description,
        :P_DISCOUNT_PERCENT,
        :P_START_DATE,
        :P_END_DATE,
        'LAPSED_CUSTOMERS',
        15.00,
        1000;
    
    v_promo_count := v_promo_count + 1;
    
    -- Birthday promotion
    INSERT INTO PROMOTIONS (promo_code, description, discount_percent, start_date, end_date, target_segment, min_order_value, max_uses)
    SELECT 
        'BDAY' || TO_VARCHAR(:P_START_DATE, 'MMDD'),
        'Happy Birthday! Free dessert with any order',
        100,  -- 100% off dessert
        :P_START_DATE,
        :P_END_DATE,
        'BIRTHDAY_MONTH',
        20.00,
        500;
    
    v_promo_count := v_promo_count + 1;
    
    -- VIP customer promotion
    INSERT INTO PROMOTIONS (promo_code, description, discount_percent, start_date, end_date, target_segment, min_order_value, max_uses)
    SELECT 
        'VIP' || TO_VARCHAR(:P_START_DATE, 'MMDD'),
        'VIP Exclusive: ' || (:P_DISCOUNT_PERCENT + 5) || '% off',
        :P_DISCOUNT_PERCENT + 5,
        :P_START_DATE,
        :P_END_DATE,
        'VIP_CUSTOMERS',
        30.00,
        200;
    
    v_promo_count := v_promo_count + 1;
    
    RETURN 'Created ' || v_promo_count || ' promotions for ' || P_START_DATE || ' to ' || P_END_DATE;
END;

-- Test
CALL SP_GENERATE_PROMOTIONS(CURRENT_DATE(), CURRENT_DATE() + 30, 20);


-- ============================================================
-- PART 2: JAVASCRIPT STORED PROCEDURES
-- For complex string manipulation, JSON handling, external calls
-- ============================================================

-- --------------------------------------------------------
-- 2.1 Parse and Validate Order JSON
-- Demonstrates: JSON parsing, validation, error handling
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_VALIDATE_ORDER_JSON(ORDER_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'Validates incoming order JSON and returns parsed result or errors'
AS
$$
    var result = {
        is_valid: true,
        errors: [],
        parsed_order: null
    };
    
    try {
        // Parse JSON
        var order = JSON.parse(ORDER_JSON);
        
        // Validate required fields
        var requiredFields = ['customer_id', 'items', 'order_type'];
        requiredFields.forEach(function(field) {
            if (!order.hasOwnProperty(field) || order[field] === null) {
                result.errors.push('Missing required field: ' + field);
                result.is_valid = false;
            }
        });
        
        // Validate items array
        if (order.items && Array.isArray(order.items)) {
            if (order.items.length === 0) {
                result.errors.push('Order must contain at least one item');
                result.is_valid = false;
            }
            
            order.items.forEach(function(item, index) {
                if (!item.item_id) {
                    result.errors.push('Item ' + (index + 1) + ' missing item_id');
                    result.is_valid = false;
                }
                if (!item.quantity || item.quantity <= 0) {
                    result.errors.push('Item ' + (index + 1) + ' has invalid quantity');
                    result.is_valid = false;
                }
            });
        }
        
        // Validate order type
        var validTypes = ['DELIVERY', 'PICKUP', 'DINE_IN'];
        if (order.order_type && validTypes.indexOf(order.order_type) === -1) {
            result.errors.push('Invalid order_type. Must be: ' + validTypes.join(', '));
            result.is_valid = false;
        }
        
        // Validate delivery address for delivery orders
        if (order.order_type === 'DELIVERY' && !order.delivery_address) {
            result.errors.push('Delivery orders require delivery_address');
            result.is_valid = false;
        }
        
        // Calculate totals if valid
        if (result.is_valid) {
            var subtotal = 0;
            order.items.forEach(function(item) {
                subtotal += (item.price || 0) * item.quantity;
            });
            
            order.subtotal = subtotal;
            order.tax = Math.round(subtotal * 0.0825 * 100) / 100;
            order.total = Math.round((subtotal + order.tax) * 100) / 100;
            
            result.parsed_order = order;
        }
        
    } catch (e) {
        result.is_valid = false;
        result.errors.push('JSON parsing error: ' + e.message);
    }
    
    return result;
$$;

-- Test with valid order
CALL SP_VALIDATE_ORDER_JSON('{
    "customer_id": 1,
    "order_type": "DELIVERY",
    "delivery_address": "123 Main St, Austin TX",
    "items": [
        {"item_id": 1, "item_name": "Pepperoni", "quantity": 2, "price": 16.99},
        {"item_id": 15, "item_name": "Garlic Knots", "quantity": 1, "price": 6.99}
    ]
}');

-- Test with invalid order
CALL SP_VALIDATE_ORDER_JSON('{"customer_id": null, "items": []}');


-- --------------------------------------------------------
-- 2.2 Generate Customer Email Content
-- Demonstrates: String templating, dynamic content
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_GENERATE_EMAIL_CONTENT(
    CUSTOMER_ID INT,
    EMAIL_TYPE VARCHAR  -- 'WELCOME', 'ORDER_CONFIRMATION', 'LOYALTY_UPDATE', 'WINBACK'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Query customer data
    var customerQuery = `
        SELECT 
            c.first_name,
            c.last_name,
            c.email,
            c.loyalty_points,
            c.registration_date,
            COUNT(DISTINCT o.order_id) as total_orders,
            COALESCE(SUM(o.total_amount), 0) as lifetime_value,
            MAX(o.order_timestamp) as last_order_date
        FROM DIM_CUSTOMER c
        LEFT JOIN FACT_ORDER o ON c.customer_id = o.customer_id
        WHERE c.customer_id = ${CUSTOMER_ID}
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email, 
                 c.loyalty_points, c.registration_date
    `;
    
    var stmt = snowflake.createStatement({sqlText: customerQuery});
    var result = stmt.execute();
    
    if (!result.next()) {
        return {success: false, error: 'Customer not found'};
    }
    
    var customer = {
        firstName: result.getColumnValue('FIRST_NAME'),
        lastName: result.getColumnValue('LAST_NAME'),
        email: result.getColumnValue('EMAIL'),
        loyaltyPoints: result.getColumnValue('LOYALTY_POINTS'),
        totalOrders: result.getColumnValue('TOTAL_ORDERS'),
        lifetimeValue: result.getColumnValue('LIFETIME_VALUE')
    };
    
    // Email templates
    var templates = {
        'WELCOME': {
            subject: 'Welcome to Bella Napoli, ' + customer.firstName + '! 🍕',
            body: `Dear ${customer.firstName},\n\n` +
                  `Welcome to the Bella Napoli family! We're thrilled to have you.\n\n` +
                  `As a welcome gift, enjoy 15% off your first order with code: WELCOME15\n\n` +
                  `Start earning loyalty points with every purchase - you already have ${customer.loyaltyPoints} points!\n\n` +
                  `Buon appetito!\n` +
                  `The Bella Napoli Team`
        },
        'ORDER_CONFIRMATION': {
            subject: 'Your Bella Napoli Order is Confirmed! 🎉',
            body: `Hi ${customer.firstName},\n\n` +
                  `Great news - we've received your order and our kitchen is firing up the ovens!\n\n` +
                  `You now have ${customer.loyaltyPoints} loyalty points. Keep ordering to unlock rewards!\n\n` +
                  `Track your order in the Bella Napoli app.\n\n` +
                  `Grazie mille!\n` +
                  `The Bella Napoli Team`
        },
        'LOYALTY_UPDATE': {
            subject: customer.firstName + ', You\'ve Earned More Points! ⭐',
            body: `Hey ${customer.firstName}!\n\n` +
                  `Your loyalty is paying off! You now have ${customer.loyaltyPoints} points.\n\n` +
                  `${customer.loyaltyPoints >= 500 ? '🎊 Congratulations! You\'re a Silver member!' : 
                     'Just ' + (500 - customer.loyaltyPoints) + ' more points until Silver status!'}\n\n` +
                  `You've ordered ${customer.totalOrders} times and we appreciate every single one.\n\n` +
                  `Keep those points coming!\n` +
                  `The Bella Napoli Team`
        },
        'WINBACK': {
            subject: 'We Miss You, ' + customer.firstName + '! 😢🍕',
            body: `Dear ${customer.firstName},\n\n` +
                  `It's been a while since your last slice! We miss seeing you.\n\n` +
                  `Here's 20% off your next order to welcome you back: MISSYOU20\n\n` +
                  `Your ${customer.loyaltyPoints} loyalty points are waiting for you!\n\n` +
                  `Come back soon - your favorite pizza is calling!\n\n` +
                  `Warmly,\n` +
                  `The Bella Napoli Team`
        }
    };
    
    var template = templates[EMAIL_TYPE] || templates['WELCOME'];
    
    return {
        success: true,
        email_type: EMAIL_TYPE,
        recipient: customer.email,
        subject: template.subject,
        body: template.body,
        customer_data: customer
    };
$$;

-- Test
CALL SP_GENERATE_EMAIL_CONTENT(1, 'LOYALTY_UPDATE');


-- ============================================================
-- PART 3: PYTHON STORED PROCEDURES
-- For data science, ML, complex transformations
-- ============================================================

-- --------------------------------------------------------
-- 3.1 Customer RFM Segmentation
-- Demonstrates: Pandas, NumPy, ML-style scoring
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_CALCULATE_RFM_SEGMENTS()
RETURNS TABLE (
    customer_id INT,
    customer_name VARCHAR,
    recency_days INT,
    frequency INT,
    monetary FLOAT,
    r_score INT,
    f_score INT,
    m_score INT,
    rfm_segment VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'calculate_rfm'
EXECUTE AS CALLER
COMMENT = 'Calculates RFM segmentation for all customers using Python'
AS
$$
import pandas as pd
import numpy as np
from snowflake.snowpark import Session

def calculate_rfm(session: Session):
    # Query customer order data
    query = """
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            DATEDIFF(DAY, MAX(o.order_timestamp), CURRENT_TIMESTAMP()) AS recency_days,
            COUNT(DISTINCT o.order_id) AS frequency,
            COALESCE(SUM(o.total_amount), 0) AS monetary
        FROM DIM_CUSTOMER c
        LEFT JOIN FACT_ORDER o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
    """
    
    df = session.sql(query).to_pandas()
    
    # Handle customers with no orders
    df['recency_days'] = df['recency_days'].fillna(999)
    df['frequency'] = df['frequency'].fillna(0)
    df['monetary'] = df['monetary'].fillna(0)
    
    # Calculate RFM scores using quintiles (1-5)
    # For recency, lower is better so we reverse the score
    df['r_score'] = pd.qcut(df['recency_days'], q=5, labels=[5, 4, 3, 2, 1], duplicates='drop').astype(int)
    df['f_score'] = pd.qcut(df['frequency'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5], duplicates='drop').astype(int)
    df['m_score'] = pd.qcut(df['monetary'].rank(method='first'), q=5, labels=[1, 2, 3, 4, 5], duplicates='drop').astype(int)
    
    # Define segments based on RFM scores
    def get_segment(row):
        r, f, m = row['r_score'], row['f_score'], row['m_score']
        
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal Customers'
        elif r >= 4 and f <= 2:
            return 'New Customers'
        elif r <= 2 and f >= 3:
            return 'At Risk'
        elif r <= 2 and f <= 2 and m >= 3:
            return 'Cant Lose Them'
        elif r <= 2 and f <= 2 and m <= 2:
            return 'Lost'
        elif r >= 3 and f >= 2:
            return 'Potential Loyalists'
        else:
            return 'Need Attention'
    
    df['rfm_segment'] = df.apply(get_segment, axis=1)
    
    # Select and rename columns for output
    result_df = df[[
        'CUSTOMER_ID', 'CUSTOMER_NAME', 'RECENCY_DAYS', 'FREQUENCY', 
        'MONETARY', 'r_score', 'f_score', 'm_score', 'rfm_segment'
    ]]
    
    result_df.columns = [
        'CUSTOMER_ID', 'CUSTOMER_NAME', 'RECENCY_DAYS', 'FREQUENCY',
        'MONETARY', 'R_SCORE', 'F_SCORE', 'M_SCORE', 'RFM_SEGMENT'
    ]
    
    return session.create_dataframe(result_df)
$$;

-- Run RFM segmentation
CALL SP_CALCULATE_RFM_SEGMENTS();


-- --------------------------------------------------------
-- 3.2 Anomaly Detection in Sales Data
-- Demonstrates: Statistical analysis, outlier detection
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_DETECT_SALES_ANOMALIES(
    P_LOOKBACK_DAYS INT DEFAULT 90,
    P_THRESHOLD_STDDEV FLOAT DEFAULT 2.0
)
RETURNS TABLE (
    anomaly_date DATE,
    location_id INT,
    location_name VARCHAR,
    actual_revenue FLOAT,
    expected_revenue FLOAT,
    std_dev FLOAT,
    z_score FLOAT,
    anomaly_type VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy', 'scipy')
HANDLER = 'detect_anomalies'
EXECUTE AS CALLER
AS
$$
import pandas as pd
import numpy as np
from scipy import stats
from snowflake.snowpark import Session

def detect_anomalies(session: Session, p_lookback_days: int, p_threshold_stddev: float):
    # Query daily sales data
    query = f"""
        SELECT 
            ds.sales_date,
            ds.location_id,
            l.location_name,
            ds.total_revenue,
            DAYOFWEEK(ds.sales_date) AS day_of_week,
            ds.is_weekend
        FROM FACT_DAILY_SALES ds
        JOIN DIM_LOCATION l ON ds.location_id = l.location_id
        WHERE ds.sales_date >= DATEADD(DAY, -{p_lookback_days}, CURRENT_DATE())
        ORDER BY ds.location_id, ds.sales_date
    """
    
    df = session.sql(query).to_pandas()
    
    anomalies = []
    
    # Process each location separately
    for location_id in df['LOCATION_ID'].unique():
        loc_df = df[df['LOCATION_ID'] == location_id].copy()
        location_name = loc_df['LOCATION_NAME'].iloc[0]
        
        # Calculate statistics by day of week for more accurate comparison
        for dow in loc_df['DAY_OF_WEEK'].unique():
            dow_df = loc_df[loc_df['DAY_OF_WEEK'] == dow]
            
            if len(dow_df) < 4:  # Need minimum data points
                continue
            
            mean_revenue = dow_df['TOTAL_REVENUE'].mean()
            std_revenue = dow_df['TOTAL_REVENUE'].std()
            
            if std_revenue == 0:
                continue
            
            # Calculate z-scores
            dow_df = dow_df.copy()
            dow_df['z_score'] = (dow_df['TOTAL_REVENUE'] - mean_revenue) / std_revenue
            
            # Identify anomalies
            for _, row in dow_df.iterrows():
                if abs(row['z_score']) > p_threshold_stddev:
                    anomaly_type = 'HIGH' if row['z_score'] > 0 else 'LOW'
                    anomalies.append({
                        'ANOMALY_DATE': row['SALES_DATE'],
                        'LOCATION_ID': int(location_id),
                        'LOCATION_NAME': location_name,
                        'ACTUAL_REVENUE': float(row['TOTAL_REVENUE']),
                        'EXPECTED_REVENUE': float(mean_revenue),
                        'STD_DEV': float(std_revenue),
                        'Z_SCORE': float(row['z_score']),
                        'ANOMALY_TYPE': anomaly_type
                    })
    
    if not anomalies:
        # Return empty dataframe with correct schema
        return session.create_dataframe([], schema=[
            'ANOMALY_DATE', 'LOCATION_ID', 'LOCATION_NAME', 'ACTUAL_REVENUE',
            'EXPECTED_REVENUE', 'STD_DEV', 'Z_SCORE', 'ANOMALY_TYPE'
        ])
    
    result_df = pd.DataFrame(anomalies)
    return session.create_dataframe(result_df)
$$;

-- Detect anomalies
CALL SP_DETECT_SALES_ANOMALIES(90, 2.0);


-- --------------------------------------------------------
-- 3.3 Menu Item Recommendation Engine
-- Demonstrates: Collaborative filtering concepts
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_RECOMMEND_ITEMS(
    P_CUSTOMER_ID INT,
    P_NUM_RECOMMENDATIONS INT DEFAULT 5
)
RETURNS TABLE (
    item_id INT,
    item_name VARCHAR,
    category_name VARCHAR,
    recommendation_score FLOAT,
    reason VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'recommend_items'
EXECUTE AS CALLER
AS
$$
import pandas as pd
import numpy as np
from snowflake.snowpark import Session

def recommend_items(session: Session, p_customer_id: int, p_num_recommendations: int):
    
    # Get customer's order history
    customer_history_query = f"""
        SELECT DISTINCT oi.item_id
        FROM FACT_ORDER_ITEM oi
        JOIN FACT_ORDER o ON oi.order_id = o.order_id
        WHERE o.customer_id = {p_customer_id}
    """
    customer_items = session.sql(customer_history_query).to_pandas()['ITEM_ID'].tolist()
    
    # Get popular items among similar customers (who ordered same items)
    similar_customers_query = f"""
        WITH customer_items AS (
            SELECT DISTINCT oi.item_id
            FROM FACT_ORDER_ITEM oi
            JOIN FACT_ORDER o ON oi.order_id = o.order_id
            WHERE o.customer_id = {p_customer_id}
        ),
        similar_customers AS (
            SELECT DISTINCT o.customer_id
            FROM FACT_ORDER o
            JOIN FACT_ORDER_ITEM oi ON o.order_id = oi.order_id
            WHERE oi.item_id IN (SELECT item_id FROM customer_items)
            AND o.customer_id != {p_customer_id}
        )
        SELECT 
            oi.item_id,
            m.item_name,
            c.category_name,
            COUNT(DISTINCT o.order_id) AS order_count,
            COUNT(DISTINCT o.customer_id) AS customer_count,
            AVG(COALESCE(r.overall_rating, 4)) AS avg_rating
        FROM FACT_ORDER o
        JOIN FACT_ORDER_ITEM oi ON o.order_id = oi.order_id
        JOIN DIM_MENU_ITEM m ON oi.item_id = m.item_id
        JOIN DIM_CATEGORY c ON m.category_id = c.category_id
        LEFT JOIN FACT_REVIEW r ON o.order_id = r.order_id
        WHERE o.customer_id IN (SELECT customer_id FROM similar_customers)
        AND m.is_available = TRUE
        GROUP BY oi.item_id, m.item_name, c.category_name
        ORDER BY order_count DESC
    """
    
    similar_df = session.sql(similar_customers_query).to_pandas()
    
    # Filter out items customer already ordered
    recommendations = similar_df[~similar_df['ITEM_ID'].isin(customer_items)].copy()
    
    # Calculate recommendation score
    if len(recommendations) > 0:
        # Normalize metrics
        recommendations['order_norm'] = recommendations['ORDER_COUNT'] / recommendations['ORDER_COUNT'].max()
        recommendations['customer_norm'] = recommendations['CUSTOMER_COUNT'] / recommendations['CUSTOMER_COUNT'].max()
        recommendations['rating_norm'] = recommendations['AVG_RATING'] / 5.0
        
        # Weighted score
        recommendations['RECOMMENDATION_SCORE'] = (
            recommendations['order_norm'] * 0.4 +
            recommendations['customer_norm'] * 0.3 +
            recommendations['rating_norm'] * 0.3
        ).round(3)
        
        # Add reason
        def get_reason(row):
            if row['CUSTOMER_COUNT'] > 10:
                return f"Popular with {int(row['CUSTOMER_COUNT'])} similar customers"
            elif row['AVG_RATING'] >= 4.5:
                return f"Highly rated ({row['AVG_RATING']:.1f} stars)"
            else:
                return "Recommended based on your taste"
        
        recommendations['REASON'] = recommendations.apply(get_reason, axis=1)
        
        # Select top N
        result = recommendations.nlargest(p_num_recommendations, 'RECOMMENDATION_SCORE')[
            ['ITEM_ID', 'ITEM_NAME', 'CATEGORY_NAME', 'RECOMMENDATION_SCORE', 'REASON']
        ]
    else:
        # Fallback: recommend popular items
        popular_query = """
            SELECT 
                m.item_id,
                m.item_name,
                c.category_name,
                0.5 AS recommendation_score,
                'Popular item' AS reason
            FROM DIM_MENU_ITEM m
            JOIN DIM_CATEGORY c ON m.category_id = c.category_id
            WHERE m.is_available = TRUE
            AND m.category_id = 1  -- Pizzas
            LIMIT 5
        """
        result = session.sql(popular_query).to_pandas()
    
    return session.create_dataframe(result)
$$;

-- Get recommendations for a customer
CALL SP_RECOMMEND_ITEMS(5, 5);


-- ============================================================
-- PART 4: UTILITY PROCEDURES
-- ============================================================

-- --------------------------------------------------------
-- 4.1 Audit Log Procedure
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_LOG_AUDIT_EVENT(
    P_EVENT_TYPE VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_RECORD_ID INT,
    P_DETAILS VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    -- Create audit table if not exists
    CREATE TABLE IF NOT EXISTS AUDIT_LOG (
        audit_id INT AUTOINCREMENT,
        event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        event_type VARCHAR(50),
        table_name VARCHAR(100),
        record_id INT,
        user_name VARCHAR(100) DEFAULT CURRENT_USER(),
        session_id INT DEFAULT CURRENT_SESSION(),
        details VARIANT
    );
    
    INSERT INTO AUDIT_LOG (event_type, table_name, record_id, details)
    VALUES (:P_EVENT_TYPE, :P_TABLE_NAME, :P_RECORD_ID, :P_DETAILS);
    
    RETURN 'Audit event logged successfully';
END;


-- --------------------------------------------------------
-- 4.2 Data Quality Check Procedure
-- --------------------------------------------------------

CREATE OR REPLACE PROCEDURE SP_RUN_DATA_QUALITY_CHECKS()
RETURNS TABLE (
    check_name VARCHAR,
    table_name VARCHAR,
    status VARCHAR,
    issue_count INT,
    details VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    res RESULTSET;
BEGIN
    -- Create temp table for results
    CREATE OR REPLACE TEMPORARY TABLE DQ_RESULTS (
        check_name VARCHAR,
        table_name VARCHAR,
        status VARCHAR,
        issue_count INT,
        details VARCHAR
    );
    
    -- Check 1: Orders with null customer
    INSERT INTO DQ_RESULTS
    SELECT 
        'Null Customer ID' AS check_name,
        'FACT_ORDER' AS table_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count,
        'Orders without customer reference' AS details
    FROM FACT_ORDER WHERE customer_id IS NULL;
    
    -- Check 2: Negative order totals
    INSERT INTO DQ_RESULTS
    SELECT 
        'Negative Totals' AS check_name,
        'FACT_ORDER' AS table_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count,
        'Orders with negative total_amount' AS details
    FROM FACT_ORDER WHERE total_amount < 0;
    
    -- Check 3: Orphaned order items
    INSERT INTO DQ_RESULTS
    SELECT 
        'Orphaned Order Items' AS check_name,
        'FACT_ORDER_ITEM' AS table_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count,
        'Order items without parent order' AS details
    FROM FACT_ORDER_ITEM oi
    WHERE NOT EXISTS (SELECT 1 FROM FACT_ORDER o WHERE o.order_id = oi.order_id);
    
    -- Check 4: Invalid ratings
    INSERT INTO DQ_RESULTS
    SELECT 
        'Invalid Ratings' AS check_name,
        'FACT_REVIEW' AS table_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count,
        'Reviews with rating outside 1-5 range' AS details
    FROM FACT_REVIEW WHERE overall_rating NOT BETWEEN 1 AND 5;
    
    -- Check 5: Duplicate customers (by email)
    INSERT INTO DQ_RESULTS
    SELECT 
        'Duplicate Emails' AS check_name,
        'DIM_CUSTOMER' AS table_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status,
        COUNT(*) AS issue_count,
        'Customers with duplicate email addresses' AS details
    FROM (
        SELECT email, COUNT(*) AS cnt 
        FROM DIM_CUSTOMER 
        GROUP BY email 
        HAVING COUNT(*) > 1
    );
    
    res := (SELECT * FROM DQ_RESULTS ORDER BY status DESC, check_name);
    RETURN TABLE(res);
END;

-- Run data quality checks
CALL SP_RUN_DATA_QUALITY_CHECKS();


-- ============================================================
-- PART 5: PROCEDURE MANAGEMENT
-- ============================================================

-- View all procedures
SHOW PROCEDURES IN SCHEMA BELLA_NAPOLI;

-- View procedure definition
DESCRIBE PROCEDURE SP_PROCESS_DAILY_SALES(DATE);

-- Grant execute permission
-- GRANT USAGE ON PROCEDURE SP_PROCESS_DAILY_SALES(DATE) TO ROLE analyst_role;

COMMIT;
