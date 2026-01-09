-- ============================================================
-- SNOWFLAKE INTELLIGENCE DEMO: PIZZERIA BELLA NAPOLI
-- Script 1: Schema and Table Definitions
-- ============================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS PIZZERIA_DEMO;
USE DATABASE PIZZERIA_DEMO;

CREATE SCHEMA IF NOT EXISTS BELLA_NAPOLI;
USE SCHEMA BELLA_NAPOLI;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Menu Categories
CREATE OR REPLACE TABLE DIM_CATEGORY (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description VARCHAR(200),
    display_order INT
);

-- Menu Items (Pizzas, Sides, Drinks, Desserts)
CREATE OR REPLACE TABLE DIM_MENU_ITEM (
    item_id INT PRIMARY KEY,
    category_id INT REFERENCES DIM_CATEGORY(category_id),
    item_name VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    base_price DECIMAL(8,2) NOT NULL,
    cost_to_make DECIMAL(8,2),
    prep_time_minutes INT,
    calories INT,
    is_vegetarian BOOLEAN DEFAULT FALSE,
    is_vegan BOOLEAN DEFAULT FALSE,
    is_gluten_free BOOLEAN DEFAULT FALSE,
    is_available BOOLEAN DEFAULT TRUE,
    created_date DATE DEFAULT CURRENT_DATE()
);

-- Pizza Sizes
CREATE OR REPLACE TABLE DIM_SIZE (
    size_id INT PRIMARY KEY,
    size_name VARCHAR(20) NOT NULL,
    size_inches INT,
    price_multiplier DECIMAL(4,2) DEFAULT 1.00
);

-- Customers
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    registration_date DATE,
    loyalty_points INT DEFAULT 0,
    preferred_order_type VARCHAR(20), -- DELIVERY, PICKUP, DINE_IN
    birthday DATE
);

-- Employees
CREATE OR REPLACE TABLE DIM_EMPLOYEE (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    role VARCHAR(50), -- MANAGER, CHEF, CASHIER, DELIVERY_DRIVER
    hourly_rate DECIMAL(6,2),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Store Locations
CREATE OR REPLACE TABLE DIM_LOCATION (
    location_id INT PRIMARY KEY,
    location_name VARCHAR(100),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    opening_time TIME,
    closing_time TIME,
    seating_capacity INT,
    has_delivery BOOLEAN DEFAULT TRUE
);

-- Ingredients (for inventory tracking)
CREATE OR REPLACE TABLE DIM_INGREDIENT (
    ingredient_id INT PRIMARY KEY,
    ingredient_name VARCHAR(100) NOT NULL,
    unit_of_measure VARCHAR(20), -- LB, OZ, EACH, GAL
    cost_per_unit DECIMAL(8,4),
    supplier VARCHAR(100),
    is_perishable BOOLEAN DEFAULT TRUE,
    shelf_life_days INT
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- Orders (Header)
CREATE OR REPLACE TABLE FACT_ORDER (
    order_id INT PRIMARY KEY,
    customer_id INT REFERENCES DIM_CUSTOMER(customer_id),
    employee_id INT REFERENCES DIM_EMPLOYEE(employee_id),
    location_id INT REFERENCES DIM_LOCATION(location_id),
    order_timestamp TIMESTAMP_NTZ NOT NULL,
    order_type VARCHAR(20), -- DELIVERY, PICKUP, DINE_IN
    subtotal DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2) DEFAULT 0,
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(20), -- CASH, CREDIT, DEBIT, MOBILE
    order_status VARCHAR(20), -- PENDING, PREPARING, READY, DELIVERED, COMPLETED, CANCELLED
    delivery_address VARCHAR(200),
    estimated_ready_time TIMESTAMP_NTZ,
    actual_ready_time TIMESTAMP_NTZ,
    delivery_time TIMESTAMP_NTZ,
    special_instructions VARCHAR(500)
);

-- Order Line Items (Detail)
CREATE OR REPLACE TABLE FACT_ORDER_ITEM (
    order_item_id INT PRIMARY KEY,
    order_id INT REFERENCES FACT_ORDER(order_id),
    item_id INT REFERENCES DIM_MENU_ITEM(item_id),
    size_id INT REFERENCES DIM_SIZE(size_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(8,2) NOT NULL,
    line_total DECIMAL(10,2) NOT NULL,
    special_requests VARCHAR(300)
);

-- Customer Reviews
CREATE OR REPLACE TABLE FACT_REVIEW (
    review_id INT PRIMARY KEY,
    order_id INT REFERENCES FACT_ORDER(order_id),
    customer_id INT REFERENCES DIM_CUSTOMER(customer_id),
    location_id INT REFERENCES DIM_LOCATION(location_id),
    review_date TIMESTAMP_NTZ,
    overall_rating INT, -- 1-5 stars
    food_rating INT,
    service_rating INT,
    delivery_rating INT,
    review_text VARCHAR(2000),
    review_source VARCHAR(50) -- WEBSITE, GOOGLE, YELP, DOORDASH
);

-- Inventory Levels
CREATE OR REPLACE TABLE FACT_INVENTORY (
    inventory_id INT PRIMARY KEY,
    location_id INT REFERENCES DIM_LOCATION(location_id),
    ingredient_id INT REFERENCES DIM_INGREDIENT(ingredient_id),
    record_date DATE,
    quantity_on_hand DECIMAL(10,2),
    quantity_used DECIMAL(10,2),
    quantity_received DECIMAL(10,2),
    quantity_wasted DECIMAL(10,2),
    reorder_point DECIMAL(10,2),
    reorder_quantity DECIMAL(10,2)
);

-- Daily Sales Summary (for forecasting)
CREATE OR REPLACE TABLE FACT_DAILY_SALES (
    sales_date DATE,
    location_id INT REFERENCES DIM_LOCATION(location_id),
    total_orders INT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(8,2),
    dine_in_orders INT,
    pickup_orders INT,
    delivery_orders INT,
    total_pizzas_sold INT,
    new_customers INT,
    weather_condition VARCHAR(50),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    PRIMARY KEY (sales_date, location_id)
);

COMMIT;
