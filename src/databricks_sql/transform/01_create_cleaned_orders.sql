-- Drop if exists using Databricks syntax
DROP TABLE IF EXISTS cleaned_orders;

-- Create table using Databricks Delta format
CREATE TABLE cleaned_orders (
    order_id STRING,
    customer_name STRING,
    price DECIMAL(10,2),
    order_date DATE,
    passenger_count INT,
    product_name STRING
) USING DELTA;

-- Insert data using Databricks syntax
INSERT INTO cleaned_orders
SELECT 
    order_id,
    customer_name,
    price,
    CAST(order_date AS DATE),
    passenger_count,
    CASE 
        WHEN product_id = 'fast-track' THEN 'fast_track' 
        ELSE product_id 
    END AS product_name
FROM raw_orders;