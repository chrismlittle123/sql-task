CREATE TABLE IF NOT EXISTS cleaned_orders (
    order_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    price DECIMAL(10,2),
    order_date DATE,
    passenger_count INTEGER,
    product_name TEXT
) AS
SELECT 
    customer_name,
    order_id,
    price,
    order_date,
    passenger_count,
    CASE 
        WHEN product_id = 'fast-track' THEN 'fast_track' 
        ELSE product_id 
    END AS product_name
FROM 
    raw_orders; 