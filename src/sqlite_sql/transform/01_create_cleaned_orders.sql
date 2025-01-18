CREATE TABLE IF NOT EXISTS cleaned_orders (
    order_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    price DECIMAL(10,2),
    order_date DATE,
    passenger_count INTEGER,
    product_name TEXT
);

INSERT INTO cleaned_orders (order_id, customer_name, price, order_date, passenger_count, product_name)
SELECT 
    order_id,
    customer_name,
    price,
    order_date,
    passenger_count,
    CASE 
        WHEN product_id IN ('fast-track', 'fast_track') THEN 'fast_track' 
        ELSE product_id 
    END AS product_name
FROM 
    raw_orders;