CREATE TABLE IF NOT EXISTS cleaned_orders AS
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