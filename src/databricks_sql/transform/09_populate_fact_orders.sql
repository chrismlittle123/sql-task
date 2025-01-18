-- Populate fact orders table
INSERT INTO fact_orders (
    order_id,
    customer_id,
    product_id,
    date_id,
    price,
    passenger_count
)
SELECT 
    co.order_id,
    dc.customer_id,
    dp.product_id,
    dd.date_id,
    co.price,
    co.passenger_count
FROM cleaned_orders co
JOIN dim_customers dc ON co.customer_name = dc.customer_name
JOIN dim_products dp ON co.product_name = dp.product_name
JOIN dim_dates dd ON co.order_date = dd.full_date; 