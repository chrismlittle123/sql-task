INSERT OR IGNORE INTO fact_orders (
    order_id, customer_id, product_id, date_id, price, passenger_count
)
SELECT 
    r.order_id,
    c.customer_id,
    p.product_id,
    d.date_id,
    r.price,
    r.passenger_count
FROM cleaned_orders r
JOIN dim_customers c ON r.customer_name = c.customer_name
JOIN dim_products p ON r.product_name = p.product_name
JOIN dim_dates d ON r.order_date = d.full_date; 