CREATE TABLE IF NOT EXISTS metrics_top_customers_last_week AS
SELECT 
    c.customer_name,
    SUM(f.price) as total_value,
    COUNT(f.order_id) as number_of_orders
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_dates d ON f.date_id = d.date_id
WHERE d.full_date >= :start_date
GROUP BY c.customer_name
ORDER BY total_value DESC; 