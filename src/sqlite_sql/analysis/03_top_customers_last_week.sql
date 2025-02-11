CREATE TABLE IF NOT EXISTS metrics_top_customers_last_week AS
SELECT 
    c.customer_name,
    SUM(f.price) AS total_value,
    COUNT(f.order_id) AS number_of_orders
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
JOIN dim_dates d ON f.date_id = d.date_id
WHERE d.full_date >= '2023-12-03' AND d.full_date <= '2023-12-10'
GROUP BY c.customer_name
ORDER BY total_value DESC;