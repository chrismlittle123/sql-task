CREATE TABLE IF NOT EXISTS metrics_total_orders_by_customer AS
SELECT 
    c.customer_name,
    COUNT(f.order_id) AS total_orders
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_orders DESC;