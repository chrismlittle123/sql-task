-- Create total orders by customer metrics
DROP TABLE IF EXISTS metrics_total_orders_by_customer;

CREATE OR REPLACE TABLE metrics_total_orders_by_customer
USING DELTA AS
SELECT 
    c.customer_name,
    COUNT(f.order_id) AS total_orders
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_orders DESC;