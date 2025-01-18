INSERT OR IGNORE INTO dim_customers (customer_name) 
SELECT DISTINCT customer_name 
FROM cleaned_orders; 