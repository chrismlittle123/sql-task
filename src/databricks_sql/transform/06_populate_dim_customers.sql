-- Populate customers dimension
INSERT INTO dim_customers (customer_name)
SELECT DISTINCT customer_name FROM cleaned_orders; 