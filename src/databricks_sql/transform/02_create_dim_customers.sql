-- Create customers dimension table
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    customer_id INT GENERATED ALWAYS AS IDENTITY,
    customer_name STRING
) USING DELTA; 