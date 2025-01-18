-- Create customers dimension table
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name STRING
) USING DELTA; 