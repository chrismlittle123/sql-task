-- Create fact orders table
DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders (
    order_id STRING,
    customer_id INT,
    product_id INT,
    date_id INT,
    price DECIMAL(10,2),
    passenger_count INT,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
) USING DELTA; 