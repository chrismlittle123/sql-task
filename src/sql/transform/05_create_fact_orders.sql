CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    date_id INTEGER,
    price DECIMAL(10,2),
    passenger_count INTEGER,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id)
); 