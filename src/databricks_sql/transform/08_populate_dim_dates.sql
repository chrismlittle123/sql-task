-- Populate dates dimension
INSERT INTO dim_dates (full_date, year, month, day)
SELECT DISTINCT
    order_date,
    YEAR(order_date),
    MONTH(order_date),
    DAY(order_date)
FROM cleaned_orders; 