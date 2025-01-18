INSERT OR IGNORE INTO dim_dates (full_date, year, month, day)
SELECT DISTINCT 
    order_date,
    CAST(strftime('%Y', order_date) AS INTEGER),
    CAST(strftime('%m', order_date) AS INTEGER),
    CAST(strftime('%d', order_date) AS INTEGER)
FROM cleaned_orders; 