import sqlite3


conn = sqlite3.connect('application.db')
result = conn.execute("SELECT * FROM raw_orders")

# Columns are : 'customer_name', 'order_id', 'price', 'order_date', 'product_id', 'passenger_count
print(result.fetchall())