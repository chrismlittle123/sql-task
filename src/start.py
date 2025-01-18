import sqlite3
import pandas as pd


conn = sqlite3.connect("data/application.db")


result = conn.execute("SELECT * FROM raw_orders")

df = pd.DataFrame(
    result.fetchall(),
    columns=[
        "customer_name",
        "order_id",
        "price",
        "order_date",
        "product_id",
        "passenger_count",
    ],
)
print(df)
