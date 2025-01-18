import sqlite3
import os


def execute_sql_file(cursor, file_path):
    with open(file_path, "r") as sql_file:
        sql = sql_file.read()
        cursor.execute(sql)


def create_star_schema():
    conn = sqlite3.connect("data/application.db")
    cursor = conn.cursor()

    # Create SQL directory if it doesn't exist
    sql_dir = os.path.join(os.path.dirname(__file__), "sql")
    if not os.path.exists(sql_dir):
        os.makedirs(sql_dir)

    # Execute each SQL file in order
    sql_files = [
        "01_create_cleaned_orders.sql",
        "02_create_dim_customers.sql",
        "03_create_dim_products.sql",
        "04_create_dim_dates.sql",
        "05_create_fact_orders.sql",
        "06_populate_dim_customers.sql",
        "07_populate_dim_products.sql",
        "08_populate_dim_dates.sql",
        "09_populate_fact_orders.sql",
    ]

    for sql_file in sql_files:
        file_path = os.path.join(sql_dir, sql_file)
        execute_sql_file(cursor, file_path)
        print(f"Executed {sql_file}")

    conn.commit()

    # Print statistics
    print("\nData transformation completed!")
    for table in ["dim_customers", "dim_products", "dim_dates", "fact_orders"]:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")

    conn.close()


if __name__ == "__main__":
    create_star_schema()
