import os
from typing import List
import sys

# Add project root to Python path when running directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.sqlite_database import Database


class DataTransformer:
    def __init__(self, db_path: str = "data/application.db"):
        self.db = Database(db_path)
        self.sql_dir = os.path.join(
            os.path.dirname(__file__), "sqlite_sql", "transform"
        )

    def set_connection(self, connection):
        """Set a specific connection (used for testing)"""
        self.db.set_connection(connection)

    def transform_data(self) -> None:
        """Transform raw data into star schema"""
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
            # Read and execute each SQL file as a script
            file_path = os.path.join(self.sql_dir, sql_file)
            with open(file_path, "r") as f:
                sql = f.read()
            self.db.execute_sql_script(sql)


if __name__ == "__main__":
    transformer = DataTransformer()
    transformer.transform_data()
