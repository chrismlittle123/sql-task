import os
from typing import List
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.databricks_connection import DatabricksConnection


class DatabricksTransformer:
    def __init__(self):
        self.connection = DatabricksConnection()
        self.sql_dir = os.path.join(os.path.dirname(__file__), "sql", "transform")

    def execute_sql_from_file(self, filename: str) -> None:
        """Execute a SQL file using Databricks SQL"""
        file_path = os.path.join(self.sql_dir, filename)
        with open(file_path, "r") as f:
            sql = f.read()

        # Split SQL into separate statements
        statements = sql.split(";")

        # Execute each non-empty statement
        for statement in statements:
            if statement.strip():
                self.connection.execute_sql(statement)

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
            print(f"Executing {sql_file}...")
            self.execute_sql_from_file(sql_file)
            print(f"Completed {sql_file}")


if __name__ == "__main__":
    transformer = DatabricksTransformer()
    transformer.transform_data()
