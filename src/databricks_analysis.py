import os
from typing import List
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.databricks_connection import DatabricksConnection


class DatabricksAnalysis:
    def __init__(self):
        self.connection = DatabricksConnection()
        self.sql_dir = os.path.join(
            os.path.dirname(__file__), "databricks_sql", "analysis"
        )

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

    def get_analysis_results(self) -> None:
        """Get analysis results"""
        sql_files = [
            "01_total_orders_by_customer.sql",
            "02_most_recent_order_by_customer.sql",
            "03_top_customers_last_week.sql",
        ]

        for sql_file in sql_files:
            print(f"Executing {sql_file}...")
            self.execute_sql_from_file(sql_file)
            print(f"Completed {sql_file}")


if __name__ == "__main__":
    analysis = DatabricksAnalysis()
    analysis.get_analysis_results()
