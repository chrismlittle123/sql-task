from databricks.sdk.runtime import *
import os
from typing import List, Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd


class DatabricksAnalysis:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.sql_dir = os.path.join(os.path.dirname(__file__), "sql", "analysis")

    def execute_sql_from_file(
        self, filename: str, params: Dict[str, Any] = None
    ) -> pd.DataFrame:
        """Execute a SQL file using Databricks SQL and return results as pandas DataFrame"""
        file_path = os.path.join(self.sql_dir, filename)
        with open(file_path, "r") as f:
            sql = f.read()

        # Replace parameters if provided
        if params:
            for key, value in params.items():
                sql = sql.replace(f":{key}", f"'{value}'")

        # Execute SQL and convert to pandas
        return self.spark.sql(sql).toPandas()

    def get_total_orders_by_customer(self) -> pd.DataFrame:
        """Get total number of orders for each customer"""
        return self.execute_sql_from_file("01_total_orders_by_customer.sql")

    def get_most_recent_orders(self) -> pd.DataFrame:
        """Get most recent order for each customer"""
        return self.execute_sql_from_file("02_most_recent_order_by_customer.sql")

    def get_top_customers_last_week(
        self, reference_date: datetime = None
    ) -> pd.DataFrame:
        """Get customers who provided most value in the past week"""
        if reference_date is None:
            reference_date = datetime.now()

        start_date = (reference_date - timedelta(days=7)).strftime("%Y-%m-%d")
        return self.execute_sql_from_file(
            "03_top_customers_last_week.sql", {"start_date": start_date}
        )


def display_analysis_results():
    """Display all analysis results"""
    analysis = DatabricksAnalysis()

    print("\nTotal Orders by Customer:")
    display(analysis.get_total_orders_by_customer())

    print("\nMost Recent Orders:")
    display(analysis.get_most_recent_orders())

    print("\nTop Customers Last Week:")
    display(analysis.get_top_customers_last_week())


if __name__ == "__main__":
    display_analysis_results()
