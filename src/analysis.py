from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
from .database import Database


class OrderAnalysis:
    def __init__(self, db_path: str = "data/application.db"):
        self.db = Database(db_path)
        self.sql_dir = os.path.join(os.path.dirname(__file__), "sql", "analysis")

    def set_connection(self, connection):
        """Set a specific connection (used for testing)"""
        self.db.set_connection(connection)

    def get_total_orders_by_customer(self) -> List[Dict[str, Any]]:
        """Get total number of orders for each customer"""
        return self.db.execute_sql_file(self.sql_dir, "01_total_orders_by_customer.sql")

    def get_most_recent_orders(self) -> List[Dict[str, Any]]:
        """Get most recent order for each customer"""
        return self.db.execute_sql_file(
            self.sql_dir, "02_most_recent_order_by_customer.sql"
        )

    def get_top_customers_last_week(
        self, reference_date: datetime = None
    ) -> List[Dict[str, Any]]:
        """Get customers who provided most value in the past week"""
        if reference_date is None:
            reference_date = datetime.now()

        start_date = (reference_date - timedelta(days=7)).strftime("%Y-%m-%d")
        return self.db.execute_sql_file(
            self.sql_dir, "03_top_customers_last_week.sql", {"start_date": start_date}
        )
