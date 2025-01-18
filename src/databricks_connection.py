from databricks.sql import connect
from databricks.sql.client import Connection
import os
from typing import List, Dict, Any
from dotenv import load_dotenv
import pandas as pd


class DatabricksConnection:
    def __init__(self):
        # Initialize connection attribute
        self._connection = None

        # Load environment variables
        load_dotenv()

        # Get connection parameters from environment variables
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")

        # Validate environment variables
        missing_vars = []
        if not self.server_hostname:
            missing_vars.append("DATABRICKS_SERVER_HOSTNAME")
        if not self.http_path:
            missing_vars.append("DATABRICKS_HTTP_PATH")
        if not self.access_token:
            missing_vars.append("DATABRICKS_ACCESS_TOKEN")

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}. "
                "Please check your .env file."
            )

    def get_connection(self) -> Connection:
        """Get or create a Databricks SQL connection"""
        try:
            if self._connection is None:
                self._connection = connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    access_token=self.access_token,
                )
            return self._connection
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Databricks: {str(e)}")

    def execute_sql(self, sql: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a pandas DataFrame"""
        try:
            connection = self.get_connection()
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # Get column names
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    # Fetch results and convert to DataFrame
                    results = cursor.fetchall()
                    return pd.DataFrame(results, columns=columns)
                return pd.DataFrame()
        except Exception as e:
            raise RuntimeError(f"Failed to execute SQL: {str(e)}")

    def close(self):
        """Close the connection"""
        try:
            if hasattr(self, "_connection") and self._connection is not None:
                self._connection.close()
                self._connection = None
        except Exception as e:
            print(f"Warning: Failed to close connection: {str(e)}")

    def __del__(self):
        """Ensure connection is closed when object is deleted"""
        self.close()
