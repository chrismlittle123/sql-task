import os
from databricks import sql


class DatabricksConnection:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.connection = None

    def connect(self):
        """Establish a connection to Databricks"""
        self.connection = sql.connect(
            server_hostname=self.host,
            http_path="/sql/1.0/endpoints",
            access_token=self.token,
        )

    def execute_sql(self, query: str):
        """Execute a SQL query in Databricks"""
        if not self.connection:
            self.connect()

        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def close(self):
        """Close the Databricks connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
