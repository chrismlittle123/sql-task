import sqlite3
import pandas as pd


conn = sqlite3.connect("data/application.db")

# Get all table names
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
table_names = [table[0] for table in tables]

# Write each table to a dataframe and then to a CSV file
for table_name in table_names:
    result = conn.execute(f"SELECT * FROM {table_name}")
    df = pd.DataFrame(
        result.fetchall(), columns=[column[0] for column in result.description]
    )
    print(df)
    df.to_csv(f"data/{table_name}.csv", index=False)

conn.close()
