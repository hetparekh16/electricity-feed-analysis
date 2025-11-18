import os
import pandas as pd
import duckdb
from efa import config



def save_to_duckdb(df, table_name, mode):
    """
    Save dataframe to DuckDB database.
    
    Args:
        df: DataFrame to save
        table_name: Name of the table to create/append to
        mode: Either "append" (default) to add data or "replace" to overwrite the table
    """

    os.makedirs(config.data_path, exist_ok=True)
    db_path = os.path.join(config.output_path, "efa.duckdb")

    with duckdb.connect(db_path) as con:
        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"\nReplaced table '{table_name}' in DuckDB: {db_path}")
        elif mode == "append":
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
            # Insert data
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            print(f"\nAppended to table '{table_name}' in DuckDB: {db_path}")
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'append' or 'replace'")
        
        # Show table info
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Total rows in table '{table_name}': {row_count}")