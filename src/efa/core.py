import os
import pandas as pd
import duckdb
from efa import config
from pathlib import Path

PATH_TO_DATA = Path(os.environ.get("EFA_DATA_PATH", Path(__file__).resolve().parents[2] / "data"))
PATH_TO_DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = PATH_TO_DATA / "efa.duckdb"

def save_to_duckdb(df, table_name, mode="append"):
    """
    Save dataframe to DuckDB database.

    Args:
        df: DataFrame to save
        table_name: Name of the table to create/append to
        mode: Either "append" (default) to add data or "replace" to overwrite the table
    """
    db_path = str(DB_PATH)
    PATH_TO_DATA.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(db_path) as con:
        # register the pandas DataFrame so SQL can reference it as 'df'
        con.register("df", df)

        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            print(f"\nReplaced table '{table_name}' in DuckDB: {db_path}")
        elif mode == "append":
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            print(f"\nAppended to table '{table_name}' in DuckDB: {db_path}")
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'append' or 'replace'")

        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Total rows in table '{table_name}': {row_count}")