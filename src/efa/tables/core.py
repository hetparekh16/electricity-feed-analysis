import os
import polars as pl
import duckdb
from pathlib import Path
from loguru import logger
from typing import Literal

PATH_TO_DATA = Path(os.environ.get("EFA_DATA_PATH", Path(__file__).resolve().parents[3] / "data"))
PATH_TO_DATA.mkdir(parents=True, exist_ok=True)

DB_PATH = PATH_TO_DATA / "efa.duckdb"


class Table:
    """Minimal Table class for managing data with schema validation and DuckDB persistence.
    
    Attributes
    ----------
    table_name : str
        Name of the table in the database
    schema : dict
        Polars schema dict for validation
    """
    
    def __init__(self, table_name: str, schema: dict):
        """Initialize a Table instance.
        
        Parameters
        ----------
        table_name : str
            Name of the table
        schema : dict
            Polars schema definition (column name -> DataType)
        """
        if not isinstance(schema, dict):
            raise TypeError(
                f"Schema must be a dict mapping column names to Polars types, got {type(schema)}"
            )
        
        self.table_name = table_name
        self.schema = schema
        self._shape = None
    
    def _validate_schema(self, df: pl.DataFrame) -> None:
        """Validate DataFrame schema against expected schema.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to validate
            
        Raises
        ------
        Exception
            If schema validation fails
        """
        logger.debug("Performing schema validation...")
        
        # Check columns exist
        missing_cols = [col for col in self.schema if col not in df.columns]
        if missing_cols:
            raise Exception(f"❌ Schema validation failed! Missing columns: {missing_cols}")
            
        # Check types (basic check)
        # Note: Polars types might be strict, so we just check if they are compatible if needed
        # For now, we trust DuckDB/Polars to handle type conversion or error out on write
        logger.info("Schema validation passed ✅")

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "replace"] = "append",
        validate: bool = True
    ) -> None:
        """Write DataFrame to DuckDB table.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to write
        mode : Literal["append", "replace"], optional
            Write mode, by default "append"
        validate : bool, optional
            Whether to validate schema, by default True
            
        Raises
        ------
        ValueError
            If mode is invalid or DataFrame is empty
        Exception
            If validation or write operation fails
        """
        if mode not in ["append", "replace"]:
            raise ValueError(f"Mode must be 'append' or 'replace', got '{mode}'")
        
        if df.is_empty():
            raise ValueError("Cannot write empty DataFrame")
        
        logger.info(f"Writing {len(df)} rows to table '{self.table_name}'...")
        
        # Validation
        if validate:
            self._validate_schema(df)
        
        # Write to DuckDB
        db_path = str(DB_PATH)
        with duckdb.connect(db_path) as con:
            # DuckDB can query Polars DataFrames directly!
            # We register it as a view
            
            if mode == "replace":
                con.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                con.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM df")
                logger.info(f"Replaced table '{self.table_name}'")
            else:  # append
                # Create table if not exists
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table_name} AS "
                    f"SELECT * FROM df WHERE 1=0"
                )
                con.execute(f"INSERT INTO {self.table_name} SELECT * FROM df")
                logger.info(f"Appended to table '{self.table_name}'")
            
            # Update cached shape
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {self.table_name}"
            ).fetchone()[0]
            
            # Get columns from DuckDB
            col_count = len(
                con.execute(f"DESCRIBE {self.table_name}").df()
            )
            self._shape = (row_count, col_count)
            
            logger.info(f"Total rows in table '{self.table_name}': {row_count}")
    
    def read(self) -> pl.DataFrame:
        """Read table from DuckDB.
        
        Returns
        -------
        pl.DataFrame
            Table data as DataFrame
            
        Raises
        ------
        Exception
            If table doesn't exist
        """
        db_path = str(DB_PATH)
        
        with duckdb.connect(db_path) as con:
            try:
                # Use .pl() to get Polars DataFrame directly
                df = con.execute(f"SELECT * FROM {self.table_name}").pl()
                self._shape = df.shape
                logger.info(
                    f"Read {df.shape[0]} rows from table '{self.table_name}'"
                )
                return df
            except duckdb.CatalogException:
                raise Exception(
                    f"Table '{self.table_name}' not found. "
                    "Create it by writing data first."
                )
    
    @property
    def exists(self) -> bool:
        """Check if table exists in database.
        
        Returns
        -------
        bool
            True if table exists, False otherwise
        """
        db_path = str(DB_PATH)
        with duckdb.connect(db_path) as con:
            result = con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_name = '{self.table_name}'"
            ).fetchone()[0]
            return result > 0
    
    @property
    def shape(self) -> tuple[int, int] | None:
        """Get shape of the table.
        
        Returns
        -------
        tuple[int, int] | None
            (rows, columns) or None if table doesn't exist
        """
        if self._shape is not None:
            return self._shape
        
        if not self.exists:
            return None
        
        db_path = str(DB_PATH)
        with duckdb.connect(db_path) as con:
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {self.table_name}"
            ).fetchone()[0]
            col_count = len(
                con.execute(f"DESCRIBE {self.table_name}").df()
            )
            self._shape = (row_count, col_count)
            return self._shape
