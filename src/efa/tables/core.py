import os
import pandas as pd
import duckdb
import pandera.pandas as pa
import pandera.errors
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
    schema : pa.DataFrameModel
        Pandera schema for validation
    """
    
    def __init__(self, table_name: str, schema: pa.DataFrameModel):
        """Initialize a Table instance.
        
        Parameters
        ----------
        table_name : str
            Name of the table
        schema : pa.DataFrameModel
            Pandera DataFrameModel for schema definition and validation
        """
        if not isinstance(schema, type) or not issubclass(schema, pa.DataFrameModel):
            raise TypeError(
                f"Schema must be a pandera.DataFrameModel class, got {type(schema)}"
            )
        
        self.table_name = table_name
        self.schema = schema
        self._shape = None
    
    def _validate_shallow(self, df: pd.DataFrame) -> None:
        """Perform shallow validation (column names and types only).
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to validate
            
        Raises
        ------
        Exception
            If schema validation fails
        """
        logger.debug("Performing shallow validation...")
        try:
            # Validate only structure (no data checks)
            self.schema.validate(df.head(0), lazy=True)
            logger.info("Shallow validation passed ✅")
        except pandera.errors.SchemaError as ex:
            logger.error(f"Shallow validation failed: {ex}")
            raise Exception("❌ Shallow validation failed with schema mismatch!")
    
    def _validate_deep(self, df: pd.DataFrame) -> None:
        """Perform deep validation (including data value checks).
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to validate
            
        Raises
        ------
        Exception
            If validation fails
        """
        logger.debug("Performing deep validation...")
        try:
            self.schema.validate(df, lazy=False)
            logger.info("Deep validation passed ✅")
        except (pandera.errors.SchemaError, pandera.errors.SchemaErrors) as ex:
            logger.error(f"Deep validation failed: {ex}")
            if hasattr(ex, 'failure_cases'):
                logger.debug(f"Failure cases:\n{ex.failure_cases}")
            raise Exception("❌ Deep validation failed!")
    
    def write(
        self,
        df: pd.DataFrame,
        mode: Literal["append", "replace"] = "append",
        validate: bool = True,
        deep_validate: bool = False
    ) -> None:
        """Write DataFrame to DuckDB table.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to write
        mode : Literal["append", "replace"], optional
            Write mode, by default "append"
        validate : bool, optional
            Whether to validate schema, by default True
        deep_validate : bool, optional
            Whether to perform deep validation, by default False
            
        Raises
        ------
        ValueError
            If mode is invalid or DataFrame is empty
        Exception
            If validation or write operation fails
        """
        if mode not in ["append", "replace"]:
            raise ValueError(f"Mode must be 'append' or 'replace', got '{mode}'")
        
        if df.empty:
            raise ValueError("Cannot write empty DataFrame")
        
        logger.info(f"Writing {len(df)} rows to table '{self.table_name}'...")
        
        # Validation
        if validate:
            self._validate_shallow(df)
            if deep_validate:
                self._validate_deep(df)
        
        # Write to DuckDB
        db_path = str(DB_PATH)
        with duckdb.connect(db_path) as con:
            con.register("df_temp", df)
            
            if mode == "replace":
                con.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                con.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM df_temp")
                logger.info(f"Replaced table '{self.table_name}'")
            else:  # append
                # Create table if not exists
                con.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table_name} AS "
                    f"SELECT * FROM df_temp WHERE 1=0"
                )
                con.execute(f"INSERT INTO {self.table_name} SELECT * FROM df_temp")
                logger.info(f"Appended to table '{self.table_name}'")
            
            # Update cached shape
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {self.table_name}"
            ).fetchone()[0]
            col_count = len(df.columns)
            self._shape = (row_count, col_count)
            
            logger.info(f"Total rows in table '{self.table_name}': {row_count}")
    
    def read(self) -> pd.DataFrame:
        """Read table from DuckDB.
        
        Returns
        -------
        pd.DataFrame
            Table data as DataFrame
            
        Raises
        ------
        Exception
            If table doesn't exist
        """
        db_path = str(DB_PATH)
        
        with duckdb.connect(db_path) as con:
            try:
                df = con.execute(f"SELECT * FROM {self.table_name}").df()
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

