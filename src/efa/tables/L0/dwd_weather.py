import polars as pl
from efa.tables.core import Table


DwdWeatherSchema = {
    "time": pl.Datetime,
    
    # Surface wind components (10m height)
    "v_10m": pl.Float64,
    "u_10m": pl.Float64,
    
    # Model-level wind components (upper air)
    "v_level61": pl.Float64,
    "v_level62": pl.Float64,
    "v_level63": pl.Float64,
    "v_level64": pl.Float64,
    
    "u_level61": pl.Float64,
    "u_level62": pl.Float64,
    "u_level63": pl.Float64,
    "u_level64": pl.Float64,
    
    # Solar radiation
    "aswdir_s": pl.Float64,
    "aswdifd_s": pl.Float64,
    
    # Temperature
    "t_2m": pl.Float64,
    
    # Location
    "latitude": pl.Float64,
    "longitude": pl.Float64,
}


class DwdWeather(Table):
    """Table for DWD ICON-D2 weather forecast timeseries data."""
    
    def __init__(self):
        super().__init__(table_name="L0_DwdWeather", schema=DwdWeatherSchema)