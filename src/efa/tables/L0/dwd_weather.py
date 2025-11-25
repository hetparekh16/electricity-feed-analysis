import pandera.pandas as pa
from pandera.typing import Series

from efa.tables.core import Table


class DwdWeatherSchema(pa.DataFrameModel):
    """Schema for DWD ICON-D2 weather forecast timeseries data."""
    
    time: Series[pa.DateTime] = pa.Field(nullable=False, description="Valid forecast time")
    
    # Surface wind components (10m height)
    v_10m: Series[float] = pa.Field(nullable=False, description="Northward wind at 10m (m/s)")
    u_10m: Series[float] = pa.Field(nullable=False, description="Eastward wind at 10m (m/s)")
    
    # Model-level wind components (upper air)
    v_level61: Series[float] = pa.Field(nullable=False, description="Northward wind at ~184m")
    v_level62: Series[float] = pa.Field(nullable=False, description="Northward wind at ~127m")
    v_level63: Series[float] = pa.Field(nullable=False, description="Northward wind at ~78m")
    v_level64: Series[float] = pa.Field(nullable=False, description="Northward wind at ~38m")
    
    u_level61: Series[float] = pa.Field(nullable=False, description="Eastward wind at ~184m")
    u_level62: Series[float] = pa.Field(nullable=False, description="Eastward wind at ~127m")
    u_level63: Series[float] = pa.Field(nullable=False, description="Eastward wind at ~78m")
    u_level64: Series[float] = pa.Field(nullable=False, description="Eastward wind at ~38m")
    
    # Solar radiation
    aswdir_s: Series[float] = pa.Field(ge=0, nullable=False, description="Direct shortwave radiation (W/m²)")
    aswdifd_s: Series[float] = pa.Field(ge=0, nullable=False, description="Diffuse shortwave radiation (W/m²)")
    
    # Temperature
    t_2m: Series[float] = pa.Field(ge=200, le=350, nullable=False, description="Temperature at 2m (K)")
    
    # Location
    latitude: Series[float] = pa.Field(ge=-90, le=90, nullable=False, description="Latitude")
    longitude: Series[float] = pa.Field(ge=-180, le=180, nullable=False, description="Longitude")

    class Config:
        strict = "filter"  # Allow extra columns but validate specified ones
        coerce = True  # Coerce types where possible


class DwdWeather(Table):
    """Table for DWD ICON-D2 weather forecast timeseries data."""
    
    def __init__(self):
        super().__init__(table_name="L0_DwdWeather", schema=DwdWeatherSchema)