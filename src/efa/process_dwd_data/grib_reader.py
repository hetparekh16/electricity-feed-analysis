"""Low-level GRIB file reading operations."""
import os
import shutil
import xarray as xr
import pandas as pd
from pathlib import Path
from loguru import logger

# Single temp directory for all GRIB operations
TEMP_DIR = Path('data/temp/grib_processing')
# Remove and recreate to ensure clean permissions
if TEMP_DIR.exists():
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
os.environ['CFGRIB_INDEXPATH'] = str(TEMP_DIR)


def extract_multiple_points(file_path: str, locations: list[dict]) -> dict[int, tuple[pd.Timestamp, float]]:
    """Extract values for multiple locations from a single GRIB file.
    
    This is much faster than reading the file multiple times.
    
    Args:
        file_path: Path to GRIB2 file
        locations: List of dicts with 'lat', 'lon' keys
        
    Returns:
        Dict mapping location_index -> (valid_time, value)
    """
    # Copy to temp (cfgrib needs write access for index)
    temp_file = TEMP_DIR / Path(file_path).name
    
    # Remove if already exists to avoid permission issues
    if temp_file.exists():
        temp_file.unlink()
    
    shutil.copy(file_path, temp_file)
    
    try:
        ds = xr.open_dataset(temp_file, engine='cfgrib')
        var_name = list(ds.data_vars)[0]
        
        # Get valid time once
        if 'valid_time' in ds.coords:
            time = pd.Timestamp(ds['valid_time'].values.flat[0])
        else:
            time = pd.Timestamp(ds['time'].values.flat[0])
            if 'step' in ds.coords:
                time += pd.Timedelta(ds['step'].values.flat[0])
        
        # Extract values for all locations
        results = {}
        for idx, loc in enumerate(locations):
            try:
                value = float(ds[var_name].sel(
                    latitude=loc['lat'], 
                    longitude=loc['lon'], 
                    method='nearest'
                ).values)
                results[idx] = (time, value)
            except Exception as e:
                logger.debug(f"Failed to extract location {idx} from {Path(file_path).name}: {e}")
                continue
        
        return results
    finally:
        ds.close()
        _cleanup_temp_file(temp_file)


def _cleanup_temp_file(temp_file: Path) -> None:
    """Remove temp file and its index files."""
    temp_file.unlink(missing_ok=True)
    for idx in TEMP_DIR.glob(f"{temp_file.name}*.idx"):
        idx.unlink(missing_ok=True)


def cleanup_all() -> None:
    """Remove all temporary files."""
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
