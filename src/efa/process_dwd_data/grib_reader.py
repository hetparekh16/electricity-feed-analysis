"""Low-level GRIB file reading operations."""
import os
import shutil
import xarray as xr
import polars as pl
from pathlib import Path
from loguru import logger

# Single temp directory for all GRIB operations
TEMP_DIR = Path('data/temp/grib_processing')
# Remove and recreate to ensure clean permissions
if TEMP_DIR.exists():
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)
os.environ['CFGRIB_INDEXPATH'] = str(TEMP_DIR)


def extract_multiple_points(file_path: Path, locations: list[dict], direct_read: bool = True) -> dict[int, tuple[pl.Timestamp, float]]:
    """Extract values for multiple locations from a single GRIB file.
    
    This is much faster than reading the file multiple times.
    
    Args:
        file_path: Path to GRIB2 file
        locations: List of dicts with 'lat', 'lon' keys
        direct_read: If True, read directly from source without copying (faster)
        
    Returns:
        Dict mapping location_index -> (valid_time, value)
    """
    path_obj = file_path
    
    if direct_read:
        # Use a unique index file path for this process/file to avoid collisions
        # We hash the full path to get a unique but consistent filename
        import hashlib
        file_hash = hashlib.md5(str(path_obj).encode()).hexdigest()
        index_path = TEMP_DIR / f"{path_obj.name}_{file_hash}.idx"
        
        target_file = path_obj
        backend_kwargs = {'indexpath': str(index_path)}
    else:
        # Copy to temp (legacy mode)
        target_file = TEMP_DIR / path_obj.name
        # Remove if already exists to avoid permission issues
        if target_file.exists():
            target_file.unlink()
        shutil.copy(file_path, target_file)
        backend_kwargs = {}
    
    try:
        ds = xr.open_dataset(target_file, engine='cfgrib', backend_kwargs=backend_kwargs)
        var_name = list(ds.data_vars)[0]
        
        # Get valid time once
        if 'valid_time' in ds.coords:
            time = pl.Timestamp(ds['valid_time'].values.flat[0])
        else:
            time = pl.Timestamp(ds['time'].values.flat[0])
            if 'step' in ds.coords:
                time += pl.Timedelta(ds['step'].values.flat[0])
        
        # Extract values for all locations
        results = {}
        for idx, loc in enumerate(locations):
            try:
                val_array = ds[var_name].sel(
                    latitude=loc['lat'], 
                    longitude=loc['lon'], 
                    method='nearest'
                )
                
                if val_array.size > 1:
                    # If multiple values (e.g. multiple steps/members), take the first one
                    value = float(val_array.values.flat[0])
                else:
                    value = float(val_array.values)
                results[idx] = (time, value)
            except Exception as e:
                logger.debug(f"Failed to extract location {idx} from {path_obj.name}: {e}")
                continue
        
        ds.close()
        return results
        
    finally:
        if not direct_read:
            _cleanup_temp_file(target_file)
        # For direct read, we might want to keep the index for reuse, or clean it up.
        # Given the volume of files (20k), keeping 20k index files might fill up inodes/space.
        # Let's clean up index files for now to be safe.
        if direct_read:
             # Clean up the specific index file if we can identify it
             # cfgrib usually creates file.idx or file.hash.idx
             # A simple glob in TEMP_DIR for the filename should work
             for idx_file in TEMP_DIR.glob(f"{path_obj.name}*.idx"):
                 idx_file.unlink(missing_ok=True)


def _cleanup_temp_file(temp_file: Path) -> None:
    """Remove temp file and its index files."""
    temp_file.unlink(missing_ok=True)
    for idx in TEMP_DIR.glob(f"{temp_file.name}*.idx"):
        idx.unlink(missing_ok=True)


def cleanup_all() -> None:
    """Remove all temporary files."""
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
