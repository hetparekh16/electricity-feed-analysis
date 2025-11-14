import os
import glob
import shutil

os.environ['CFGRIB_INDEXPATH'] = os.path.join('data', 'temp', 'cfgrib_idx')

import xarray as xr
import pandas as pd
import numpy as np


def get_file_list(data_path, variable, forecast_hour=0):
    """
    Get list of GRB2 files for a specific variable and forecast hour.
    Excludes ensemble prediction (icon-d2-eps) files and .idx files.
    """
    model_level_pattern = os.path.join(data_path, "*", f"icon-d2_de_lat-lon_model-level_*_{forecast_hour:03d}_*_{variable}.grb2")
    single_level_pattern = os.path.join(data_path, "*", f"icon-d2_de_lat-lon_single-level_*_{forecast_hour:03d}_*_{variable}.grb2")
    
    files = glob.glob(model_level_pattern) + glob.glob(single_level_pattern)
    
    # Filter out .idx files and icon-d2-eps files
    files = [f for f in files if not f.endswith('.idx') and 'icon-d2-eps' not in f]
    
    return sorted(files)


def clean_index_files(file_path):
    """
    Remove any existing index files for a GRIB2 file.
    
    Args:
        file_path (str): Path to the GRIB2 file
    """
    # Pattern for index files created by cfgrib
    idx_patterns = [
        file_path + '.*.idx',
        file_path + '.idx'
    ]
    
    for pattern in idx_patterns:
        for idx_file in glob.glob(pattern):
            try:
                os.remove(idx_file)
            except Exception:
                pass


def load_grib_value(file_path, lat, lon, temp_dir):
    """
    Load GRB2 file and extract value at specific lat/lon.
    Index files are created in temp_dir.

    Args:
        file_path (str): Path to GRB2 file
        lat (float): Latitude
        lon (float): Longitude
        temp_dir (str): Temporary directory for index files

    Returns:
        tuple: (valid_time, value) - The valid time and value at the location
    """
    # Clean up any existing corrupted index files
    clean_index_files(file_path)
    
    # Set CFGRIB index path to temp directory
    os.environ['CFGRIB_INDEXPATH'] = temp_dir
    
    try:
        # Load with xarray
        ds = xr.open_dataset(file_path, engine='cfgrib')

        # Get the data variable
        var_name = list(ds.data_vars)[0]

        # Select value at nearest lat/lon
        value = ds[var_name].sel(latitude=lat, longitude=lon, method='nearest').values

        # Convert to scalar
        if np.ndim(value) > 0:
            value = value.flatten()[0]  # Take first element if array
        value = float(value)

        # Extract valid_time from the dataset
        if 'valid_time' in ds.coords:
            # Get valid_time coordinate and convert to timestamp
            vt = ds['valid_time'].values
            # Handle if it's an array
            if np.ndim(vt) > 0:
                vt = vt.flatten()[0]
            valid_time = pd.Timestamp(vt)
        elif 'time' in ds.coords and 'step' in ds.coords:
            # Calculate valid_time from time + step
            time_val = ds['time'].values
            step_val = ds['step'].values
            
            # Handle arrays
            if np.ndim(time_val) > 0:
                time_val = time_val.flatten()[0]
            if np.ndim(step_val) > 0:
                step_val = step_val.flatten()[0]
                
            time = pd.Timestamp(time_val)
            step = pd.Timedelta(step_val)
            valid_time = time + step
        else:
            # Fallback: use time if valid_time not available
            time_val = ds['time'].values
            if np.ndim(time_val) > 0:
                time_val = time_val.flatten()[0]
            valid_time = pd.Timestamp(time_val)

        ds.close()
        return valid_time, value

    except Exception as e:
        raise Exception(f"Error reading {file_path}: {str(e)}")


def process_files_for_location(data_path, variable, lat, lon, forecast_hour=0):
    """
    Process all files for a variable and location, return dataframe.
    Creates a temporary directory for index files that is cleaned up after processing.

    Args:
        data_path (str): Data path
        variable (str): Variable name
        lat (float): Latitude
        lon (float): Longitude
        forecast_hour (int): Forecast hour

    Returns:
        pd.DataFrame: DataFrame with time and value
    """
    files = get_file_list(data_path, variable, forecast_hour)
    data = []

    # Create temp directory for index files
    temp_dir = os.path.join('data', 'temp', f'cfgrib_idx_{variable}_{forecast_hour}')
    os.makedirs(temp_dir, exist_ok=True)

    try:
        for file_path in files:
            try:
                valid_time, value = load_grib_value(file_path, lat, lon, temp_dir)
                data.append({'time': valid_time, 'value': value})
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                continue
    finally:
        # Cleanup temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    df = pd.DataFrame(data)
    if not df.empty:
        df = df.set_index('time')
    return df