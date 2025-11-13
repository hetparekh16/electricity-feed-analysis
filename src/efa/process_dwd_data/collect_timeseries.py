import os
import glob
import pandas as pd
from efa.process_dwd_data.utils import process_files_for_location


def get_available_variables(data_path):
    """
    Extract all available variable names from the data folder.
    
    Args:
        data_path (str): Path to historical data directory
        
    Returns:
        list: List of unique variable names
    """
    # Pattern: icon-d2_de_lat-lon_*_YYYYMMDDHH_FFF_*_{variable}.grb2
    pattern = os.path.join(data_path, "*", "*.grb2*")
    files = glob.glob(pattern)
    
    variables = set()
    for file_path in files:
        filename = os.path.basename(file_path)
        # Remove .bz2 extension if present
        if filename.endswith('.bz2'):
            filename = filename[:-4]
        
        if not filename.endswith('.grb2'):
            continue
            
        # Remove .grb2 extension
        filename = filename.replace('.grb2', '')
        
        # Split by underscore
        parts = filename.split('_')
        
        # Determine if it's model-level or single-level
        # Model-level format: icon-d2_de_lat-lon_model-level_YYYYMMDDHH_FFF_LEVEL_VARIABLE
        # Single-level format: icon-d2_de_lat-lon_single-level_YYYYMMDDHH_FFF_2d_VARIABLE
        
        if 'model-level' in filename:
            # For model-level: variable is everything after level number
            # icon-d2_de_lat-lon_model-level_YYYYMMDDHH_FFF_LEVEL_VARIABLE
            # Parts: ['icon-d2', 'de', 'lat-lon', 'model-level', 'YYYYMMDDHH', 'FFF', 'LEVEL', 'VARIABLE']
            if len(parts) >= 8:
                variable = '_'.join(parts[7:])  # Join remaining parts after level
                variables.add(variable)
        elif 'single-level' in filename:
            # For single-level: variable is everything after '2d'
            # icon-d2_de_lat-lon_single-level_YYYYMMDDHH_FFF_2d_VARIABLE
            # Parts: ['icon-d2', 'de', 'lat-lon', 'single-level', 'YYYYMMDDHH', 'FFF', '2d', 'VARIABLE']
            if len(parts) >= 8:
                variable = '_'.join(parts[7:])  # Join remaining parts after '2d'
                variables.add(variable)
    
    return sorted(list(variables))


def main():
    """
    Main function to collect timeseries data for DWD weather variables.
    """
    # Hardcoded configuration
    data_path = "data/historical_data"
    output_path = "data/processed"
    forecast_hours = range(0, 49)  # Process forecast hours 0-48
    
    lat = 53.495
    lon = 10.011

    # Discover available variables
    print("Discovering available variables...")
    variables = get_available_variables(data_path)
    print(f"Found {len(variables)} variables: {variables}")

    print(f"\nProcessing data for location: lat={lat}, lon={lon}")
    
    # Collect data for each variable and forecast hour
    dfs = {}
    for var in variables:
        print(f"Processing variable: {var}")
        all_data = []
        
        for forecast_hour in forecast_hours:
            try:
                df = process_files_for_location(data_path, var, lat, lon, forecast_hour)
                if not df.empty:
                    all_data.append(df)
            except Exception as e:
                print(f"  Error processing {var} at forecast hour {forecast_hour}: {e}")
                continue
        
        if all_data:
            # Combine all forecast hours and remove duplicates (keep first)
            combined_df = pd.concat(all_data).sort_index()
            combined_df = combined_df[~combined_df.index.duplicated(keep='first')]
            dfs[var] = combined_df
            print(f"  Collected {len(combined_df)} records for {var}")
        else:
            print(f"  No data found for {var}")


if __name__ == "__main__":
    main()