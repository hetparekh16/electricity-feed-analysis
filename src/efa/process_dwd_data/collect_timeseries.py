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
        if 'model-level' in filename:
            if len(parts) >= 8:
                variable = '_'.join(parts[7:])
                variables.add(variable)
        elif 'single-level' in filename:
            if len(parts) >= 8:
                variable = '_'.join(parts[7:])
                variables.add(variable)
    
    return sorted(list(variables))


def cleanup_idx_files(data_path):
    """
    Remove all .idx files from the data directory.
    
    Args:
        data_path (str): Path to data directory
    """
    idx_pattern = os.path.join(data_path, "**", "*.idx")
    idx_files = glob.glob(idx_pattern, recursive=True)
    
    for idx_file in idx_files:
        try:
            os.remove(idx_file)
            print(f"Removed: {idx_file}")
        except Exception as e:
            print(f"Failed to remove {idx_file}: {e}")

            
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
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            dfs[var] = combined_df
            print(f"  Collected {len(combined_df)} records for {var}")
        else:
            print(f"  No data found for {var}")

    # Merge all variables into single dataframe
    if dfs:
        # Start with first variable
        first_var = list(dfs.keys())[0]
        result = dfs[first_var].rename(columns={'value': first_var})

        # Merge others
        for var in list(dfs.keys())[1:]:
            result = result.join(dfs[var].rename(columns={'value': var}), how='outer')

        # Add location columns
        result['latitude'] = lat
        result['longitude'] = lon

        # Sort by time
        result = result.sort_index()

        print(f"\nFinal dataset shape: {result.shape}")
        print(f"Time range: {result.index.min()} to {result.index.max()}")
        print(f"Variables collected: {list(dfs.keys())}")

        # Save to parquet
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, f"timeseries_lat{lat}_lon{lon}.parquet")
        result.to_parquet(output_file)
        print(f"\nSaved to: {output_file}")

    else:
        print("\nNo data collected.")


if __name__ == "__main__":
    main()