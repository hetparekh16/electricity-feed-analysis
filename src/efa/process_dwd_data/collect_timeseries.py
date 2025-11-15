import os
import glob
import pandas as pd
from efa.process_dwd_data.utils import process_files_for_location


def get_available_variables(data_path, include_eps=False):
    """
    Extract all available variable names from the data folder.
    
    Args:
        data_path (str): Path to historical data directory
        include_eps (bool): Whether to include EPS files (default: False for deterministic only)
        
    Returns:
        dict: Dictionary with structure {variable: {level_type: [levels]}}
    """
    pattern = os.path.join(data_path, "*", "*.grb2")
    files = glob.glob(pattern)
    
    variables = {}
    
    for file_path in files:
        filename = os.path.basename(file_path)
        
        if not include_eps and 'icon-d2-eps' in filename:
            continue
        
        if not filename.endswith('.grb2'):
            continue
            
        filename = filename.replace('.grb2', '')
        parts = filename.split('_')
        
        if 'model-level' in filename:
            # Format: icon-d2_de_lat-lon_model-level_YYYYMMDDHH_FFF_LEVEL_VARIABLE
            if len(parts) >= 8:
                level = parts[6]  # Level number (61, 62, 63, 64) for metric name (e.g., 'u', 'v')
                variable = '_'.join(parts[7:])
                
                if variable not in variables:
                    variables[variable] = {}
                
                if 'model-level' not in variables[variable]:
                    variables[variable]['model-level'] = set()
                
                variables[variable]['model-level'].add(level)
                
        elif 'single-level' in filename:
            # Format: icon-d2_de_lat-lon_single-level_YYYYMMDDHH_FFF_2d_VARIABLE
            if len(parts) >= 8:
                # Variable is everything after '2d_'
                variable = '_'.join(parts[7:])  # e.g., 'u_10m', 'v_10m', 't_2m', 'aswdir_s', 'aswdifd_s'
                
                if variable not in variables:
                    variables[variable] = {}
                
                variables[variable]['single-level'] = [None]
    
    # Convert sets to sorted lists
    for var in variables:
        for level_type in variables[var]:
            if isinstance(variables[var][level_type], set):
                variables[var][level_type] = sorted(list(variables[var][level_type]))
    
    return variables


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
        except Exception as e:
            print(f"Failed to remove {idx_file}: {e}")


def collect_timeseries_for_each_metric(data_path, variables_info, levels, lat, lon, forecast_hours):

    # Collect data for each variable, level type, and level
    dfs = {}
    
    for var, level_info in variables_info.items():
        for level_type, levels in level_info.items():
            for level in levels:
                # For model-level variables add level suffix to column name
                if level_type == 'model-level':
                    col_name = f"{var}_level{level}"
                    level_arg = level
                else:
                    col_name = var
                    level_arg = None
                
                print(f"Processing {col_name}...")
                all_data = []
                
                for forecast_hour in forecast_hours:
                    try:
                        df = process_files_for_location(
                            data_path, 
                            var, 
                            lat, 
                            lon, 
                            forecast_hour,
                            level=level_arg
                        )
                        if not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        print(f"  Error at forecast hour {forecast_hour}: {e}")
                        continue
                
                if all_data:
                    # Combine all forecast hours and remove duplicates
                    combined_df = pd.concat(all_data).sort_index()
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    dfs[col_name] = combined_df
                    print(f"  ✓ Collected {len(combined_df)} records for {col_name}")
                    cleanup_idx_files(data_path)
                else:
                    print(f"  ✗ No data found for {col_name}")


def main():
    """
    Main function to collect timeseries data for DWD weather variables.
    """
    # Configuration
    data_path = "data/historical_data"
    output_path = "data/processed"
    forecast_hours = range(0, 49)  # Process forecast hours 0-48
    
    lat = 53.495
    lon = 10.011

    # Discover available variables (deterministic only)
    print("Discovering available variables (deterministic forecasts only)...")
    variables_info = get_available_variables(data_path, include_eps=False)
    
    print(f"\nFound {len(variables_info)} variables:")
    for var, levels in variables_info.items():
        print(f"  {var}: {levels}")

    print(f"\nProcessing data for location: lat={lat}, lon={lon}")
    
    dfs = collect_timeseries_for_each_metric(
        data_path, 
        variables_info, 
        levels=None, 
        lat=lat, 
        lon=lon, 
        forecast_hours=forecast_hours
    )

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

        print(f"\n{'='*60}")
        print(f"Final dataset summary:")
        print(f"{'='*60}")
        print(f"Shape: {result.shape}")
        print(f"Time range: {result.index.min()} to {result.index.max()}")
        print(f"Columns: {list(result.columns)}")
        print(f"\nColumn breakdown:")
        for col in result.columns:
            non_null = result[col].notna().sum()
            print(f"  {col}: {non_null} non-null values")

        # Save to parquet
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(output_path, f"timeseries_deterministic_lat{lat}_lon{lon}.parquet")
        result.to_parquet(output_file)
        print(f"\n✓ Saved to: {output_file}")

    else:
        print("\n✗ No data collected.")


if __name__ == "__main__":
    main()