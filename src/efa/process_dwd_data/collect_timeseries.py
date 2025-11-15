import os
import pandas as pd
from efa.process_dwd_data.utils import get_available_variables, collect_timeseries_for_each_metric


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
        print(f"{var}: {levels}")

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
        print(f"\nSaved to: {output_file}")

    else:
        print("\nNo data collected.")


if __name__ == "__main__":
    main()