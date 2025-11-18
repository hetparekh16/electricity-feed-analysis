from efa.process_dwd_data.utils import get_available_variables, collect_timeseries_for_each_metric
from efa import config
from efa.core import save_to_duckdb

def main():
    """
    Main function to collect timeseries data for DWD weather variables.
    """

    print("Discovering available variables (deterministic forecasts only)...")
    variables_info = get_available_variables(config.dwd_data_path, include_eps=False)

    print(f"\nProcessing data for location: lat={config.lat}, lon={config.lon}")
    
    dfs = collect_timeseries_for_each_metric(
        config.dwd_data_path, 
        variables_info, 
        levels=None, 
        lat=config.lat, 
        lon=config.lon, 
        forecast_hours=config.forecast_hours
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
        result['latitude'] = config.lat
        result['longitude'] = config.lon

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

        # Save to DuckDB
        save_to_duckdb(df=result, table_name="weather_timeseries", mode="append")

    else:
        print("\nNo data collected.")


if __name__ == "__main__":
    main()