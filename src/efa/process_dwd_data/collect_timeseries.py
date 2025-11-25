"""Main script for collecting DWD timeseries data."""
import pandas as pd
from loguru import logger
from efa import config, tables
from efa.process_dwd_data import grib_reader, file_discovery
from collections import defaultdict
from pathlib import Path


def process_variable_from_files(variable: str, level: str | None, 
                                file_paths: list[Path], locations: list[dict]) -> dict[int, pd.Series]:
    """Process pre-discovered files for all locations.
    
    Strategy: Read each file once, extract all locations simultaneously.
    
    Args:
        variable: Variable name
        level: Model level or None
        file_paths: List of file paths to process
        locations: List of dicts with 'lat', 'lon' keys
        
    Returns:
        Dict mapping location_index -> pd.Series (timeseries)
    """
    col_name = f"{variable}_level{level}" if level else variable
    logger.info(f"Processing {col_name} ({len(file_paths)} files)...")
    
    # Store data for each location
    location_data = defaultdict(list)
    processed = 0
    failed = 0
    
    for file_path in file_paths:
        try:
            # Extract all locations from this file at once
            results = grib_reader.extract_multiple_points(str(file_path), locations)
            
            # Add to each location's data
            for loc_idx, (time, value) in results.items():
                location_data[loc_idx].append({'time': time, 'value': value})
            
            processed += 1
            
            if processed % 500 == 0:  # Log every 500 files
                logger.info(f"  Progress: {processed}/{len(file_paths)} files")
        except Exception as e:
            failed += 1
            if failed <= 3:  # Only show first 3 errors
                logger.warning(f"Failed: {file_path.name}: {e}")
    
    if failed > 3:
        logger.warning(f"... and {failed - 3} more failures (suppressed)")
    
    # Convert to Series for each location
    result_series = {}
    for loc_idx, data in location_data.items():
        if not data:
            logger.warning(f"No data collected for {col_name} at location {loc_idx}")
            result_series[loc_idx] = pd.Series(dtype=float)
            continue
        
        # Create Series and remove duplicates
        df = pd.DataFrame(data).set_index('time')['value']
        df = df[~df.index.duplicated(keep='last')].sort_index()
        result_series[loc_idx] = df
        
        logger.debug(f"Location {loc_idx}: {len(df)} records")
    
    logger.info(f"✓ Success rate: {processed}/{len(file_paths)} ({100*processed/len(file_paths):.1f}%)")
    
    return result_series


def main():
    """Collect DWD weather timeseries and write to database."""
    logger.info("=" * 60)
    logger.info("Starting DWD data collection pipeline")
    logger.info(f"Locations: {len(config.locations)}")
    for idx, loc in enumerate(config.locations):
        logger.info(f"  [{idx}] lat={loc['lat']}, lon={loc['lon']}")
    logger.info("=" * 60)
    
    # Step 1: Discover variables
    logger.info("\nStep 1: Discovering available variables...")
    variables = file_discovery.discover_variables(config.mount_path)
    total_vars = sum(len(levels) for levels in variables.values())
    logger.info(f"Found {total_vars} variable-level combinations")
    logger.info(f"Variables: {list(variables.keys())}")
    
    # Step 2: Find ALL files at once (BIG TIME SAVER!)
    logger.info("\nStep 2: Finding all files (ONE-TIME SCAN of all directories)...")
    all_files = file_discovery.find_all_files_once(config.mount_path, max_forecast_hours=2)
    logger.info("✓ File discovery complete!\n")
    
    # Step 3: Process each variable
    logger.info("Step 3: Processing variables (batch mode - all locations at once)...")
    all_location_data = defaultdict(dict)  # location_index -> {col_name -> Series}
    current = 0
    
    for var, levels in variables.items():
        for level in levels:
            current += 1
            col_name = f"{var}_level{level}" if level else var
            logger.info(f"\n[{current}/{total_vars}] {col_name}")
            
            # Get pre-discovered files
            key = (var, level)
            if key not in all_files:
                logger.warning(f"No files found for {col_name}")
                continue
            
            file_paths = all_files[key]
            
            # Process all locations at once from these files
            location_series = process_variable_from_files(
                var, level, file_paths, config.locations
            )
            
            # Store series for each location
            for loc_idx, series in location_series.items():
                all_location_data[loc_idx][col_name] = series
    
    # Step 4: Build DataFrames
    logger.info("\n\nStep 4: Building DataFrames for each location...")
    location_dataframes = []
    
    for loc_idx, series_dict in all_location_data.items():
        df = pd.DataFrame(series_dict)
        df = df.reset_index().rename(columns={'index': 'time'})
        
        # Add location metadata
        df['latitude'] = config.locations[loc_idx]['lat']
        df['longitude'] = config.locations[loc_idx]['lon']
        
        location_dataframes.append(df)
        
        logger.info(f"Location {loc_idx}: shape={df.shape}, time range={df['time'].min()} to {df['time'].max()}")
    
    # Step 5: Write to database
    logger.info("\nStep 5: Writing to database...")
    
    # Combine all locations into single DataFrame
    combined_df = pd.concat(location_dataframes, ignore_index=True)
    logger.info(f"Combined dataset shape: {combined_df.shape}")
    logger.info(f"Time range: {combined_df['time'].min()} to {combined_df['time'].max()}")
    logger.info(f"Missing values: {combined_df.isna().sum().sum()}")
    
    tables.L0.DwdWeather().write(df=combined_df, mode="replace")
    logger.info("✓ Data successfully written to database")
    
    # Cleanup
    logger.info("\nStep 6: Cleaning up temporary files...")
    grib_reader.cleanup_all()
    logger.info("✓ Cleanup complete")
    
    logger.info("\n" + "=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()