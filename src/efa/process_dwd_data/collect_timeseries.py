"""Main script for collecting DWD timeseries data."""
import pandas as pd
from loguru import logger
from collections import defaultdict
from pathlib import Path

from efa import config, tables
from efa.process_dwd_data import grib_reader, file_discovery, processor

DATA_PATH = config.dwd_data_path

def discover_resources() -> tuple[dict, dict]:
    """Discover available variables and files.

    Returns
    -------
    tuple[dict, dict]
        Tuple containing:
        - variables: Dict of variable names to levels.
        - all_files: Dict mapping (variable, level) to list of file paths.
    """
    logger.info("=" * 60)
    logger.info("Starting DWD data collection pipeline")
    logger.info(f"Locations: {len(config.locations)}")
    for idx, loc in enumerate(config.locations):
        logger.info(f"  [{idx}] lat={loc['lat']}, lon={loc['lon']}")
    logger.info("=" * 60)
    
    # Step 1: Discover variables
    logger.info("\nStep 1: Discovering available variables...")
    variables = file_discovery.discover_variables(DATA_PATH)
    total_vars = sum(len(levels) for levels in variables.values())
    logger.info(f"Found {total_vars} variable-level combinations")
    logger.info(f"Variables: {list(variables.keys())}")
    
    # Step 2: Find ALL files at once (BIG TIME SAVER!)
    logger.info("\nStep 2: Finding all files (ONE-TIME SCAN of all directories)...")
    all_files = file_discovery.find_all_files_once(DATA_PATH, max_forecast_hours=2)
    logger.info("✓ File discovery complete!\n")
    
    return variables, all_files


def collect_all_variables(variables: dict, all_files: dict) -> pd.DataFrame:
    """Collect data for all variables and build the final dataset.

    Parameters
    ----------
    variables : `dict`
        Dictionary of available variables and their levels.
    all_files : `dict`
        Dictionary mapping (variable, level) to file paths.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame containing all variables for all locations.
    """
    logger.info("Step 3: Processing variables (batch mode - all locations at once)...")
    all_location_data = defaultdict(dict)  # location_index -> {col_name -> Series}
    
    total_vars = sum(len(levels) for levels in variables.values())
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
            location_series = processor.process_variable_from_files(
                var, level, file_paths, config.locations
            )
            
            # Store series for each location
            for loc_idx, series in location_series.items():
                all_location_data[loc_idx][col_name] = series
    
    # Step 4: Build DataFrames
    return processor.build_dataset(all_location_data, config.locations)


def save_data(df: pd.DataFrame) -> None:
    """Write the collected data to the database.

    Parameters
    ----------
    df : `pd.DataFrame`
        The DataFrame to write.
    """
    logger.info("\nStep 5: Writing to database...")
    logger.info(f"Combined dataset shape: {df.shape}")
    logger.info(f"Time range: {df['time'].min()} to {df['time'].max()}")
    logger.info(f"Missing values: {df.isna().sum().sum()}")
    
    tables.L0.DwdWeather().write(df=df, mode="replace")
    logger.info("✓ Data successfully written to database")


def run():
    """Execute the DWD data collection pipeline.

    Pipeline steps:
    1. Discover variables and files
    2. Collect data for all variables
    3. Save data to database
    4. Cleanup temporary files
    """
    try:
        # 1. Discover resources
        variables, all_files = discover_resources()
        
        # 2. Collect data
        combined_df = collect_all_variables(variables, all_files)
        
        # 3. Save data
        save_data(combined_df)
        
        logger.success("------ Pipeline completed successfully! ------")
        
    finally:
        # 4. Cleanup
        logger.info("\nStep 6: Cleaning up temporary files...")
        grib_reader.cleanup_all()
        logger.info("✓ Cleanup complete")
        logger.info("=" * 60)


if __name__ == "__main__":
    run()