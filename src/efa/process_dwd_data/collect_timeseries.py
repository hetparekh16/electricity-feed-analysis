"""Main script for collecting DWD timeseries data."""
import polars as pl
from loguru import logger
from collections import defaultdict

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
    
    return variables


def collect_all_variables(variables: dict, all_files: dict) -> pl.DataFrame:
    """Collect data for all variables and build the final dataset.

    Parameters
    ----------
    variables : `dict`
        Dictionary of available variables and their levels.
    all_files : `dict`
        Dictionary mapping (variable, level) to file paths.

    Returns
    -------
    pl.DataFrame
        Combined DataFrame containing all variables for all locations.
    """
    logger.info("Step 3: Processing variables (batch mode - all locations at once)...")
    all_location_data = defaultdict(dict)  # location_index -> {col_name -> DataFrame}
    
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
            location_dfs = processor.process_variable_from_files(
                var, level, file_paths, config.locations
            )
            
            # Store dataframe for each location
            for loc_idx, df in location_dfs.items():
                all_location_data[loc_idx][col_name] = df
    
    # Step 4: Build DataFrames
    return processor.build_dataset(all_location_data, config.locations)


def run():
    """Execute the DWD data collection pipeline.

    Pipeline steps:
    1. Discover variables and files
    2. Collect data for all variables
    3. Save data to database
    4. Cleanup temporary files
    """
    try:
        # 0. Initialize workspace (clean temp files)
        # MUST be done before parallel workers start to avoid race conditions
        grib_reader.initialize_workspace()

        # 1. Discover resources
        variables = discover_resources()

        # 2. Find ALL files at once
        all_files = file_discovery.find_all_files_once(DATA_PATH, max_forecast_hours=2)
        
        # 3. Collect data
        combined_df = collect_all_variables(variables, all_files)
        
        # 4. Save data
        tables.L0.DwdWeather().write(df=combined_df, mode="replace")
        logger.info("Data successfully written to database")
        
    finally:
        # 5. Cleanup
        logger.info("\nStep 6: Cleaning up temporary files...")
        grib_reader.cleanup_all()
        logger.info("Cleanup complete")
        logger.info("=" * 60)


if __name__ == "__main__":
    run()