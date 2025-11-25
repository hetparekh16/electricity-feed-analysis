import pandas as pd
from loguru import logger
from collections import defaultdict
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from efa.process_dwd_data import grib_reader

def _process_chunk(args):
    """Helper for parallel processing."""
    file_paths, locations, variable, level = args
    results = defaultdict(list)
    processed = 0
    failed = 0
    
    for file_path in file_paths:
        try:
            # Use direct read (True) for better performance
            file_results = grib_reader.extract_multiple_points(str(file_path), locations, direct_read=True)
            
            for loc_idx, (time, value) in file_results.items():
                results[loc_idx].append({'time': time, 'value': value})
            processed += 1
        except Exception as e:
            failed += 1
            # Don't log every failure in worker to avoid clutter, maybe just count
            
    return results, processed, failed


def process_variable_from_files(variable: str, level: str | None, 
                                file_paths: list[Path], locations: list[dict]) -> dict[int, pd.Series]:
    """Process pre-discovered files for all locations using PARALLEL processing.

    Parameters
    ----------
    variable : `str`
        Variable name.
    level : `str | None`
        Model level or None.
    file_paths : `list[Path]`
        List of file paths to process.
    locations : `list[dict]`
        List of dicts with 'lat', 'lon' keys.

    Returns
    -------
    dict[int, pd.Series]
        Dict mapping location_index -> pd.Series (timeseries).
    """
    col_name = f"{variable}_level{level}" if level else variable
    logger.info(f"Processing {col_name} ({len(file_paths)} files) with parallel processing...")
    
    # Determine chunk size and workers
    # Use 75% of available CPUs or at least 4
    max_workers = max(4, int((os.cpu_count() or 4) * 0.75))
    chunk_size = max(1, len(file_paths) // (max_workers * 4))
    
    chunks = [file_paths[i:i + chunk_size] for i in range(0, len(file_paths), chunk_size)]
    logger.info(f"Split into {len(chunks)} chunks (approx {chunk_size} files each) using {max_workers} workers")
    
    location_data = defaultdict(list)
    total_processed = 0
    total_failed = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Prepare args for each chunk
        futures = [
            executor.submit(_process_chunk, (chunk, locations, variable, level)) 
            for chunk in chunks
        ]
        
        for future in as_completed(futures):
            chunk_results, processed, failed = future.result()
            total_processed += processed
            total_failed += failed
            
            # Merge results
            for loc_idx, data_list in chunk_results.items():
                location_data[loc_idx].extend(data_list)
                
            if total_processed % 1000 == 0:
                logger.info(f"  Progress: {total_processed}/{len(file_paths)} files")

    if total_failed > 0:
        logger.warning(f"Failed to process {total_failed} files")
    
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
    
    logger.info(f"✓ Success rate: {total_processed}/{len(file_paths)} ({100*total_processed/len(file_paths):.1f}%)")
    
    return result_series

def build_dataset(all_location_data: dict, locations: list[dict]) -> pd.DataFrame:
    """Build final dataset from collected series.

    Parameters
    ----------
    all_location_data : `dict`
        Dictionary mapping location index to column series.
    locations : `list[dict]`
        List of location dictionaries with lat/lon.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame for all locations.
    """
    logger.info("\n\nStep 4: Building DataFrames for each location...")
    location_dataframes = []
    
    for loc_idx, series_dict in all_location_data.items():
        df = pd.DataFrame(series_dict)
        df = df.reset_index().rename(columns={'index': 'time'})
        
        # Add location metadata
        df['latitude'] = locations[loc_idx]['lat']
        df['longitude'] = locations[loc_idx]['lon']
        
        location_dataframes.append(df)
        
        logger.info(f"Location {loc_idx}: shape={df.shape}, time range={df['time'].min()} to {df['time'].max()}")
    
    # Combine all locations into single DataFrame
    combined_df = pd.concat(location_dataframes, ignore_index=True)
    return combined_df
