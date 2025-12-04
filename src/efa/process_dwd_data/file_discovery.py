"""File discovery and variable detection."""
from pathlib import Path
from loguru import logger
from collections import defaultdict
import re
import time
from typing import Union, Optional, Dict, List, Tuple


def parse_dwd_filename(filename: str) -> Optional[dict]:
    """Parse DWD GRIB2 filename using regex.
    
    Matches patterns like:
    - icon-d2_de_lat-lon_model-level_2021010100_000_61_u.grb2
    - icon-d2_de_lat-lon_single-level_2021010100_000_2d_t_2m.grb2
    """
    # Regex pattern with named groups
    pattern = r"icon-d2_de_lat-lon_(?P<type>model-level|single-level)_(?P<date>\d{10})_(?P<hour>\d{3})_(?P<level_or_2d>.*?)_(?P<var>.*)\.grb2"
    match = re.match(pattern, filename)
    
    if not match:
        return None
        
    data = match.groupdict()
    
    if data['type'] == 'model-level':
        return {
            'type': 'model-level',
            'date': data['date'],
            'forecast_hour': int(data['hour']),
            'level': data['level_or_2d'],
            'variable': data['var']
        }
    else:
        # single-level
        return {
            'type': 'single-level',
            'date': data['date'],
            'forecast_hour': int(data['hour']),
            'level': None,
            'variable': data['var']
        }


def discover_variables(data_path: Path) -> Dict[str, List[Optional[str]]]:
    """Discover available variables by scanning one folder.
    
    Args:
        data_path: Path to data directory
        
    Returns:
        Dict mapping variable_name -> list of levels (None for single-level)
        Example: {'u': ['61', '62'], 't_2m': [None]}
    """
    # Find first valid subdirectory (skip 'today' if present)
    base_path = data_path
    sample_dir = None
    for subdir in sorted(base_path.iterdir()):
        if subdir.is_dir() and subdir.name != 'today':
            sample_dir = subdir
            break
    
    if not sample_dir:
        raise FileNotFoundError(f"No valid subdirectories found in {data_path}")
    
    logger.info(f"Scanning {sample_dir.name} to discover variables...")
    
    variables = {}
    
    # Scan files using regex parser
    for file_path in sample_dir.glob("icon-d2_de_*.grb2"):
        if 'icon-d2-eps' in file_path.name:
            continue
            
        parsed = parse_dwd_filename(file_path.name)
        if not parsed:
            continue
            
        var = parsed['variable']
        level = parsed['level']
        
        if var not in variables:
            variables[var] = []
            
        # For single-level, level is None. For model-level, it's a string.
        # We need to handle the list structure correctly.
        if level is None:
            # Single level variable
            if None not in variables[var]:
                variables[var].append(None)
        else:
            # Model level variable
            if level not in variables[var]:
                variables[var].append(level)
    
    # Sort levels for consistency
    for var in variables:
        if variables[var][0] is not None:
            variables[var] = sorted(variables[var])
    
    logger.info(f"Found {len(variables)} variables: {list(variables.keys())}")
    return variables


def find_variable_files(data_path: Path, variable: str, level: Optional[str] = None, 
                       max_forecast_hours: int = 2) -> List[Tuple[Path, int]]:
    """Find files for a variable, only first N hours from each run.
    
    Strategy: Use only the freshest forecasts (0-2 hours) from each run to build
    a continuous timeseries. This avoids using degraded long-range forecasts.
    
    OPTIMIZED: Uses single glob per directory instead of multiple globs.
    
    Args:
        data_path: Path to data directory
        variable: Variable name (e.g., 'u', 't_2m')
        level: Model level (e.g., '61') or None for single-level
        max_forecast_hours: Maximum forecast hour to include (default 2 = hours 0,1,2)
        
    Returns:
        List of tuples (file_path, forecast_hour) for processing
    """
    # Build pattern with wildcard for hour - matches ALL forecast hours
    if level:
        pattern = f"icon-d2_de_lat-lon_model-level_*_???_{level}_{variable}.grb2"
    else:
        pattern = f"icon-d2_de_lat-lon_single-level_*_???_2d_{variable}.grb2"
    
    logger.info(f"Collecting first {max_forecast_hours + 1} hours (000-{max_forecast_hours:03d}) from each run...")
    base_path = data_path
    
    # Get sorted list of run directories
    run_dirs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name != 'today'])
    total_dirs = len(run_dirs)
    logger.info(f"Found {total_dirs} run directories to scan")
    
    files_to_process = []
    
    # For each run, do ONE glob and filter results
    last_log_time = time.time()
    for idx, run_dir in enumerate(run_dirs, 1):
        current_time = time.time()
        if current_time - last_log_time >= 30:
            logger.info(f"  Scanned {idx}/{total_dirs} directories...")
            last_log_time = current_time
        
        # Single glob per directory! The ??? wildcard matches any 3-digit hour
        for file_path in run_dir.glob(pattern):
            if 'icon-d2-eps' in file_path.name:
                continue
            
            # Extract forecast hour using regex
            parsed = parse_dwd_filename(file_path.name)
            if not parsed:
                continue
                
            forecast_hour = parsed['forecast_hour']
            
            # Only keep files within our hour range (0, 1, 2)
            if forecast_hour <= max_forecast_hours:
                files_to_process.append((file_path, forecast_hour))
    
    logger.info(f"Selected {len(files_to_process)} files (first {max_forecast_hours + 1} hours from each run)")
    
    return files_to_process


def find_all_files_once(data_path: Path, max_forecast_hours: int = 2) -> Dict[tuple, List[Path]]:
    """Find ALL files for ALL variables in one scan.
    
    This is much faster than scanning directories separately for each variable.
    
    Args:
        data_path: Path to data directory
        max_forecast_hours: Maximum forecast hour to include (default 2 = hours 0,1,2)
        
    Returns:
        Dict mapping (variable, level) -> list of file paths
        Example: {('u', '61'): [path1, path2, ...], ('t_2m', None): [path1, ...]}
    """
    logger.info(f"Scanning all directories ONCE for all variables (forecast hours 0-{max_forecast_hours})...")
    
    base_path = data_path
    run_dirs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name != 'today'])
    total_dirs = len(run_dirs)
    logger.info(f"Found {total_dirs} run directories to scan")
    
    # Store files grouped by (variable, level)
    files_by_var = defaultdict(list)
    
    last_log_time = time.time()
    for idx, run_dir in enumerate(run_dirs, 1):
        current_time = time.time()
        if current_time - last_log_time >= 30:
            logger.info(f"  Scanned {idx}/{total_dirs} directories...")
            last_log_time = current_time
        
        # Get ALL deterministic .grb2 files in this directory at once
        for file_path in run_dir.glob("icon-d2_de_*.grb2"):
            # Skip EPS files
            if 'icon-d2-eps' in file_path.name:
                continue
            
            parsed = parse_dwd_filename(file_path.name)
            if not parsed:
                continue
                
            forecast_hour = parsed['forecast_hour']
            
            # Only keep first N hours
            if forecast_hour > max_forecast_hours:
                continue
                
            var = parsed['variable']
            level = parsed['level']
            key = (var, level)
            
            files_by_var[key].append(file_path)
    
    total_files = sum(len(files) for files in files_by_var.values())
    logger.info(f"Found {total_files} files for {len(files_by_var)} variable-level combinations")
    
    return files_by_var
