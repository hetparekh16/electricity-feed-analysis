"""File discovery and variable detection."""
from pathlib import Path
from loguru import logger
from collections import defaultdict


def discover_variables(data_path: str) -> dict[str, list[str | None]]:
    """Discover available variables by scanning one folder.
    
    Args:
        data_path: Path to data directory
        
    Returns:
        Dict mapping variable_name -> list of levels (None for single-level)
        Example: {'u': ['61', '62'], 't_2m': [None]}
    """
    # Find first valid subdirectory (skip 'today' if present)
    base_path = Path(data_path)
    sample_dir = None
    for subdir in sorted(base_path.iterdir()):
        if subdir.is_dir() and subdir.name != 'today':
            sample_dir = subdir
            break
    
    if not sample_dir:
        raise FileNotFoundError(f"No valid subdirectories found in {data_path}")
    
    logger.info(f"Scanning {sample_dir.name} to discover variables...")
    
    variables = {}
    
    # Model-level files: icon-d2_de_lat-lon_model-level_*_???_61_u.grb2
    for file_path in sample_dir.glob("icon-d2_de_lat-lon_model-level_*.grb2"):
        if 'icon-d2-eps' in file_path.name:
            continue
        
        parts = file_path.stem.split('_')
        level = parts[6]  # e.g., "61"
        var = parts[7]    # e.g., "u"
        
        if var not in variables:
            variables[var] = []
        if level not in variables[var]:
            variables[var].append(level)
    
    # Single-level files: icon-d2_de_lat-lon_single-level_*_???_2d_t_2m.grb2
    for file_path in sample_dir.glob("icon-d2_de_lat-lon_single-level_*.grb2"):
        if 'icon-d2-eps' in file_path.name:
            continue
        
        parts = file_path.stem.split('_')
        var = '_'.join(parts[7:])  # e.g., "t_2m", "u_10m"
        
        if var not in variables:
            variables[var] = [None]
    
    # Sort levels for consistency
    for var in variables:
        if variables[var][0] is not None:
            variables[var] = sorted(variables[var])
    
    logger.info(f"Found {len(variables)} variables: {list(variables.keys())}")
    return variables


def find_variable_files(data_path: str, variable: str, level: str | None = None, 
                       max_forecast_hours: int = 2) -> list[tuple[Path, int]]:
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
    base_path = Path(data_path)
    
    # Get sorted list of run directories
    run_dirs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name != 'today'])
    total_dirs = len(run_dirs)
    logger.info(f"Found {total_dirs} run directories to scan")
    
    files_to_process = []
    
    # For each run, do ONE glob and filter results
    for idx, run_dir in enumerate(run_dirs, 1):
        if idx % 1000 == 0:
            logger.info(f"  Scanned {idx}/{total_dirs} directories...")
        
        # Single glob per directory! The ??? wildcard matches any 3-digit hour
        for file_path in run_dir.glob(pattern):
            if 'icon-d2-eps' in file_path.name:
                continue
            
            # Extract forecast hour from filename
            parts = file_path.stem.split('_')
            forecast_hour = int(parts[5])
            
            # Only keep files within our hour range (0, 1, 2)
            if forecast_hour <= max_forecast_hours:
                files_to_process.append((file_path, forecast_hour))
    
    logger.info(f"Selected {len(files_to_process)} files (first {max_forecast_hours + 1} hours from each run)")
    
    return files_to_process


def find_all_files_once(data_path: str, max_forecast_hours: int = 2) -> dict[tuple, list[Path]]:
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
    
    base_path = Path(data_path)
    run_dirs = sorted([d for d in base_path.iterdir() if d.is_dir() and d.name != 'today'])
    total_dirs = len(run_dirs)
    logger.info(f"Found {total_dirs} run directories to scan")
    
    # Store files grouped by (variable, level)
    files_by_var = defaultdict(list)
    
    for idx, run_dir in enumerate(run_dirs, 1):
        if idx % 1000 == 0:
            logger.info(f"  Scanned {idx}/{total_dirs} directories...")
        
        # Get ALL deterministic .grb2 files in this directory at once
        for file_path in run_dir.glob("icon-d2_de_*.grb2"):
            # Skip EPS files
            if 'icon-d2-eps' in file_path.name:
                continue
            
            parts = file_path.stem.split('_')
            
            # Extract forecast hour
            try:
                forecast_hour = int(parts[5])
            except (IndexError, ValueError):
                continue
            
            # Only keep first N hours
            if forecast_hour > max_forecast_hours:
                continue
            
            # Parse variable and level
            if 'model-level' in file_path.name:
                # Model-level: icon-d2_de_lat-lon_model-level_*_???_61_u.grb2
                try:
                    level = parts[6]
                    var = parts[7]
                    key = (var, level)
                except IndexError:
                    continue
            else:
                # Single-level: icon-d2_de_lat-lon_single-level_*_???_2d_t_2m.grb2
                try:
                    var = '_'.join(parts[7:])
                    key = (var, None)
                except IndexError:
                    continue
            
            files_by_var[key].append(file_path)
    
    total_files = sum(len(files) for files in files_by_var.values())
    logger.info(f"✓ Found {total_files} files for {len(files_by_var)} variable-level combinations")
    
    return files_by_var
