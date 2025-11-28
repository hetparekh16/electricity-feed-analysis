from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Multiple locations (lat, lon pairs)
locations = [
    {'lat': 53.908585, 'lon': 9.193248},   # Location 1
    {'lat': 53.518114, 'lon': 9.918907},   # Location 2
    {'lat': 49.735281, 'lon': 9.703521},   # Neubrunn
    {'lat': 51.160670, 'lon': 12.410260},  # Energiepark
]

dwd_data_path = PROJECT_ROOT / "data" / "historical_data"

# Mac
remote_path_mac = 'smb://triton.ieet.tuhh.de/dwd-data/'
mount_path_mac = '/Volumes/dwd-data/'

# Windows
remote_path_win = r'\\triton.ieet.tuhh.de\dwd-data\\'
mount_path_win = r"Z:\\dwd-data\"
