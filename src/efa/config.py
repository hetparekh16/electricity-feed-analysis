forecast_hours = list(range(0, 49))  # 0 to 48 hours 

# Multiple locations (lat, lon pairs)
locations = [
    {'lat': 53.908585, 'lon': 9.193248},   # Location 1
    {'lat': 53.518114, 'lon': 9.918907},   # Location 2
    {'lat': 49.735281, 'lon': 9.703521},   # Neubrunn
    {'lat': 51.160670, 'lon': 12.410260},  # Energiepark
]


dwd_data_path = "data/historical_data"
remote_path = 'smb://triton.ieet.tuhh.de/dwd-data/'
mount_path = '/Volumes/dwd-data/'