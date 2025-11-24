from efa.process_dwd_data.utils import get_available_variables, collect_timeseries_for_each_metric, merge_metrics_to_dataframe
from efa import config
from efa import tables
from loguru import logger

def main():
    """
    Main function to collect timeseries data for DWD weather variables.
    """

    logger.info("Discovering available variables (deterministic forecasts only)...")
    variables_info = get_available_variables(config.dwd_data_path, include_eps=False)

    logger.info(f"\nProcessing data for location: lat={config.lat}, lon={config.lon}")
    dfs = collect_timeseries_for_each_metric(
        config.dwd_data_path, 
        variables_info, 
        levels=None, 
        lat=config.lat, 
        lon=config.lon, 
        forecast_hours=config.forecast_hours
    )

    result = merge_metrics_to_dataframe(dfs)

    tables.L0.DwdWeather().write(df=result, mode="replace")

if __name__ == "__main__":
    main()