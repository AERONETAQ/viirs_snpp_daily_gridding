import time
import json
import os
import yaml
import sys
from joblib import Parallel, delayed
from datetime import datetime, timedelta
from viirs_snpp_daily_gridding import get_earthdata_credentials, process_data, logger

def load_config(config_path: str) -> dict:
    """
    Load configuration from a YAML file.

    Args:
        config_path (str): Path to the configuration YAML file.

    Returns:
        dict: Configuration parameters.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        ValueError: If there is an error decoding the YAML file.
    """
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError as e:
        logger.error(f"An error occurred: {e}")
        raise FileNotFoundError(f"Configuration file '{config_path}' not found. Please ensure it exists in the current directory.")
    except yaml.YAMLError as e:
        logger.error(f"An error occurred: {e}")
        raise ValueError(f"Error decoding '{config_path}'. Ensure the file contains valid YAML.")


def main(
    grid_size=None,
    start_date=None,
    end_date=None,
    data_export_path=None,
    min_lon=None,
    max_lon=None,
    min_lat=None,
    max_lat=None,
    num_cores=None,
    config_path="config.yaml"
):
    """
    Main entry point for VIIRS SNPP daily gridding.
    All parameters are optional; if not provided, they are loaded from config.yaml.
    Returns:
        dict: {
            'success_rate': float,
            'processed_days': list of (year, day),
            'failed_days': list of (year, day)
        }
    """
    try:
        os.environ['MAIN_PID'] = str(os.getpid())
        # Load config if any parameter is missing
        if None in [grid_size, start_date, end_date, data_export_path, min_lon, max_lon, min_lat, max_lat, num_cores]:
            config = load_config(config_path)
            grid_size = grid_size if grid_size is not None else config["grid_size"]
            start_date = start_date if start_date is not None else config["start_date"]
            end_date = end_date if end_date is not None else config["end_date"]
            data_export_path = data_export_path if data_export_path is not None else config["data_export_path"]
            min_lon = min_lon if min_lon is not None else config["min_lon"]
            max_lon = max_lon if max_lon is not None else config["max_lon"]
            min_lat = min_lat if min_lat is not None else config["min_lat"]
            max_lat = max_lat if max_lat is not None else config["max_lat"]
            num_cores = num_cores if num_cores is not None else config["num_cores"]

        # Validate parameters
        if not isinstance(grid_size, (int, float)):
            raise ValueError("grid_size must be a number.")
        for name, val in [("min_lon", min_lon), ("max_lon", max_lon), ("min_lat", min_lat), ("max_lat", max_lat)]:
            if not isinstance(val, (int, float)):
                raise ValueError(f"{name} must be a number.")
        if not isinstance(num_cores, int) or num_cores < 1:
            raise ValueError("num_cores must be a positive integer.")
        for date_name, date_val in [("start_date", start_date), ("end_date", end_date)]:
            if not (isinstance(date_val, str) and len(date_val) == 8 and date_val.isdigit()):
                raise ValueError(f"{date_name} must be a string in YYYYMMDD format.")
            
        start_date_obj = datetime.strptime(start_date, "%Y%m%d")
        end_date_obj = datetime.strptime(end_date, "%Y%m%d")
        year_day_list = []
        current_date = start_date_obj
        while current_date <= end_date_obj:
            year = str(current_date.year)
            day_of_year = str(current_date.strftime("%j"))
            year_day_list.append((year, day_of_year))
            current_date += timedelta(days=1)
        logger.set_date_range(start_date, end_date)
        logger.info(f"===== Current MAIN WORKER PID is {os.getpid()}, Main process started at: {datetime.now()}  =====")
        logger.info(f"Processing data from {start_date} to {end_date} for VIIRS-SNPP")
        logger.info(f"Grid size: {grid_size}")
        logger.info(f"Data export path: {data_export_path}")
        logger.info(f"Longitude range: {min_lon} to {max_lon}")
        logger.info(f"Latitude range: {min_lat} to {max_lat}")
        logger.info(f"Number of processes used: {num_cores}")
        logger.info(f"Number of dates to process: {len(year_day_list)}")
        logger.info(f"Processing dates: {year_day_list}")
        logger.info(f"Start date: {start_date_obj.strftime('%Y-%m-%d')}")
        logger.info(f"End date: {end_date_obj.strftime('%Y-%m-%d')}")
        creds = get_earthdata_credentials()
        logger.info("Earthdata credentials retrieved successfully.")
        results = Parallel(n_jobs=num_cores)(
            delayed(process_data)(grid_size, year, day, data_export_path, min_lon, max_lon, min_lat, max_lat, creds)
            for year, day in year_day_list
        )
        processed_days = [yd for yd, res in zip(year_day_list, results) if res]
        failed_days = [yd for yd, res in zip(year_day_list, results) if not res]
        success_rate = sum(results) / len(results) if results else 0.0
        logger.info(f"Processed {len(year_day_list)} dates")
        logger.info(f"Success rate: {success_rate:.1%}")
        if processed_days:
            logger.info(f"Successfully processed days: {processed_days}")
        if failed_days:
            logger.warning(f"Failed to process days: {failed_days}")
        logger.info("Processing complete. Outputs saved in NetCDF files.")
        return {
            'success_rate': success_rate,
            'processed_days': processed_days,
            'failed_days': failed_days
        }
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    start_time = time.time()
    try:
        result = main()
        print(result)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Fatal error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logger.info(f"Total execution time (Hour:Minute:Second): {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
        print(f"Total execution time (Hour:Minute:Second): {int(hours):02}:{int(minutes):02}:{int(seconds):02}")