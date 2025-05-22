import time
import json
import os
from joblib import Parallel, delayed
from datetime import datetime, timedelta
from logs import logger
from S3_Authentication.authentication import get_earthdata_credentials
from process_data import process_data

def load_config(config_path: str) -> dict:
    """
    Load configuration from a JSON file.

    Args:
        config_path (str): Path to the configuration JSON file.

    Returns:
        dict: Configuration parameters.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        ValueError: If there is an error decoding the JSON file.
    """
    try:
        with open(config_path, "r") as file:
            config = json.load(file)
        return config
    except FileNotFoundError as e:
        logger.error(f"An error occurred: {e}")
        raise FileNotFoundError(f"Configuration file '{config_path}' not found. Please ensure it exists in the current directory.")
    except json.JSONDecodeError as e:
        logger.error(f"An error occurred: {e}")
        raise ValueError(f"Error decoding '{config_path}'. Ensure the file contains valid JSON.")


def main():
    os.environ['MAIN_PID'] = str(os.getpid())
    config = load_config("config.json")
    grid_size = config["grid_size"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    data_export_path = config["data_export_path"]
    min_lon = config["min_lon"]
    max_lon = config["max_lon"]
    min_lat = config["min_lat"]
    max_lat = config["max_lat"]
    num_cores = config["num_cores"]

    start_date_obj = datetime.strptime(start_date, "%Y%j")
    end_date_obj = datetime.strptime(end_date, "%Y%j")
    year_day_list = []
    current_date = start_date_obj

    # Generate a list of (year, day_of_year) tuples
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

    logger.info(f"Processed {len(year_day_list)} dates")
    logger.info(f"Success rate: {sum(results)/len(results):.1%}")
    logger.info("Processing complete. Outputs saved in NetCDF files.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, rem = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(rem, 60)
    logger.info(f"Total execution time (Hour:Minute:Second): {int(hours):02}:{int(minutes):02}:{int(seconds):02}")