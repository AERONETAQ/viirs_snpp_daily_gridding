import time
import os
import sys
from joblib import Parallel, delayed
from datetime import datetime, timedelta
from viirs_snpp_daily_gridding import get_earthdata_credentials, process_data, logger


def run_gridding(
    grid_size: float,
    start_date: str,
    end_date: str,
    data_export_path: str,
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
    num_cores: int,
    earthdata_username: str,
    earthdata_password: str
) -> dict:
    """
    Run the VIIRS-SNPP daily gridding process over a specified date and spatial range.

    Args:
        grid_size (float): The size of the grid cell in degrees.
        start_date (str): Start date in 'YYYYMMDD' format.
        end_date (str): End date in 'YYYYMMDD' format.
        data_export_path (str): Path to export the output data.
        min_lon (float): Minimum longitude of the grid.
        max_lon (float): Maximum longitude of the grid.
        min_lat (float): Minimum latitude of the grid.
        max_lat (float): Maximum latitude of the grid.
        num_cores (int): Number of CPU cores to use for parallel processing.
        earthdata_username (str): NASA Earthdata username for authentication.
        earthdata_password (str): NASA Earthdata password for authentication.

    Returns:
        dict: A dictionary containing:
            - 'success_rate' (float): Fraction of successful days processed.
            - 'processed_days' (list): List of (year, day) tuples successfully processed.
            - 'failed_days' (list): List of (year, day) tuples that failed.
            - 'total_execution_time' (str): Elapsed time in HH:MM:SS format.

    Raises:
        ValueError: If any input parameter is invalid.
        Exception: For any other fatal error during processing.
    """
    start_time = time.time()
    try:
        os.environ['MAIN_PID'] = str(os.getpid())
        
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

        # Generate a list of (year, day) tuples for the date range
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
        creds = get_earthdata_credentials(earthdata_username, earthdata_password)
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

        # Calculate elapsed time
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        elapsed_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        return {
            'success_rate': success_rate,
            'processed_days': processed_days,
            'failed_days': failed_days,
            'total_execution_time': elapsed_str
        }
    except Exception as e:
        logger.error(f"Fatal error in run_gridding: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run VIIRS SNPP daily gridding.")
    parser.add_argument('--grid_size', type=float, default=0.1, help='Grid cell size in degrees')
    parser.add_argument('--start_date', type=str, default="20240101", help='Start date (YYYYMMDD)')
    parser.add_argument('--end_date', type=str, default="20240105", help='End date (YYYYMMDD)')
    parser.add_argument('--data_export_path', type=str, default="./output", help='Output directory')
    parser.add_argument('--min_lon', type=float, default=-180, help='Minimum longitude')
    parser.add_argument('--max_lon', type=float, default=180, help='Maximum longitude')
    parser.add_argument('--min_lat', type=float, default=-90, help='Minimum latitude')
    parser.add_argument('--max_lat', type=float, default=90, help='Maximum latitude')
    parser.add_argument('--num_cores', type=int, default=4, help='Number of CPU cores')
    parser.add_argument('--earthdata_username', type=str, default="your_username", help='Earthdata username')
    parser.add_argument('--earthdata_password', type=str, default="your_password", help='Earthdata password')
    args = parser.parse_args()

    start_time = time.time()
    try:
        result = run_gridding(
            grid_size=args.grid_size,
            start_date=args.start_date,
            end_date=args.end_date,
            data_export_path=args.data_export_path,
            min_lon=args.min_lon,
            max_lon=args.max_lon,
            min_lat=args.min_lat,
            max_lat=args.max_lat,
            num_cores=args.num_cores,
            earthdata_username=args.earthdata_username,
            earthdata_password=args.earthdata_password
        )
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
