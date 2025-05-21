import os
import time
import numpy as np
from datetime import datetime
from logs import logger 
from process_data import grid, db_dt_processing, process_files, plot_map
from export_data import export_netcdf
from web_scraping import get_file_list_dynamically

def process_data(
    grid_size: float,
    year: str,
    day: str,
    satellite: str,
    data_export_path: str,
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
    creds: dict,
) -> None:
    """
    Processes satellite data in parallel by gridding, combining, and exporting results.

    Args:
        grid_size (float): The size of the grid cells for gridding the data.
        year (str): The year of the data to process (e.g., "2023").
        day (str): The day of the year of the data to process (e.g., "001" for January 1st).
        satellite (str): The satellite name (e.g., "NOAA20").
        data_export_path (str): The path where the processed data will be exported.
        min_lon (float): The minimum longitude for the region of interest.
        max_lon (float): The maximum longitude for the region of interest.
        min_lat (float): The minimum latitude for the region of interest.
        max_lat (float): The maximum latitude for the region of interest.
        creds (dict): The AWS credentials for accessing the data.

    Returns:
        None
    """
    try:
        start_time = time.time()
        current_processing_date = datetime.strptime(year+day, "%Y%j")
        logger.set_date(current_processing_date) # setting date for logging files for each process
        logger.info(f"===== Current CHILD WORKER PID is {os.getpid()}, processing data for day {year+day} or {current_processing_date}, processing date is {datetime.now()} =====")

        files_AERDB = get_file_list_dynamically(year, day, f"AERDB_L2_VIIRS_{satellite}")
        files_AERDT = get_file_list_dynamically(year, day, f"AERDT_L2_VIIRS_{satellite}")

        if not files_AERDB:
            raise FileNotFoundError("No valid AERDB files found for the specified date range. Please verify the folder path and date range in the configuration.")
        if not files_AERDT:
            raise FileNotFoundError("No valid AERDT files found for the specified date range.")

        # processing Aerosol-DeepBlue data
        aod_db, lat_db, lon_db, vza_db = process_files(files_AERDB, satellite, "AERDB", -0.05, 5.0, creds)

        # Grid the AERDB data
        avgtau_db, stdtau_db, grdlat, grdlon, _, _, count_db, _ = grid(
                [min_lat, max_lat, min_lon, max_lon],
                grid_size,
                aod_db,  
                lat_db,
                lon_db,
                vza_db
        )

        del aod_db, lat_db, lon_db, vza_db  # Free up memory
                
        logger.info(f"Gridding AERDB data for {year+day} completed.")

        # processing Aerosol-DarkTarget data
        aod_dt, lat_dt, lon_dt, vza_dt = process_files(files_AERDT, satellite, "AERDT", -0.05, 5.0, creds)

        # Grid the AERDT data
        avgtau_dt, stdtau_dt, _, _, _, _, count_dt, sensorZenithAngle_dt = grid(
            [min_lat, max_lat, min_lon, max_lon],
            grid_size,
            aod_dt,
            lat_dt,
            lon_dt,
            vza_dt
        )

        del aod_dt, lat_dt, lon_dt, vza_dt 
        
        logger.info(f"Gridding AERDT data for {year+day} completed.")

        # Combine Deep Blue and Dark Target data
        dbdt_tau, dtdb_tau, avg_tau = db_dt_processing(avgtau_db, avgtau_dt)
        
        logger.info(f"Combining AERDB and AERDT data for {year+day} completed.")

        # Export results to NetCDF
        export_netcdf(
            grdlon,
            grdlat,
            dbdt_tau,
            dtdb_tau,
            avg_tau,
            avgtau_db,
            count_db,
            stdtau_db,
            avgtau_dt,
            count_dt,
            stdtau_dt,
            sensorZenithAngle_dt,
            year+day,
            files_AERDB,
            files_AERDT,
            data_export_path
        )

        logger.info(f"Processing complete. Outputs saved NetCDF file in: {data_export_path}.")
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, rem = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(rem, 60)
        logger.info(f"Total execution time (Hour:Minute:Second): {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
        return True
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False