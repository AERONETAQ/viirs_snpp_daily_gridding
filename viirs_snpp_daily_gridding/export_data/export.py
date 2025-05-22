import os
import numpy as np
import xarray as xr
import dask.array as da
from logs import logger
from datetime import datetime, timezone

def get_file_export_name(file_date: int, current_time: datetime) -> str:
    """
    Generates a filename for exporting NetCDF files with a specific format.
    Args:
        file_date (int): The date of the file in YYYYMMDD format.
        current_time: current_datetime object
    Returns:
        str: The generated filename in the format 
             'AER_DB_DT_D3_G10_VIIRS_.YYYYMMDD.V0.ProcessingDateTime.nc', 
             where YYYYMMDD is the provided file_date and ProcessingDate is the current date.
    """
    datetime_ = current_time.strftime("%Y%m%d%H%M%S")
    processing_date = datetime_[:4] + f"{datetime.strptime(datetime_[:8], '%Y%m%d').timetuple().tm_yday:03}" + datetime_[8:]
    return f"AER_DBDT_D10KM_L3_VIIRS_SNPP.{file_date}.V001.{processing_date}.nc"


def export_netcdf(
    grdlon: np.ndarray,
    grdlat: np.ndarray,
    dbdt_tau: np.ndarray,
    dtdb_tau: np.ndarray,
    avg_tau: np.ndarray,
    avgtau_db: np.ndarray,
    count_db: np.ndarray,
    stdtau_db: np.ndarray,
    avgtau_dt: np.ndarray,
    count_dt: np.ndarray,
    stdtau_dt: np.ndarray,
    vza_dt: np.ndarray,
    date: str,
    deep_blue_files: list[str],
    dark_target_files: list[str],
    export_path : str
) -> None:
    """
    Export gridded AOD data to a NetCDF file with compression.
    This function takes gridded AOD, processes them to replace invalid values with NaNs, converts them to float32, and saves them in a NetCDF file.
    The NetCDF file includes metadata for each variable and coordinate.
    Parameters:
    -----------
    grdlon : np.ndarray
        2D array of longitude values for the grid.
    grdlat : np.ndarray
        2D array of latitude values for the grid.
    dbdt_tau : np.ndarray
        2D array of AOD values derived using DB as the preferred source, falling back to DT if DB is invalid.
    dtdb_tau : np.ndarray
        2D array of AOD values derived using DT as the preferred source, falling back to DB if DT is invalid.
    avg_tau : np.ndarray
        2D array of combined average AOD values from DT and DB sources.
    avgtau_db : np.ndarray
        2D array of mean AOD values from the Deep Blue (DB) for the grids.
    count_db : np.ndarray
        2D array of the number of samples for DB AOD values for the grids.
    stdtau_db : np.ndarray
        2D array of standard deviation of DB AOD values.
    avgtau_dt : np.ndarray
        2D array of mean AOD values from the Dark Target (DT) algorithm.
    count_dt : np.ndarray
        2D array of the number of samples for DT AOD values.
    stdtau_dt : np.ndarray
        2D array of standard deviation of DT AOD values.
    vza_dt : np.ndarray
        2D array of sensor zenith angle values, mean per grid.
    date : str
        Start Date of the file in YYYYMMDD format.
    deep_blue_files : list[str]
        List of Deep Blue file paths used to generate the data.
    dark_target_files : list[str]
        List of Dark Target file paths used to generate the data.
    export_path: str
        Path to the directory where the NetCDF file will be saved.
    
    Returns:
        None

    Raises:
        Exception: If there is an error during the export process.
    """
    try:
        # adding a new axis to the arrays for time dimension
        count_dt = count_dt[np.newaxis, :, :]
        avgtau_dt = avgtau_dt[np.newaxis, :, :]
        stdtau_dt = stdtau_dt[np.newaxis, :, :]
        count_db = count_db[np.newaxis, :, :]
        avgtau_db = avgtau_db[np.newaxis, :, :]
        stdtau_db = stdtau_db[np.newaxis, :, :]
        dtdb_tau = dtdb_tau[np.newaxis, :, :]
        dbdt_tau = dbdt_tau[np.newaxis, :, :]
        avg_tau = avg_tau[np.newaxis, :, :]
        vza_dt = vza_dt[np.newaxis, :, :]

        # extracting latitude and longitude 1d arrays from meshgrids
        lon_1d = np.round(grdlon[:, 0], 7)
        lat_1d = np.round(grdlat[0, :], 7)

        current_time = datetime.now(timezone.utc)
        iso_timestamp_full = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        iso_timestamp_short = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")  
        product_name = get_file_export_name(date, current_time) # this will be the name of the file to be exported
        
        date_object = datetime.strptime(date, "%Y%j")
        date_for_processing_day = date_object.strftime("%Y-%m-%d") # converting 2022205 to 2022-07-24
        
        # value for time dimension (days since 1990-01-01)
        days_since = np.array([(date_object - datetime(1990, 1, 1)).days], dtype=np.int32)

        ds = xr.Dataset(
            {
                "DT_Number_Of_Pixels": (["Time", "Longitude", "Latitude"], count_dt),
                "DT_AOD_550_AVG": (["Time", "Longitude", "Latitude"], avgtau_dt),
                "DT_AOD_550_STD": (["Time", "Longitude", "Latitude"], stdtau_dt),
                "DB_Number_Of_Pixels": (["Time", "Longitude", "Latitude"], count_db),
                "DB_AOD_550_AVG": (["Time", "Longitude", "Latitude"], avgtau_db),
                "DB_AOD_550_STD": (["Time", "Longitude", "Latitude"], stdtau_db),
                "DT_DB_AOD_550_AVG": (["Time", "Longitude", "Latitude"], dtdb_tau),
                "DB_DT_AOD_550_AVG": (["Time", "Longitude", "Latitude"], dbdt_tau),
                "COMBINE_AOD_550_AVG": (["Time", "Longitude", "Latitude"], avg_tau),
                "Sensor_Zenith_Angle": (["Time", "Longitude", "Latitude"], vza_dt),
            },
            coords={
                "Time": days_since,  # Shape (1,)
                "Longitude": lon_1d,  # Shape (3600,)
                "Latitude": lat_1d,   # Shape (1800,)
            },
        )

        ds["Longitude"].attrs = {
            "valid_range": [-179.95, 179.95],
            "standard_name": "longitude",
            "long_name": "Geodetic Longitude",
            "units": "degree_east",
            "_CoordinateAxisType": "Lon",
        }

        ds["Latitude"].attrs = {
            "valid_range": [-89.95, 89.95],
            "standard_name": "latitude",
            "long_name": "Geodetic Latitude",
            "units": "degree_north",
            "_CoordinateAxisType": "Lat",
        }


        ds["Time"].attrs = {
            "long_name": "time",
            "standard_name": "time",
            "units": "days since 1990-01-01 00:00:00",
        }

        variable_metadata = {
            "DB_AOD_550_AVG": {
                "valid_range": [-0.05, 5],
                "_FillValue": -999.0,
                "long_name": "Deep Blue/SOAR Aerosol Optical Depth (AOD) at 550 nm over land and ocean, QA-filtered, mean for the grid",
                "coordinates": "Time Longitude Latitude"
            },

            "DB_Number_Of_Pixels": {
                "_FillValue": -999,
                "long_name": "Number of samples for Deep Blue/SOAR Aerosol Optical Depth (AOD) at 550 nm over land and ocean, QA-filtered, for the grids",
                "coordinates": "Time Longitude Latitude"
            },

            "DB_AOD_550_STD": {
                "_FillValue": -999.0,
                "long_name": "Deep Blue/SOAR Aerosol Optical Depth (AOD) at 550 nm over land and ocean, QA-filtered, standard deviation for the grid",
                "coordinates": "Time Longitude Latitude"
            },

            "DT_AOD_550_AVG": {
                "valid_range": [-0.05, 5],
                "_FillValue": -999.0,
                "long_name": "Aerosol Optical Depth (AOD) at 0.55 micron for both ocean (Average) (Quality flag = 1, 2, 3) and land (corrected) (Quality flag = 3), mean for the grid",
                "coordinates": "Time Longitude Latitude"
            },

            "DT_Number_Of_Pixels": {
                "_FillValue": -999,
                "long_name": "Number of samples for Aerosol Optical Depth (AOD) at 0.55 micron for both ocean (Average) (Quality flag = 1, 2, 3) and land (corrected) (Quality flag = 3), for the grid",
                "coordinates": "Time Longitude Latitude"
            },

            "DT_AOD_550_STD": {
                "_FillValue": -999.0,
                "long_name": "Aerosol Optical Depth (AOD) at 0.55 micron for both ocean (Average) (Quality flag = 1, 2, 3) and land (corrected) (Quality flag = 3), standard deviation for the grid",
                "coordinates": "Time Longitude Latitude"
            },

            "DT_DB_AOD_550_AVG": {
                "valid_range": [-0.05, 5],
                "_FillValue": -999.0,
                "long_name": "Aerosol Optical Depth (AOD) at 550 nm, derived using DT as preferred source, falling back to DB if DT is invalid. Values are averaged over the grid.",
                "coordinates": "Time Longitude Latitude"
            },

            "DB_DT_AOD_550_AVG": {
                "valid_range": [-0.05, 5],
                "_FillValue": -999.0,
                "long_name": "Aerosol Optical Depth (AOD) at 550 nm, derived using DB as preferred source, falling back to DT if DB is invalid. Values are averaged over the grid.",
                "coordinates": "Time Longitude Latitude"
            },

            "COMBINE_AOD_550_AVG": {
                "valid_range": [-0.05, 5],
                "_FillValue": -999.0,
                "long_name": "Aerosol Optical Depth (AOD) at 550 nm, combined average from DT and DB sources. Values are averaged over the grid, using valid values from both DT and DB.",
                "coordinates": "Time Longitude Latitude"
            },

            "Sensor_Zenith_Angle": {
                "valid_range": [0., 90.],
                "_FillValue": -999.0,
                "long_name": "SNPP VIIRS Sensor Viewing Angle, mean for the grid",
                "units": "degree",
                "coordinates": "Time Longitude Latitude"
            }
        }

        for var, attrs in variable_metadata.items():
            ds[var].attrs = attrs
        
        ds.attrs = {
            "description": "Suomi National Polar-Orbiting Partnership (SNPP) Visible Infrared Imaging Radiometer Suite (VIIRS) Deep Blue (DB) & Dark Target (DT) combined Level 3 daily aerosol data, 0.1x0.1 degree grid",
            "comment": "Data are the arithmetic mean of all SNPP VIIRS Deep Blue/SOAR & Dark Target Level 2 data located in each grid element after filtering by confidence flag. The averaging is performed using different combinations and conditions on DT and DB products and derived multiple parameters.",
            "references": "https://doi.org/10.3390/rs12172847",
            "institution": "Biospheric Sciences Laboratory, NASA Goddard Space Flight Center",
            "LongName": "SNPP VIIRS High Resolution Level 3 daily aerosol data, 0.1x0.1 degree grid",
            "ProductionDateTime": iso_timestamp_full,
            'NorthernmostLatitude' : 89.95,
            'WesternmostLongitude' : -179.95,  
            'SouthernmostLatitude' : -89.95,  
            'EasternmostLongitude' : 179.95, 
            "latitude_resolution" : 0.1,
            "longitude_resolution" : 0.1,
            "related_url" : "https://deepblue.gsfc.nasa.gov & https://darktarget.gsfc.nasa.gov/",
            "keywords" : "aerosol optical depth, thickness, land, ocean, high resolution, gridded, viirs",
            "data_set_language" : "en",
            "Format" : "NetCDF4",
            "ProcessingLevel" : "Level 3",
            "keywords_vocabulary" : "NASA Global Change Master Directory (GCMD) Science Keywords",
            "license" : "http://science.nasa.gov/earth-science/earth-science-data/data-information-policy/",
            "stdname_vocabulary" : "NetCDF Climate and Forecast (CF) Metadata Convention",
            "NCO" : "netCDF Operators version 4.7.9 (Homepage = http://nco.sf.net, Code = http://github.com/nco/nco)",
            "VersionID" : "001",
            "pge_version" : "001",
            "title" : "SNPP VIIRS High Resolution Level 3 daily aerosol data, 0.1x0.1 degree grid",
            "DayNightFlag" : "Day",
            "GranuleID" :  product_name,
            "platform" : "Suomi-NPP",
            "instrument" : "VIIRS",
            "Conventions" : "CF-1.7, ACDD-1.3",
            "history" : "",
            "RangeBeginningDate" : date_for_processing_day,
            "RangeBeginningTime" : "00:00:00.000000",
            "RangeEndingDate" : date_for_processing_day,
            "RangeEndingTime" : "23:59:59.000000",
            "source" : "AERDB_L2 2.0.2,AERDT_L2 2.0.2",
            "date_created" :  iso_timestamp_short,
            "product_name" : product_name,
            "ShortName" : "AER_DBDT_D10KM_L3_VIIRS_SNPP",
            "product_version" : "1.0",
            "AlgorithmType" : "OPS",
            "IdentifierProductDOI" : "10.5067/VIIRS/AER_DBDT_D10KM_L3_VIIRS_SNPP.001",
            "IdentifierProductDOIAuthority" : "https://www.doi.org/",
            "input_files": ", ".join(deep_blue_files + dark_target_files),
            "ancillary_files" : "",
            "DataCenterId" : "GES-DISC",
            "project" : "NASA Terra, Aqua and SNPP ROSES 2016",
            "creator_name" : "Pawan Gupta",
            "creator_url" : "https://science.gsfc.nasa.gov/sci/bio/pawan.gupta",
            "creator_email" : "pawan.gupta@nasa.gov",
            "creator_institution" : "Biospheric Sciences Laboratory, NASA Goddard Space Flight Center",
            "publisher_institution" : "NASA Goddard Earth Sciences (GES) Data and Information Services Center (DISC)",
            "DataSetQuality": "The gridded data are validated against AERONET measurements and found comparable in quality as level 2 original dataset"
        }

        chunk_sizes = {
            "Longitude": 100,
            "Latitude": 100
        }

        # Remove fill values for lat and lon, which is set as NAN by default
        ds["Latitude"].encoding.update({"_FillValue": None})
        ds["Longitude"].encoding.update({"_FillValue": None})

        # Convert variables to dask arrays with explicit dimensions
        for var in ds.data_vars:
            ds[var].data = da.from_array(ds[var].data, chunks=chunk_sizes)

        # Apply compression
        ### YOU CAN INCREASE THE COMPRESSION LEVEL HERE UP TO 9; BUT, the size reduction beyond 6 is not significant
        ### and it will take more time to compress the file
        ### We can explore other compression techniques as well to see if we can get better compression
        compression = dict(zlib=True, complevel=6, shuffle=True)
        encoding = {var: compression for var in ds.data_vars}
        ds.to_netcdf(os.path.join(export_path, product_name), encoding=encoding, compute=True)

    except Exception as e:
        logger.error(f"Failed to export NetCDF file: {e}")