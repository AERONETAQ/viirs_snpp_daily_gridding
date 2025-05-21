import re
import os
import json
import s3fs
from typing import Tuple
import xarray as xr
import numpy as np
from tqdm import tqdm
from logs import logger
from S3_Authentication import get_earthdata_credentials

def process_files(
    files: list[str],
    satellite: str,
    data_type: str,
    min_value: float,
    max_value: float,
    creds: dict[str]
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Reads and filters data using NumPy instead of pandas.

    Returns:
        Tuple[np.ndarray, ...]: (aod, lat, lon, vza) after filtering.
    """
    aod, lat, lon, vza = read_data_from_files(files, data_type, satellite, creds)
    mask = (aod >= min_value) & (aod <= max_value)
    return aod[mask], lat[mask], lon[mask], vza[mask]


def read_data_from_files(file_paths: list[str], file_type: str, satellite: str, creds: dict[str]) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Reads and processes aerosol data from either AERDB or AERDT files.

    Args:
        file_paths (list[str]): List of file paths to read.
        file_type (str): Type of files to read, either "AERDB" or "AERDT".
        satellite (str): Either SNPP or NOAA20
        creds (dict[str]): Dictionary containing AWS credentials.
            - accessKeyId
            - secretAccessKey
            - sessionToken
    Returns:
        pd.DataFrame: A DataFrame containing the processed data from all files.
    """

    s3_fs = s3fs.S3FileSystem(
        key = creds['accessKeyId'],
        secret = creds['secretAccessKey'],
        token = creds['sessionToken'],
    )

    aod_list = []
    lat_list = []
    lon_list = []
    vza_list = []

    for file_path in tqdm(file_paths, desc=f"Processing {file_type} Files"):
        try:
            if file_type == 'AERDB':
                s3_path = f"s3://prod-lads/AERDB_L2_VIIRS_{satellite}/{file_path}"
                ds = xr.open_mfdataset(s3_fs.open(s3_path), decode_timedelta=True)
                aod = ds["Aerosol_Optical_Thickness_550_Land_Ocean_Best_Estimate"].values.ravel()
                lat = ds["Latitude"].values.ravel()
                lon = ds["Longitude"].values.ravel()
                vza = ds["Viewing_Zenith_Angle"].values.ravel()
            elif file_type == 'AERDT':
                s3_path = f"s3://prod-lads/AERDT_L2_VIIRS_{satellite}/{file_path}"
                geolocation = xr.open_dataset(s3_fs.open(s3_path), group="geolocation_data", decode_timedelta=True)
                geophysical = xr.open_dataset(s3_fs.open(s3_path), group="geophysical_data", decode_timedelta=True)
                aod = geophysical["Optical_Depth_Land_And_Ocean"].values.ravel()
                lat = geolocation["latitude"].values.ravel()
                lon = geolocation["longitude"].values.ravel()
                vza = geolocation["sensor_zenith_angle"].values.ravel()
            else:
                logger.error(f"Unsupported file type: {file_type}")
                continue

            mask = ~np.isnan(aod) & ~np.isnan(lat) & ~np.isnan(lon) #& ~np.isnan(vza)
            aod_list.append(aod[mask])
            lat_list.append(lat[mask])
            lon_list.append(lon[mask])
            vza_list.append(vza[mask])

        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")

    # Concatenate all arrays (more efficient than pandas concat)
    aod_all = np.concatenate(aod_list)
    lat_all = np.concatenate(lat_list)
    lon_all = np.concatenate(lon_list)
    vza_all = np.concatenate(vza_list)

    return aod_all, lat_all, lon_all, vza_all