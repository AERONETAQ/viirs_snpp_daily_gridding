import os
import requests
import base64
import earthaccess
import json
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from viirs_snpp_daily_gridding.logs import logger

# Load environment variables
load_dotenv()

def get_earthdata_credentials(username, password):
    """
    Authenticate with NASA Earthdata and retrieve S3 credentials for accessing LAADS DAAC data.
    Args:
        username (str): Earthdata username
        password (str): Earthdata password
    Returns:
        dict: A dictionary containing the S3 credentials (access key, secret key, and session token).
    Raises:
        ValueError: If the Earthdata username or password is not provided.
        requests.RequestException: If there is a failure during the request to retrieve credentials.
    """
    if not username or not password:
        logger.error("Earthdata username or password not provided.")
        raise ValueError("Missing Earthdata credentials")
    try:
        earthaccess.login(strategy="netrc", username=username, password=password)
        creds = earthaccess.get_s3_credentials(daac="LAADS")
        return creds
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise