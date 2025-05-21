import os
import requests
import base64
import earthaccess
import json
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from logs import logger

# Load environment variables
load_dotenv()

def get_earthdata_credentials():
    """
    Authenticate with NASA Earthdata and retrieve S3 credentials for accessing LAADS DAAC data.
    This function uses environment variables `EARTHDATA_USERNAME` and `EARTHDATA_PASSWORD` 
    for authentication. It logs in to Earthdata using the `earthaccess` library, retrieves 
    temporary AWS S3 credentials, and initializes an S3FileSystem object for accessing S3 resources.
    Returns:
        dict: A dictionary containing the S3 credentials (access key, secret key, and session token).
        
    Raises:
        ValueError: If the Earthdata username or password is not set in the environment variables.
        requests.RequestException: If there is a failure during the request to retrieve credentials.
    """

    username = os.getenv("EARTHDATA_USERNAME")
    password = os.getenv("EARTHDATA_PASSWORD")

    if not username or not password:
        logger.error("Earthdata username or password not set in environment variables.")
        raise ValueError("Missing Earthdata credentials")

    try:
        earthaccess.login()
        creds = earthaccess.get_s3_credentials(daac="LAADS")
        return creds

    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise