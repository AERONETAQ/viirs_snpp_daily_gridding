# viirs_snpp_daily_gridding

![VIIRS SNPP Daily Gridding](images/image.png)

## Overview

`viirs_snpp_daily_gridding` is a modern, production-ready Python package for processing and gridding daily VIIRS SNPP satellite data. It automates the workflow from raw data ingestion to spatially gridded NetCDF outputs, supporting atmospheric and environmental research. The package is designed for both programmatic (library) and command-line (CLI) use, with robust logging, multiprocessing, and clear error handling.

## Features

- Batch processing of daily VIIRS SNPP Level 2 data directly from S3 (no download necessary)
- Flexible spatial gridding (user-defined resolution and extent)
- Output in NetCDF format
- Multiprocessing for fast batch processing
- Modern logging (console, multiprocessing-aware)
- Clean, parameter-driven API 
- Usable as both a Python library and CLI tool

## Installation

Clone the repository and install dependencies (recommended: use a virtual environment):

### Using venv (standard Python virtual environment)

```bash
python -m venv venv
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
# source venv/bin/activate
pip install .
```

### Using conda

```bash
conda create -n viirs_env python=3.9
conda activate viirs_env
pip install .
```

## Usage

### As a Python Library

Import and call the main function directly. All parameters must be provided as arguments (no config file needed):

```python
from viirs_snpp_daily_gridding import run_gridding

result = run_gridding(
    grid_size=0.1,
    start_date="20240101",
    end_date="20240105",
    data_export_path="./output",
    min_lon=-180,
    max_lon=180,
    min_lat=-90,
    max_lat=90,
    num_cores=4,
    earthdata_username="your_username",
    earthdata_password="your_password"
)
print(result)
```

### As a Command-Line Tool (CLI)

You can now run the package as a CLI and pass parameters directly via command-line arguments. All arguments have sensible defaults, so you can run with no arguments for a sample run, or override any parameter as needed.

Example (using all defaults):

```bash
python -m viirs_snpp_daily_gridding.run_gridding
```

Example (with all parameters):

```bash
python -m viirs_snpp_daily_gridding.run_gridding \
    --grid_size 0.2 \
    --start_date 20240110 \
    --end_date 20240112 \
    --data_export_path ./output \
    --min_lon -100 \
    --max_lon 100 \
    --min_lat -50 \
    --max_lat 50 \
    --num_cores 2 \
    --earthdata_username your_username \
    --earthdata_password your_password
```

**Note:**
- All arguments are optional and have defaults, but you must provide valid Earthdata credentials for real data access.
- You can still edit the script directly if you prefer.

## Credentials

Earthdata credentials are required for data access. Pass them as arguments to the relevant functions, or set them as environment variables (see `.env` for an example). Do **not** hardcode credentials in your scripts.

## Logging

All logs are output to the console and are multiprocessing-aware. No log files are written by default.

## Project Structure

```
viirs_snpp_daily_gridding/
    __init__.py
    run_gridding.py
    export_data/
    logs/
    process_data/
    s3_authentication/
    web_scraping/
```

## Contributing

Contributions are welcome! Please open issues or pull requests for improvements or bug fixes.