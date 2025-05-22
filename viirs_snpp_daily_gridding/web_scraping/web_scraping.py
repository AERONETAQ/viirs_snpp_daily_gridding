import os
import requests
from bs4 import BeautifulSoup

def get_file_list_dynamically(year: int, day: int, product: str) -> list[str]:
    '''
    Given a productname, year and a day, fetches list of files for the product to be used in the s3 bucket url

    Params:
    year-> the year for which the data to fetch
    day -> the day for which the data to fetch
    product -> the product type from LAADS DAAC webpage

    Returns:
    file_list -> list of files for the corresponding product for the given date
    '''
    file_list = []

    URL = f'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5200/{product}/{year}/{day}/'
    result = requests.get(URL)

    soup = BeautifulSoup(result.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href")
        if href and href.endswith(".nc"): 
            file_list.append(os.path.basename(href))

    return list(set(file_list))