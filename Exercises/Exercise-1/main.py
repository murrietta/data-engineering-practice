import requests
import os
import zipfile
from io import BytesIO
import logging
logger = logging.getLogger(__name__)

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]

out_folder = 'downloads'


def main():
    # make downloads folder if not existing
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)
    
    # download each file to downloads
    for download_uri in download_uris:
        # make request
        try:
            logger.info('Downloading file')
            resp = requests.get(download_uri)
        except Exception as e:
            logger.error(f'Failed to download {download_uri}, skipping\n{e}')
            continue

        # unzip the content, then write to out_folder
        try:
            logger.info('Unzipping file')
            with zipfile.ZipFile(BytesIO(resp.content)) as z:
                z.extractall(out_folder)
        except Exception as e:
            logger.error(f'Failed to extract {download_uri}, skipping\n{e}')
            continue


if __name__ == '__main__':
    main()
