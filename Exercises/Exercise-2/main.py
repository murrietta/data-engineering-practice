import requests
import pandas
from io import BytesIO
import logging
log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
log_kwargs = {'level': logging.INFO, 'filemode': 'w',
                'format': log_format, 'force': True}
logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)

url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
last_mod_date = '2022-02-07 14:03'

def main():
    # make request
    resp = requests.get(url)

    # analyzing response content shows it is a simple table in an html document
    # can find whole table like so
    table = resp.content.decode('utf-8')
    for tag, index in [['<table>', -1], ['</table>', 0]]:
        try:
            table = table.split(tag)[index]
        except Exception as e:
            logger.error(f'Failed to find any {tag} tag\n{e}')
    
    # we'll split the individual rows in this case
    table = [x.replace('</tr>', '') for x in table.split('<tr>')[1:]]

    # row 0 is header, shows there's only one datetime column (LastModified)
    try:
        # there may be multiple, assuming the first is sufficient
        row = [x for x in table if last_mod_date in x][0]
    except Exception as e:
        # if not found then exit and return 1
        logger.error(f'LastModifiedDate {last_mod_date} not found\n{e}')
        return 1

    # extract href to build URL
    try:
        href = [x for x in row.split('<td')[1].split('>') if 'href' in x][0]
        href = href.split('=')[-1].replace('"', '')
    except Exception as e:
        # if not found then exit and return 1
        logger.error(f'href not found in given row\n{e}')
        return 1

    file_url = f'{url}{href}'

    # try to download the file
    try:
        resp = requests.get(file_url)
    except Exception as e:
        logger.error(f'Failed to download file from {file_url}\n{e}')
        return 1
    
    # make dataframe
    try:
        logger.info('Acquiring data frame')
        df = pandas.read_csv(BytesIO(resp.content))
    except Exception as e:
        logger.error(f'Failed to acquire data frame from content {href}, skipping\n{e}')
    
    # print the top 5 records with highest 'HourlyDryBulbTemperature'
    # limit to first 15 columns
    df = df.sort_values('HourlyDryBulbTemperature', ascending=False).reset_index(drop=True)
    for i, row in df[:5].iterrows():
        print(row[:15])

if __name__ == '__main__':
    main()
