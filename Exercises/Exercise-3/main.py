import boto3
from io import BytesIO
import gzip
import os
import logging
log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
log_kwargs = {'level': logging.INFO, 'filemode': 'w',
                'format': log_format, 'force': True}
logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)


def main():
    # setup s3 resource and bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('commoncrawl')

    # download the file
    buf = BytesIO()
    try:
        logger.info(f'Downloading file from s3 commoncrawl')
        bucket.download_fileobj('crawl-data/CC-MAIN-2022-05/wet.paths.gz', buf)
        buf.seek(0)
    except Exception as e:
        logger.error(f'Failed to download, exiting\n{e}')
        return 1

    # open in memory
    with gzip.open(buf, 'rb') as f:
        txt = f.read().decode('utf-8')
    
    # get first uri and download that file
    uri = txt.split('\n', maxsplit=1)[0]
    prefix, fname = os.path.split(uri)
    buf = BytesIO()
    try:
        logger.info(f'Downloading file from s3 commoncrawl {fname}')
        bucket.download_fileobj('crawl-data/CC-MAIN-2022-05/wet.paths.gz', buf)
        buf.seek(0)
    except Exception as e:
        logger.error(f'Failed to download, exiting\n{e}')
        return 1

    # open in memory
    with gzip.open(buf, 'rb') as f:
        txt2 = f.read().decode('utf-8')

    # print first 100 lines of txt2 to stdout
    txt2 = txt2.split('\n', maxsplit=1000)[:1000]
    for line in txt2:
        print(line)
    
    return 0


if __name__ == '__main__':
    main()
