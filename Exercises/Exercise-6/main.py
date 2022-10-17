from pyspark.sql import SparkSession
# https://stackoverflow.com/a/44567597/5816608
import sys
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
import zipfile
import glob
import logging
# log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
# log_kwargs = {'level': logging.INFO, 'filemode': 'w',
#                 'format': log_format, 'force': True}
# logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)

data_folder = './data/'



def main():
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()
    
    # find zipped files - assuming all contain csv
    print('Finding all zipped csv files')
    data_files = glob.glob(f'{data_folder}**/*.zip', recursive=True)
    print('Trying to read files using spark')
    data_file = data_files[0]
    with zipfile.ZipFile(data_file, 'r') as z:
        for name in z.namelist():
            if ((name.endswith('.csv')) & ('MACOSX' not in name)):
                data = z.read(name).decode('utf-8')
                print(data[:100])
                try:
                    df = (spark.read.csv(data, header=True))
                    print('Success')
                    print(df.show())
                except Exception as e:
                    print('Failed to read from zip')



if __name__ == '__main__':
    main()
