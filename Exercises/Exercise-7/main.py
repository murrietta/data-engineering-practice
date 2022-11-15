from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import glob
import logging
logger = logging.getLogger(__name__)

data_folder = './data/'
output_folder = './reports/'

def main():
    spark = SparkSession.builder.appName('Exercise7') \
        .enableHiveSupport().getOrCreate()
    # your code here
    
    # find zipped files - assuming all contain csv
    print('Finding all zipped csv files')
    data_files = glob.glob(f'{data_folder}**/*.zip', recursive=True)
    print(f'Found {len(data_files)} files')
    for data_file in data_files[:1]:
        print(f'Trying to read file {data_file}')
        df = spark.read.options(header="true", inferSchema="true").format('csv').load(data_file)
        df.show(5)

if __name__ == '__main__':
    main()
