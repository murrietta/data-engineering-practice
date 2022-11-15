import sys, os
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, count, date_trunc, trunc, weekofyear, mean, lit

data_folder = './data/'
output_folder = './reports/'

if not os.path.exists(output_folder):
    os.mkdir(output_folder)

def write_df_to_csv(odf, ofolder, ofoldername):
    outpath = f'{ofolder}{ofoldername}'
    odf.coalesce(1).write.csv(outpath, header='true')

def analyze_q1(sdf, verbose=False, foldername='q01_avg_duration_by_day', write_csv=True):
    # Q1, average trip duration by day (not clear if its DOW or calendar day so going with calendar day)
    if verbose: print('Average trip duration by day')
    outdf = sdf.withColumn("tripduration", sdf.tripduration.cast('double')) \
            .withColumn("start_time", sdf.start_time.cast("date")) \
            .groupBy('start_time').mean('tripduration')
    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def analyze_q2(sdf, verbose=False, foldername='q02_count_trips_by_day', write_csv=True):
    # Q2, average trip duration by day (not clear if its DOW or calendar day so going with calendar day)
    if verbose: print('Trip Counts by Day')
    outdf = sdf.withColumn('start_day', col('start_time').cast(DateType())) \
            .withColumn('trip_day', date_trunc("start_day", "dd")) \
            .groupBy('trip_day') \
            .agg(count('start_time').alias('trip_count'))
    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def analyze_q3(sdf, verbose=False, foldername='q03_top_start_station_by_month', write_csv=True):
    # Q3, most popular start station by month
    # plan:
    #   - group by month and start station, count
    #   - sort by month, count desc
    #   - apply row number for each month
    #   - select only where row number is 1
    if verbose: print('Top start station by month')
    outdf = sdf.withColumn('start_date', col('start_time').cast(DateType())) \
            .withColumn('trip_month', trunc("start_date", "MM")) \
            .groupBy('trip_month', 'from_station_name') \
            .agg(count('start_date').alias('trip_count'))
    windowSpec  = Window.partitionBy('trip_month').orderBy(col("trip_count").desc())
    outdf = outdf.withColumn("row_number",row_number().over(windowSpec)) \
            .where(col("row_number")==1)
    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def analyze_q4(sdf, verbose=False, foldername='q04_top_3_stations_last_2_weeks', write_csv=True):
    # What were the top 3 trip stations each day for the last two weeks?
    # plan
    #   - compute weekofyear for each end_time (since using destination this time)
    #   - extract the max week in the data, then filter df for last two weeks
    #   - group by trip station (destination), count, sort descending by count and select top 3
    #   - we are assuming all data here is in the same year (it is)
    #       - if not true then need to map each date to its week (maybe most recent Sunday)
    #       - order by that week date, descending, take the last two weeks only
    #       - OR, just add year column with the weekofyear column and sort by using both columns
    if verbose: print('Top 3 stations last 2 weeks')
    outdf = sdf.withColumn('end_date', col('end_time').cast(DateType())) \
            .withColumn('end_week', weekofyear('end_date'))
    max_week = outdf.agg({"end_week": "max"}).first()[0]
    outdf = outdf.where(col('end_week') > (max_week - 2)) \
            .groupBy('to_station_name') \
            .agg(count('to_station_name').alias('trip_count')) \
            .orderBy(col('trip_count').desc()) \
            .withColumn("row_number", row_number().over(Window.partitionBy().orderBy(col('trip_count').desc()))) \
            .where(col('row_number') <= 3)

    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def analyze_q5(sdf, verbose=False, foldername='q05_gender_avg_trip_dur', write_csv=True):
    # Do Males or Females take longer trips on average?
    # plan
    #   - group by gender, avg trip_duration
    #   - return whole table (2x2)
    if verbose: print('Average trip duration by gender')
    outdf = sdf.withColumn("tripduration", sdf.tripduration.cast('double')) \
            .groupBy('gender') \
            .mean('tripduration')

    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def analyze_q6(sdf, verbose=False, foldername='q06_top_10_ages_shortest_longest_trips', write_csv=True):
    # What is the top 10 ages of those that take the longest trips, and shortest?
    # plan - question isn't obvious to me
    #   - compute age column
    #   - group by age, average tripduration
    #   - order by trip duration (asc/desc) get top 10 for each
    #   - concatenate both dfs and output to csv with column indicating if it was long/short
    if verbose: print('Top 10 ages of longest/shortest trip takers')
    predf = sdf.withColumn('age', 2022 - col('birthyear')) \
            .withColumn("tripduration", sdf.tripduration.cast('double')) \
            .groupBy('age') \
            .agg(mean('tripduration').alias('avg_tripduration'))
    outdf = predf.orderBy(col('avg_tripduration').asc()) \
            .withColumn("row_number", row_number().over(Window.partitionBy().orderBy(col('avg_tripduration').asc()))) \
            .where(col('row_number') <= 10) \
            .withColumn('type', lit('shortest_trips'))
    outdf2 = predf.orderBy(col('avg_tripduration').desc()) \
            .withColumn("row_number", row_number().over(Window.partitionBy().orderBy(col('avg_tripduration').desc()))) \
            .where(col('row_number') <= 10) \
            .withColumn('type', lit('longest_trips'))
    outdf = outdf.union(outdf2)

    if verbose: print(outdf.show())
    if write_csv: write_df_to_csv(outdf, output_folder, foldername)

def main():
    spark = SparkSession.builder.appName('Exercise6') \
        .enableHiveSupport().getOrCreate()
    
    # find zipped files - assuming all contain csv
    print('Finding all zipped csv files')
    data_files = glob.glob(f'{data_folder}**/*.csv', recursive=True)

    # loop over csv files, add to spark dataframe
    dfs = {}
    for data_file in data_files:
        print(f'Trying to read file {data_file}')
        dfs[data_file] = spark.read.options(header="true", inferSchema="true").format('csv').load(data_file)
        print(f'Dataframe count: {dfs[data_file].count()}')

    # data_files[0] contains the trip-level data, data_files[1] contains ride-level data
    # continue analytics using data_files[0]
    df = dfs[data_files[0]]
    print(df.show(20))

    # Q1
    analyze_q1(df, verbose=True)
    
    # Q2
    analyze_q2(df, verbose=True)
    
    # Q3
    analyze_q3(df, verbose=True)
    
    # Q4
    analyze_q4(df, verbose=True)

    # Q5
    analyze_q5(df, verbose=True)

    # Q6
    analyze_q6(df, verbose=True)


if __name__ == '__main__':
    main()
