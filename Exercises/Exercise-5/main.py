import psycopg2
import csv
import json
import glob
import logging
log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
log_kwargs = {'level': logging.INFO, 'filemode': 'w',
                'format': log_format, 'force': True}
logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)

data_folder = './data/'
schema_folder = './schema/'

def make_table(table_config, conn):
    """
    table creation function utilizing input from config files
    :param table_config: dict, contains all pertinent table information
    """
    table_name = table_config.get('table_name', None)
    columns = table_config.get('columns', None)
    if ((not table_name) | (not columns)):
        raise(Exception('Table name and columns must be specified!'))

    table_constraints = table_config.get('table_constraints', '')
    if table_constraints:
        table_constraints = f'\n,{table_constraints}'

    # parse columns into text
    columns_text = [f"\t{x.get('name')} {x.get('type')} {x.get('constraints')}" for x in columns]
    columns_text = ',\n'.join(columns_text)

    # create DDL
    sql = f'''DROP TABLE IF EXISTS {table_name} CASCADE;
        CREATE TABLE {table_name}(
        {columns_text}{table_constraints}
        );'''

    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except Exception as e:
            logger.error(f'Error trying to execute')
            raise(e)

def main():
    logger.info(f'Connecting to postgres instance')
    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    except Exception as e:
        logger.error('Failed to connect')
        exit(1)

    logger.info('Schema Creation - Finding schema and building tables')
    schema_files = glob.glob(f'{schema_folder}**/*.json', recursive=True)
    for schema_file in schema_files:
        logger.info(f'Schema Creation - Loading {schema_file}...')
        with open(schema_file, 'r') as f:
            schema = json.loads(f.read())
        
        logger.info('Schema Creation - Attempting table build')
        make_table(schema, conn)
    
    # limitation here will be if we have very large file for some reason
    # can try io tools to chunk it
    logger.info(f'Data Ingestion - Finding CSV file names')
    data_files = glob.glob(f'{data_folder}**/*.csv', recursive=True)
    for data_file in data_files:
        logger.info(f'Data Ingestion - Loading {data_file}')
        with open(data_file, newline='') as f:
            rows = list(csv.reader(f))
        
        # strip any leading/trailing whitespaces
        rows = [[y.strip() for y in x] for x in rows]

        # compose insert statement
        # assuming file name corresponds to table and column names are equal to file
        columns = rows[0]  # colum
        rows = rows[1:]  # omit header row
        
        table = data_file.replace(data_folder, '').replace('.csv', '')
        col_string = ', '.join(columns)
        s_string = ', '.join(['%s' for x in rows[0]])  # string for insert
        sql = f'INSERT INTO {table}({col_string}) VALUES({s_string});'
        logger.info(f'Inserting into {table}')
        logger.debug(f'sql:\n{sql}')
        logger.debug(f'values: {rows}')
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
                conn.commit()
        except Exception as e:
            logger.error('Error encountered during insert')
            raise(e)

        # select from the table to see what was inserted
        logger.debug('Selecting from table to confirm successful write...')
        with conn.cursor() as cur:
            sql = f'select * from {table};'
            cur.execute(sql)
            ret = cur.fetchall()
            logger.debug(f'{sql}\nOutput:\n')
            logger.debug(f'{ret}\n')


if __name__ == '__main__':
    main()
