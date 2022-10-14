import psycopg2
import logging
log_format = '%(asctime)s %(levelname)s %(name)s %(message)s'
log_kwargs = {'level': logging.INFO, 'filemode': 'w',
                'format': log_format, 'force': True}
logging.basicConfig(**log_kwargs)
logger = logging.getLogger(__name__)


def main():
    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgres'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here


if __name__ == '__main__':
    main()
