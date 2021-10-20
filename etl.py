import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
        load event and song data to staging tables

        Args:
            cur: cursor to excutes operation within database 
            conn: connection to redshift
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
        insert data into tables of star schema from staging tables

        Args:
            cur: cursor to excutes operation within database 
            conn: connection to redshift
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
        connect to redshift, load event and song data to staging tables 
        then insert data into tables of star schema from staging tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()