import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
        drop all tables that we created

        Args:
            cur: cursor to excutes operation within database 
            conn: connection to redshift
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
        create all tables that we need

        Args:
            cur: cursor to excutes operation within database 
            conn: connection to redshift
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
        connect to redshift, drop all tables that have been created, then re-create tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()