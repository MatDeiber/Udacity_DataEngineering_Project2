import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Function to drop the tables if they already exist
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """"
    Function to create the tables if they do not exist
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('Connecting to the Redshift cluster')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Dropping tables if they exist')
    drop_tables(cur, conn)
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()