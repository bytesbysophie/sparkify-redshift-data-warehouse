import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    '''
    Executes drop table statements from drop_table_queries
    
    Args:
    cur: Database connection cursor
    conn: Database connection object
    
    Returns:
    None
    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Executes create table statements from create_table_queries
    
    Args:
    cur: Database connection cursor
    conn: Database connection object
    
    Returns:
    None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_ENDPOINT            = config.get("DWH","DWH_ENDPOINT")
    DWH_DB                  = config.get("DWH","DWH_DB")
    DWH_DB_USER             = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD         = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT                = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()

    print("Dropping tables")
    drop_tables(cur, conn)
    print("Creating tables")
    create_tables(cur, conn)
    
    print("Closing connection")
    conn.close()


if __name__ == "__main__":
    main()