import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
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

    print("Loading tables")
    load_staging_tables(cur, conn)
    print("Inserting data")
    insert_tables(cur, conn)

    print("Closing connection")
    conn.close()


if __name__ == "__main__":
    main()