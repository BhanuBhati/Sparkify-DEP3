import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
This procedure takes cursor and connection objects as arguments.
Loops through queries in copy_table_queries list from sql_queries.py
Executes each COPY command to load required staging tables using data stored in S3 bucket.
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
This procedure takes cursor and connection objects as arguments.
Loops through queries in insert_table_queries list from sql_queries.py
Executes each query to insert data into tables using the staging tables.
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

"""
The main procedure reads the config file.
Establishes a connection to RedShift server using CLUSTER values of config file.
Makes a cursor object from the established connection.
Then, it loads the staging tables using load_staging_tables method.
Then, it inserts data into required tables using data from staging tables using insert_tables method.
Finally, ends with closing the connection.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()