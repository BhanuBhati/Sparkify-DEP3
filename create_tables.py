import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
The procedure drop_tables takes cursor and connection variables are arguments.
Loops through the drop_table_queries created in sql_queries.py
execute each of the drop table query to drop every table.
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
The procedure create_tables takes cursor and connection variables are arguments.
Loops through the create_table_queries created in sql_queries.py
execute each of the create table query to create the required tables.
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
The main procedure reads the config file.
Establishes a connection to RedShift server using CLUSTER values of config file.
Makes a cursor object from the established connection.
Then, it drops the existing tables using drop_tables method.
Then, it creates fresh tables using create_tables method.
Finally, ends with closing the connection.
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()