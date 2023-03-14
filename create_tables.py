import psycopg2
from sql_queries import create_staging_tables_queries, create_other_tables_queries, drop_table_queries

def create_database():
    """
    - Creates and connects to the capstoneprojdb
    - Returns the connection and cursor to capstoneprojdb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create capstoneprojdb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS capstoneprojdb")
    cur.execute("CREATE DATABASE capstoneprojdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to capstoneprojdb database
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstoneprojdb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

"""
    - Reads queries from a list and then drops existing tables on the database
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
    - Create tables one by one, passed as a list of queries in the for loop
"""
def create_staging_tables(cur, conn):
    for query in create_staging_tables_queries:
        cur.execute(query)
        conn.commit()
        
def create_other_tables(cur, conn):
    for query in create_other_tables_queries:
        cur.execute(query)
        conn.commit()

"""
    - Read the config file for connecting to the DB
    - Call the drop table function
    - Call the create table function
    - Close the connection
"""

def main():
    
    '''
    A function that allows connect to database and process data, then close connection.
    '''
    
#     conn = psycopg2.connect("host=127.0.0.1 dbname=capstoneprojdb user=student password=student")
#     cur = conn.cursor()

    cur, conn = create_database()
    drop_tables(cur, conn)
    create_staging_tables(cur, conn)
    create_other_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()