import configparser
import psycopg2
import pandas as pd
from sql_queries import staging_immigration_table_insert, staging_demography_table_insert, insert_other_tables_queries

"""
    - Insert data in staging immigration and demography tables with immigration and demographics CSV files.
    - Copy commands passed in the for loop for execution
"""

def staging_immigration_table(cur, conn):
    df = pd.read_csv('./data/img_data')
    for i, row in df.iterrows():
        cur.execute(staging_immigration_table_insert, list(row))
        conn.commit()
        
def staging_demography_table(cur, conn):
    df = pd.read_csv('./data/dem_data')
    for i, row in df.iterrows():
        cur.execute(staging_demography_table_insert, list(row))
        conn.commit()
        
"""
    - Process insert function on all the analytics table, insert queries are passed individually in the for loop
"""

def insert_other_tables(cur, conn):
    for query in insert_other_tables_queries:
        cur.execute(query)
        conn.commit()

            


def main():
    
    '''
    A function that allows connect to database and process data, then close connection.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=capstoneprojdb user=student password=student")
    cur = conn.cursor()

    
    staging_immigration_table(cur, conn)
    staging_demography_table(cur, conn)
    insert_other_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()