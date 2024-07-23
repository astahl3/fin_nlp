'''
Simple program to query information about the tables in local SQlite databases
and modify or delete tables within the database. 

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io 
'''

import sqlite3

############################## GLOBAL PARAMETERS ##############################

# List of database paths
path_submissions_db = \
    "/Users/astahl/fin_nlp_data/sqlite/reddit/submissions.db"
path_performance_db = \
    "/Users/astahl/fin_nlp_data/sqlite/wrds/stock_performance.db"
path_samples_db = \
    "/Users/astahl/fin_nlp_data/sqlite/reddit/submissions_samples.db"

desired_db = path_samples_db
###############################################################################

def get_tables(conn):
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = c.fetchall()
    table_names = []
    for table in tables:
        table_names.append(table[0])
        print(table[0])
        
    return table_names

def delete_table(conn, table_name):
    c = conn.cursor()
    c.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()

def print_table_info(conn, table_name):
    c = conn.cursor()
    
    # Get schema information
    c.execute(f"PRAGMA table_info({table_name});")
    columns = c.fetchall()
    print(f"\nSchema of {table_name}:")
    for column in columns:
        print(column)
    
    # Get indexes
    c.execute(f"PRAGMA index_list({table_name});")
    indexes = c.fetchall()
    print(f"\nIndexes on {table_name}:")
    for index in indexes:
        print(index)
    
    # Get foreign keys
    c.execute(f"PRAGMA foreign_key_list({table_name});")
    foreign_keys = c.fetchall()
    print(f"\nForeign keys on {table_name}:")
    for foreign_key in foreign_keys:
        print(foreign_key)
    
    # Get row count
    c.execute(f"SELECT COUNT(*) FROM {table_name};")
    row_count = c.fetchone()[0]
    print(f"\nNumber of rows in {table_name}: {row_count}")

def get_table_schema(conn, table_name):
    c = conn.cursor()
    c.execute(f"PRAGMA table_info({table_name});")
    columns = c.fetchall()
    table_cols = []
    print(f"\nSchema of {table_name}:")
    for column in columns:
        table_cols.append(column)
        print(column)
        
    return table_cols

def get_table_indexes(conn, table_name):
    c = conn.cursor()
    c.execute(f"PRAGMA index_list({table_name});")
    indexes = c.fetchall()
    table_idxs = []
    print(f"\nIndexes on {table_name}:")
    for index in indexes:
        table_idxs.append(index)
        print(index)
        
    return table_idxs

def get_table_foreign_keys(conn, table_name):
    c = conn.cursor()
    c.execute(f"PRAGMA foreign_key_list({table_name});")
    foreign_keys = c.fetchall()
    table_fkeys = []
    print(f"\nForeign keys on {table_name}:")
    for foreign_key in foreign_keys:
        table_fkeys.append(foreign_key)
        print(foreign_key)
        
    return table_fkeys

def get_table_row_count(conn, table_name):
    c = conn.cursor()
    c.execute(f"SELECT COUNT(*) FROM {table_name};")
    row_count = c.fetchone()[0]
    print(f"\nNumber of rows in {table_name}: {row_count}")
    return row_count
        
def main():
    
    # Connect to the SQLite database and get table names
    conn = sqlite3.connect(desired_db)
    
    print(f"Tables present in {desired_db}")
    table_list = get_tables(conn)
    
    # Print fields and info for each table in the list
    for table_name in table_list:
        print_table_info(conn, table_name)
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()