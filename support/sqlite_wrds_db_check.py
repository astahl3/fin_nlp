'''
Basic script for manually examining SQlite database contents; connects to the
database and loads contents into a pandas dataframe object.

Meant to be used with, e.g., crsp_performance_db.py for reviewing WRDS / CRSP
generated performance databases.

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io
'''
import pandas as pd
import sqlite3

# Root database path
root_path = '/Users/astahl/fin_nlp_data/sqlite/wrds'

# Connect to the SQLite database
path_ext = '/stock_performance.db'
conn = sqlite3.connect(root_path + path_ext)

# Table names
table_security = 'crsp_securities'
table_performance = 'crsp_returns'
desired_table = table_performance

# Query the database and load the results into a DataFrame
df = pd.read_sql_query(f"SELECT * FROM {desired_table}", conn)
#df = pd.read_sql_query("SELECT * FROM gpt_responses", conn)

# Close the database connection
conn.close()

# Now you can explore the DataFrame
print(df.head())  # Print the first few rows of the DataFrame