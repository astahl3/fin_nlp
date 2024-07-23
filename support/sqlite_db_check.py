'''
Basic script for manually examining SQlite database contents; connects to the
database and loads contents into a pandas dataframe object.

Meant to be used with, e.g., submission_data_to_db.py for reviewing reddit
submisisons database.

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io
'''
import pandas as pd
import sqlite3

# Root database path
root_path = '/Users/astahl/fin_nlp_data/sqlite'

# Connect to the SQLite database
path_subs = '/reddit/submissions.db'
path_sample = '/reddit/submissions_sample.db'


# Table names
table_ticker_matches = 'single_ticker_matches'
table_performance = 'crsp_performance'
table_sample = 'sample_1'

path_ext = path_sample
conn = sqlite3.connect(root_path + path_ext)
desired_table = table_sample

# Query the database and load the results into a DataFrame
df = pd.read_sql_query(f"SELECT * FROM {desired_table}", conn)
#df = pd.read_sql_query("SELECT * FROM gpt_responses", conn)

# Tickers to investigate (from 1000 largest company tickers)
tickers_to_check = ['BECKY']

for ticker in tickers_to_check:
    query = f'''SELECT title, match_type 
                FROM {desired_table} 
                WHERE company_match = '{ticker}'
                '''
    df_temp = pd.read_sql_query(query, conn)
    
    # check dataframe and print title results
    if df_temp.empty:
        print("No records found for ticker:", ticker)
    else:
        # Print the titles to review them
        print("Titles containing the ticker", ticker, ":")
        for index, row in df_temp.iterrows():
            print(f"Title {index + 1}: {row['title']}")
            print(f"Match Type {index + 1}: {row['match_type']}")

# Close the database connection
#conn.close()

# Now you can explore the DataFrame
print(df.head())  # Print the first few rows of the DataFrame