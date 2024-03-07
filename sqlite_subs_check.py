#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  6 16:21:06 2024

@author: astahl
"""

import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('/Users/astahl/fin_nlp_data/reddit/sqlite/stocks_submissions.db')

# Query the database and load the results into a DataFrame
df = pd.read_sql_query("SELECT * FROM submissions", conn)

# Close the database connection
conn.close()

# Now you can explore the DataFrame
print(df.head())  # Print the first few rows of the DataFrame