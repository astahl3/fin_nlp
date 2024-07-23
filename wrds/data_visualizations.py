'''
Generates visualizations about stock-related posts on reddit over time.
'''

import pandas as pd
import sqlite3
import matplotlib as mpl

# Convert out-of-date tickers to current ticker
ticker_conversions = {'GOOG':'GOOGL', 'FB':'META'}

# Root database path
root_path = '/Users/astahl/fin_nlp_data/reddit'

# Connect to the SQLite database
path_ext = '/sqlite/submissions.db'
conn = sqlite3.connect(root_path + path_ext)

# Table names
table_tickers_only = 'single_ticker_matches'

# Query the database and load the results into a DataFrame
df = pd.read_sql_query(f"SELECT * FROM {table_tickers_only}", conn)

# Apply ticker conversions
df['company_match'] = df['company_match'].replace(ticker_conversions)

# Calculate the count of mentions for each ticker
ticker_counts = df['company_match'].value_counts().reset_index()
ticker_counts.columns = ['company_match', 'count']

# Define bin edges and labels
bin_edges = [1, 2, 3, 5, 10, 20, 50, float('inf')]
bin_labels = ['1', '2', '3-5', '6-10', '11-20', '21-50', '50+']

# Assign each ticker count to a bin
ticker_counts['bin'] = pd.cut(ticker_counts['count'], bins=bin_edges, 
                              labels=bin_labels, right=False)

# Calculate the number of tickers in each bin
bin_counts = ticker_counts['bin'].value_counts().sort_index()

# Calculate the total number of posts in each bin
# Multiply the count of each company by the number of times they are mentioned 
# and sum these up for each bin
ticker_counts['total_posts'] = ticker_counts['count'] * ticker_counts['count']
bin_post_counts = ticker_counts.groupby('bin').sum()
bin_post_counts = bin_post_counts[['count']].copy()

# Plotting the results
#mpl.pyplot.figure(figsize=(10, 6))
#bin_counts.plot(kind='bar', color='skyblue')
#mpl.pyplot.title('Number of Companies in Each Bin Based on Mentions')
#mpl.pyplot.xlabel('Number of Mentions (Bins)')
#mpl.pyplot.ylabel('Number of Companies')
#mpl.pyplot.xticks(rotation=45)
#mpl.pyplot.grid(True, linestyle='--', linewidth=0.5)

# Plotting
fig, ax1 = mpl.pyplot.subplots(figsize=(10, 6))
fig_title = 'Frequency of Company Ticker Mentions in Reddit' + \
            'Submission Titles Organized by Bin Size'

# Bar plot for the number of companies in each bin
color = 'tab:blue'
ax1.set_xlabel('Number of Mentions')
ax1.set_ylabel('Number of Company Tickers in Bin', color=color)
ax1.bar(bin_counts.index, bin_counts, color=color)
ax1.tick_params(axis='y', labelcolor=color)
ax1.set_title(fig_title)

# Create a second y-axis for total number of posts
ax2 = ax1.twinx()  
color = 'tab:red'
ax2.set_ylabel('Total Number of Submissions in Bin', color=color)
ax2.plot(bin_post_counts.index, bin_post_counts, color=color, marker='o', linestyle='-')
ax2.tick_params(axis='y', labelcolor=color)
ax2.set_ylim([0, max(bin_post_counts['count']) * 1.1])  # Adjust the upper limit for better visualization

mpl.pyplot.tight_layout()
mpl.pyplot.show()

"""
# Tickers to investigate (from 1000 largest company tickers)
tickers_to_check = ['CBAT']

for ticker in tickers_to_check:
    query = f'''SELECT title, match_type 
                FROM {table_tickers_only} 
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
"""

# Close the database connection
conn.close()

# Now you can explore the DataFrame
print(df.head())  # Print the first few rows of the DataFrame