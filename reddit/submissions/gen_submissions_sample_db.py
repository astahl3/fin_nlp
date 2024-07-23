import pandas as pd
import sqlite3

############################## GLOBAL PARAMETERS ##############################

# File paths for local databases
path_root = "/Users/astahl/fin_nlp_data"
path_submissions_db = path_root + "/sqlite/reddit/submissions.db"
path_sample_db = path_root + "/sqlite/reddit/submissions_sample.db"

# Table names
submissions_table = 'single_ticker_matches'
sample_table = 'sample_1'

###############################################################################

# Connect to the existing submissions database
conn = sqlite3.connect(path_submissions_db)
df = pd.read_sql_query(f"SELECT * FROM {submissions_table}", conn)
conn.close()

# Randomly sample 10 rows from the dataframe
sample_df = df.sample(n=10, random_state=1)

# Connect to the new database (create the file if it doesn't exist)
conn_out = sqlite3.connect(path_sample_db)
c = conn_out.cursor()

# Create new database
create_table_query = f"""CREATE TABLE IF NOT EXISTS {sample_table} (
                        author TEXT,
                        author_created_utc INTEGER,
                        author_fullname TEXT,
                        created_utc INTEGER,
                        domain TEXT,
                        id TEXT,
                        is_created_from_ads_ui BOOLEAN,
                        is_crosspostable BOOLEAN,
                        is_video BOOLEAN,
                        name TEXT,
                        num_comments INTEGER,
                        num_crossposts INTEGER,
                        over_18 BOOLEAN,
                        pinned BOOLEAN,
                        retrieved_on INTEGER,
                        score INTEGER,
                        selftext TEXT,
                        send_replies BOOLEAN,
                        subreddit TEXT,
                        subreddit_id TEXT,
                        subreddit_subscribers INTEGER,
                        title TEXT,
                        upvote_ratio REAL,
                        company_match TEXT
                        )"""
c.execute(create_table_query)

# Save the sampled dataframe to the new database
sample_df.to_sql(sample_table, conn_out, if_exists='replace', index=False)

# Commit and close the connection to the new database
conn_out.commit()
conn_out.close()