#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import sqlite3
import pathlib
import pandas as pd
import re

# Global parameters
subreddit_domain = 'self.stocks'
n_per_batch = 1000
alias_matches = 0
ticker_matches = 0

'''
# If submission is valid, add to database (via RegEx)
def process_submission_re(submission, fields, ticker_pattern):
    if (submission.get('domain') == subreddit_domain and 
       submission.get('selftext') != '[removed]' and
       submission.get('selftext') != '' and
       submission.get('selftext') != '[deleted]'):
        
        # Grab title and convert to all upper case
        title_string = submission.get('title').upper()
        #title_string = submission.get('title')
        
        # Does the ticker string exist in the title 
        # (e.g., "MSFT!!!" -> True)
        
        if re.search(ticker_pattern, title_string):
            return tuple(submission.get(field) for field in fields)
        
    return None
'''

# If submission is valid, add to database (via set check)
def process_submission(submission, fields, tickers, aliases):
    global ticker_matches
    global alias_matches
    
    if (submission.get('domain') == subreddit_domain and 
       submission.get('selftext') != '[removed]' and
       submission.get('selftext') != '' and
       submission.get('selftext') != '[deleted]'):
        
        # Grab title and convert to all upper case
        #title_string = submission.get('title').upper()
        title_string = submission.get('title')
        
        if tickers and aliases:
            # Does the title contain a string matching a ticker?
            # Ticker e.g., "MSFT!!!" -> False, "... bought MSFT today ..." -> True
            if any(word in tickers for word in title_string.split()):
                ticker_matches = ticker_matches + 1
                return tuple(submission.get(field) for field in fields)   
                
            # Does the title contain a string matching an alias?
            if re.search(aliases, title_string, re.IGNORECASE):
                alias_matches += 1
                return tuple(submission.get(field) for field in fields)
        
        elif tickers:
            # Does the title contain a string matching a ticker? 
            # Ticker e.g., "MSFT!!!" -> False, "... bought MSFT today ..." -> True
            if any(word in tickers for word in title_string.split()):
                return tuple(submission.get(field) for field in fields)
        
        else:
            return tuple(submission.get(field) for field in fields)
    
    return None

def process_submissions_file(submissions_file, fields, batch_size=2000, tickers=None, aliases=None):
    """
    Process submissions from a reddit submissions.txt file in batches, extracting specified fields
    The submissions.txt file should be extracted from a reddit .zst files using: zstd -d submission_filename.zst 

    Parameters:
        - submissions_file (str): path to the file containing reddit submissions
        - fields (list of str): fields to extract from each submission
        - batch_size (int, optional): number of submissions to process in each batch; defaults to 2000
        - tickers (set, optional): set of tickers to check against submission titles; defaults to None
        - aliases (str, optional): regex string of company aliases to check against submission titles; defaults to None

    Returns:
    - None: This function does not return a value; it processes the file and may output to a database or file.

    Notes:
    - This function opens the specified file, reads submissions in batches, and processes each according to the specified fields and optional ticker/alias checks.
    - Ensure the submissions_file is correctly formatted and encoded in UTF-8.
    """
    
    #Process the submissions file in batches and extract desired fields
    with open(submissions_file, "r", encoding="utf-8") as file:
        submissions = []
        batch = []
        batch_count = 0
        for line in file:
            submission = parse_submission_line(line.strip(), fields)
            processed_submission = process_submission(submission, fields, tickers, aliases)
            if processed_submission:
                batch.append(processed_submission)
                if len(batch) >= batch_size:
                    processed_batch = process_batch(batch)
                    submissions.extend(processed_batch)
                    batch = []
                    batch_count += 1
                    print(f"Processed {batch_count * batch_size} submissions.")
        
        # Process the remaining submissions in the last batch
        if batch:
            processed_batch = process_batch(batch)
            submissions.extend(processed_batch)
            batch_count += 1
            print(f"Processed {batch_count * batch_size + len(batch)} submissions.")
    return submissions


def process_batch(batch):
    """Filter and process a batch of submissions."""
    processed_batch = []
    for submission in batch:
        if submission:
            processed_batch.append(submission)
    return processed_batch


# Parse a single line representing a submission and extract desired fields
def parse_submission_line(line, fields):
    submission_data = json.loads(line)
    submission = {field: submission_data.get(field, None) for field in fields}
    return submission


def write_submissions_to_database(submissions, batch_size=1000):
    """Write submissions to SQLite database in batches."""
    # Connect to SQLite database
    conn = sqlite3.connect("/Users/astahl/fin_nlp_data/reddit/sqlite/stocks_submissions.db")
    c = conn.cursor()

    # Create submissions table if not exists
    c.execute('''CREATE TABLE IF NOT EXISTS submissions (
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
                upvote_ratio REAL
                )''')

    # Write submissions in batches
    batch_count = 0
    for submission in submissions:
        if submission:  # Check if submission is not None
            c.execute('''INSERT OR IGNORE INTO submissions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      submission)
            batch_count += 1
            if batch_count % batch_size == 0:
                conn.commit()
                print(f"Wrote {batch_count} submissions to database.")
    conn.commit()
    conn.close()


def main():
    
    # Path to the submissions file
    submissions_file = pathlib.Path("/Users/astahl/fin_nlp_data/reddit/stocks_submissions.txt").expanduser()
    
    # Define the fields to extract
    fields = ["author", "author_created_utc", "author_fullname", "created_utc", "domain", "id",
              "is_created_from_ads_ui", "is_crosspostable", "is_video", "name", "num_comments", "num_crossposts",
              "over_18", "pinned", "retrieved_on", "score", "selftext", "send_replies", "subreddit",
              "subreddit_id", "subreddit_subscribers", "title", "upvote_ratio"]
    
    # Load tickers and company names to cross-reference with title and possibly selftext fields
    company_list = pd.read_csv('/Users/astahl/fin_nlp_data/us_companies.csv')
    company_list.loc[company_list['alias'] == "Nano Labs", 'ticker'] = "NA" # pd.read_csv() imports NA as nan
    company_list['ticker'] = company_list['ticker'].astype(str) # make sure all entries are strings

    
    # Create two sets out of the ticker and alias columns
    ticker_set = set(company_list['ticker'])
    ticker_set.remove('A') # ignore this ticker as it causes too many false positives
    
    alias_set = set(company_list['alias'])
    
    # Create a regex pattern for checking if tickers appeear in the title strings
    #ticker_pattern = r'\b(' + '|'.join(re.escape(ticker) for ticker in ticker_set) + r')\b'
    
    # Assume alias_set is a set of company aliases, some of which may contain multiple words
    alias_pattern = r'\b(' + '|'.join(re.escape(alias) for alias in company_list['alias'].astype(str)) + r')\b'

    # Process submissions file and extract desired fields in batches
    submissions = process_submissions_file(submissions_file, fields, n_per_batch, ticker_set, alias_pattern)

    # Write submissions to SQLite database
    write_submissions_to_database(submissions)
    
    print(f"ticker matches = {ticker_matches}, alias matches = {alias_matches}")

if __name__ == "__main__":
    main()




