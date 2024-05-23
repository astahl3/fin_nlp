'''
Converts extracted .zst reddit site submission dumps to a SQlite database

Version: retains only submissios which mention a ticker or company alias in the
Wilshire 5000 *or* is a due diligence post, *and* satisfies a certain minimum 
length requirement (so as to avoid links to other sites, meme posts, etc.)

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io
'''

import json
import sqlite3
import pathlib
import pandas as pd
import re
from datetime import datetime

# Subreddit (must match with file path below)
subreddit_domain = 'self.wallstreetbets'
#subreddit_domain = 'self.stocks'
#subreddit_domain = 'self.investing'

# Global parameters
n_per_batch = 100 # submissions to process and write per batch
ticker_counts = 0 # ticker matches (rank 1 qualifier)
alias_counts = 0 # alias matches (rank 2 qualifier)
dd_counts = 0 # due diligence matches with alias or ticker match
dd_na_counts = 0 # due diligence matches, no company match (rank 3 qualifier)
ticker_matches = {} # dictionary for ticker matches
alias_matches = {} # dictionary for alias matches
min_L = 60 # minimum number of words required for the post

# Subreddit submissions path: /Users/astahl/fin_nlp_data/reddit/...
# r/stocks: stocks_submissions.txt
# r/investing: investing_submissions.txt
# r/wallstreetbets: wallstreetbets_submissions.txt

# File paths
path_reddit_db_read = \
    "/Users/astahl/fin_nlp_data/reddit/wallstreetbets_submissions.txt"
path_reddit_db_write = \
    "/Users/astahl/fin_nlp_data/reddit/sqlite/qualified_submissions_test.db"
path_stocks_db_read = \
    "/Users/astahl/fin_nlp_data/us_companies_5000.csv"
path_logfile_write = \
    "/Users/astahl/fin_nlp_data/reddit/logfiles/logfile.txt"

def check_if_DD(input_string):
    ''' Return True if title indicates a due diligence post '''
    
    global dd_counts
    if 'dd' in input_string.lower().split() or \
        'due diligence' in input_string.lower():
        dd_counts += 1
        return True
    else:
        return False
    
def process_submission(submission, fields, tickers=None, aliases=None):
    '''
    (1) Process submissions and title text by performing the following:
        (a) remove hyperlinks from submissions text
        (b) remove special characters from title string
    
    (2) Evaluate whether submission satisfies each qualifying criteria:
        (a) title text contains a ticker or alias match
        (b) title text indicates a due diligence post
        (c) selftext passage must exceed a minimum length 'min_L'
    
    [(2a) or (2b)] and (2c) required to qualify 
    
    (3) If qualified, check title for due diligence tag and update is_DD field
    '''
    
    global ticker_matches
    global alias_matches
    global ticker_counts
    global alias_counts
    global dd_counts
    global dd_na_counts
    
    if (submission.get('domain') == subreddit_domain and 
       submission.get('selftext') != '[removed]' and
       submission.get('selftext') != '' and
       submission.get('selftext') != '[deleted]'):
        
        title_string = submission.get('title')
        selftext_string = submission.get('selftext')
        
        # Confirm that it is a due diligence post
        if len(selftext_string.split()) > min_L:
            
            # Remove common special characters from title to improve alias
            # and ticker matching reliability
            for char in ['$', ':', '!', '#', '~']:
                title_string = title_string.replace(char, '')
            
            # Check for ticker match in title string
            for word in title_string.split():
                if word in tickers:
                    ticker_counts += 1
                    submission['company_match'] = word
                    ticker_matches[word] = ticker_matches.get(word,0)+1
                    
                    # Check if this is a due diligence post
                    submission['is_DD'] = check_if_DD(title_string)
                            
                    return tuple(submission.get(field) for field in fields)
                
            # Does the title contain a string matching an alias?
            match = re.search(aliases, title_string, re.IGNORECASE)
            if match:
                alias_counts += 1
                matched_alias = match.group(0)
                submission['company_match'] = matched_alias
                alias_matches[matched_alias] = \
                    alias_matches.get(matched_alias,0)+1
                
                # Check if this is a due diligence post
                submission['is_DD'] = check_if_DD(title_string)
                
                return tuple(submission.get(field) for field in fields)
            
            # Add if this is a due diligence post (even without company match)
            if check_if_DD(title_string):
                submission['company_match'] = 'N/A'
                submission['is_DD'] = True
                dd_na_counts += 1
                return tuple(submission.get(field) for field in fields)
                
    return None

def process_submissions_file(submissions_file, fields, batch_size=2000, 
                             tickers=None, aliases=None):
    '''
    Process submissions from extracted Reddit .zst file in batches, parse
    specified fields (extract .zst files via zstd -d submission_filename.zst)
    Subreddit .zst archives are available at https://the-eye.eu/redarcs/ - see
    the Discord server for most the up-to-date files
    Note: submission read files must be encoded in UTF-8
    
    Parameters:
    -----------
    - submissions_file (str): path to the extracted submissions file
    - fields (list of str): fields to extract from each submission
    - batch_size (int, optional): submissions per batch
    - tickers (set, optional): set of stock tickers to match with title
    - aliases (str, optional): regex string of company aliases
    
    Returns:
    --------
    - None: function only processes submissions and outputs to a SQlite db

    '''
    
    #Process the submissions file in batches and extract desired fields
    with open(submissions_file, "r", encoding="utf-8") as file:
        submissions = []
        batch = []
        batch_count = 0
        for line in file:
            submission_data = json.loads(line.strip())
            submission = {field: submission_data.get(field, None) 
                                  for field in fields}
            
            # Select qualified submissions and identify 
            processed_submission = process_submission(submission, fields, 
                                                      tickers, aliases)
            if processed_submission:
                batch.append(processed_submission)
                if len(batch) >= batch_size:
                    processed_batch = process_batch(batch)
                    #submissions.extend(processed_batch)
                    write_submissions_to_database(processed_batch)
                    batch = []
                    batch_count += 1
                    print(f"Wrote {batch_count*batch_size} entries to db")
                    with open(path_logfile_write, 'a') as lf:
                        lf.write(f"Wrote {batch_count*batch_size} entries\n")
                        
        # Process the remaining submissions in the last batch
        if batch:
            processed_batch = process_batch(batch)
            submissions.extend(processed_batch)
            batch_count += 1
            total_processed = batch_count * batch_size + len(batch)
            print(f"Wrote {total_processed} entries to db")
            with open(path_logfile_write, 'a') as lf:
                lf.write(f"Wrote {total_processed} entries\n")
            
    with open(path_logfile_write, 'a') as lf:
        lf.write(f"Finished processing subreddit domain: {subreddit_domain}")
        
def process_batch(batch):
    ''' Filter and process a batch of submissions (placeholder function) '''
    processed_batch = []
    for submission in batch:
        if submission:
            processed_batch.append(submission)
    return processed_batch

def write_submissions_to_database(submissions):
    ''' Write passed submissions list to SQLite database '''
    
    # Connect to SQLite database
    conn = sqlite3.connect(path_reddit_db_write)
    c = conn.cursor()

    # Create submissions table if it doesn't already exist
    c.execute('''CREATE TABLE IF NOT EXISTS submissions (
                author TEXT,
                author_created_utc INTEGER,
                author_fullname TEXT,
                created_utc INTEGER,
                domain TEXT,
                id TEXT PRIMARY KEY,
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
                company_match TEXT,
                is_DD BOOLEAN
                )''')

    # Write submissions to database
    insert_query = '''INSERT OR REPLACE INTO submissions VALUES (?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    
    for submission in submissions:
        if submission:  # Check if submission is not None
            c.execute(insert_query, submission)
    
    # Close connection until next batch
    conn.commit()
    conn.close()

def main():
    
    # Open logfile to print header information
    with open(path_logfile_write, 'a') as lf:
        lf.write('\n')
        current_time = datetime.now()
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        lf.write(f"********** Starting log: {formatted_time}\n")
        lf.write("********** Program: submission_data_to_db.py\n")
        lf.write(f"********** Destination: {path_reddit_db_write}\n")
        lf.write(f"********** Subreddit domain: {subreddit_domain}\n")
    
    # Path to the submissions file
    submissions_file = pathlib.Path(path_reddit_db_read).expanduser()
  
    # Define the fields to extract
    fields = ["author", "author_created_utc", "author_fullname", "created_utc", 
              "domain", "id", "is_created_from_ads_ui", "is_crosspostable", 
              "is_video", "name", "num_comments", "num_crossposts", "over_18",
              "pinned", "retrieved_on", "score", "selftext", "send_replies", 
              "subreddit", "subreddit_id", "subreddit_subscribers", "title",
              "upvote_ratio", "company_match", "is_DD"]
    
    # Load tickers and company names to cross-reference with submission titles
    company_list = pd.read_csv(path_stocks_db_read)
        
    # The function pd.read_csv() imports NA as not available number "nan"
    company_list.loc[company_list['alias'] == "Nano Labs", 'ticker'] = "NA" 
    
    # Make sure all entries to be strings
    company_list['ticker'] = company_list['ticker'].astype(str)

    # Create set of all company tickers 
    ticker_set = set(company_list['ticker'])
    
    # Ignore ticker "A": creates too many false positives
    ticker_set.remove('A')
    
    # Ignore ticker "DD": overlaps with due diligence tag; handled separately
    ticker_set.remove('DD')
        
    # Assume alias is a set of company aliases; may contain multiple words
    alias_pattern = r'\b(' + '|'.join(re.escape(alias) \
                    for alias in company_list['alias'].astype(str)) + r')\b'

    # Process submissions file and extract desired fields of qualified 
    # submissions in batches; then write each batch to SQlite database
    process_submissions_file(submissions_file, fields, n_per_batch, 
                             ticker_set, alias_pattern)
    
    sorted_ticker_matches = sorted(ticker_matches.items(), 
                                   key=lambda item: item[1], reverse=False)
    
    # Print summary statistics
    with open(path_logfile_write, 'a') as lf:
        lf.write(f"Summary stats for {subreddit_domain}\n")
        lf.write("Ticker matches:\n")   
        for ticker, count in sorted_ticker_matches:
            lf.write(f"{ticker}: {count}\n")
        
        lf.write("ticker matches: %2.0d, alias matches: %2.0d, " 
                 % (ticker_counts, alias_counts))
        lf.write("dd only matches: %2.0d, total unique matches %2.0d \n" 
                 % (dd_na_counts, ticker_counts+alias_counts+dd_na_counts))
        
        lf.write("total titles with dd tags: %2.0d\n\n" % (dd_counts))
        
if __name__ == "__main__":
    main()




