'''
Using database of qualified reddit submissions, query the openAI GPT endpoint
completion model to gauge market sentiment, writing quality, and to confirm 
the security under consideration.

Store results in new table inside existing submissions database.

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io 
'''

import os
from openai import OpenAI as oai
import pandas as pd
import sqlite3
from datetime import datetime

############################## GLOBAL PARAMETERS ##############################

# File paths for local databases
path_root = "/Users/astahl/fin_nlp_data"
#path_submissions_db = path_root + "/sqlite/reddit/submissions.db"
path_submissions_db = path_root + "/sqlite/reddit/submissions_sample.db"
path_logfile_write = path_root + "/reddit/logfiles/logfile"
path_gpt_prompt = '/Users/astahl/fin_nlp/gpt_prompt_a.txt'

# Table names
#submissions_table = 'single_ticker_matches'
submissions_table = 'sample_1'
gpt_table = 'gpt_responses'

# Latest available date ("lad") for CRSP stock data
lad = datetime(year=2024, month=3, day=28).date()
lad_str = lad.strftime('%Y-%m-%d')

###############################################################################

def create_prompt(selftext):
    ''' 
    Prompt for obtaining consistently formatted sentiment classification and 
    quality ratings. This prompt beats out several dozen alternatives of 
    varying length in terms of the consistency with which it generates the
    desired values and in the desired structure
    
    '''
    # Open the file in read mode and load its content as a string
    with open(path_gpt_prompt, 'r') as f:
        file_content = f.read()
    
    gpt_prompt = file_content + f"{selftext}"
    return gpt_prompt


def clean_gpt_response(gpt_reply):
    '''
    Remove common mistakes from the GPT response (words, special charactes)
    '''
    # TODO: implement regex routine to remove all letters except a 
    # single substring with capital letters and length 1-5 and remove any 
    # special characters
    gpt_reply = gpt_reply.replace('sentiment','')
    gpt_reply = gpt_reply.replace('investment sentiment','')
    gpt_reply = gpt_reply.replace('investment_sentiment','')
    gpt_reply = gpt_reply.replace('quality','')
    gpt_reply = gpt_reply.replace('writing quality','')
    gpt_reply = gpt_reply.replace('writing_quality','')
    gpt_reply = gpt_reply.replace(':','')
    return gpt_reply

def main():
    global path_logfile_write
    
    # Configure API key
    api_key = os.getenv('OPENAI_API_KEY')
    
    # Connect to the original SQLite database with Reddit submissions
    conn = sqlite3.connect(path_submissions_db)
    subs = pd.read_sql_query(f"SELECT * FROM {submissions_table}", conn)
    c = conn.cursor()
    
    # Create a table in the submissions database to store GPT responses
    c.execute('''
    CREATE TABLE IF NOT EXISTS gpt_responses (
        id TEXT PRIMARY KEY,
        company_id TEXT,
        market_sentiment INT,
        writing_quality INT,
        date_added DATE
    )
    ''')
    conn.commit()
    
    # Open logfile to print header information
    current_date = datetime.date(datetime.now()).strftime('%Y-%m-%d')
    path_logfile_write = path_logfile_write + '_' + current_date + '.txt'
    with open(path_logfile_write, 'a') as lf:
        lf.write('\n')
        current_time = datetime.now()
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        lf.write(f"********** Starting log: {formatted_time}\n")
        lf.write("********** Running program: gpt_sentiment_extractor.py\n")
        lf.write(f"********** Loading DB: {path_submissions_db}\n")
        lf.write(f"********** Table read name: {submissions_table}\n")
        lf.write(f"********** Table write name: {gpt_table}\n\n")
    
    # Iterate over each entry in the dictionary
    client = oai(api_key=api_key)
    for entry_id, selftext in subs.set_index('id')['selftext']. \
                                                    to_dict().items():
        
        prompt = create_prompt(selftext)
        response = client.completions.create(
            model="gpt-3.5-turbo-instruct",  # Update engine as per your choice
            prompt=prompt,
            max_tokens=10,
            temperature=1.0
        )
        
        response_string = response.choices[0].text.strip().lower()        
        with open(path_logfile_write, 'a') as lf:
                lf.write(f'Current response: {response_string}\n\n')
                
        response_string = clean_gpt_response(response_string)
        response_text = response_string.split()
        #response_text = response.choices[0].text.strip().split()
        
        # Sometimes company_id given as two or three strings; keep first two
        if len(response_text) > 3:
            combined_name = str(response_text[2]) + ' ' + str(response_text[3])
            response_text[2] = combined_name
            del response_text[3:]
    
        try:
            market_sentiment = int(response_text[0])
            writing_quality = int(response_text[1])
            company_id = response_text[2].upper()
        except ValueError:
            market_sentiment = 0
            writing_quality = 0
            company_id = 'NA'
        
        # Print parsed GPT response results
        with open(path_logfile_write, 'a') as lf:
                lf.write("Parsed response:\n")
                lf.write(f"market sentiment = {market_sentiment}")
                lf.write(f"writing quality = {writing_quality}\n") 
                lf.write(f"company id = {company_id} \n\n")        
        
        print(f"selftext = {selftext}")
        print(f"company_id {company_id}")
        print(f"market_sentiment {market_sentiment}")
        print(f"writing_quality {writing_quality}")
        
        # Insert gpt response values into the new database
        insert_text = f"""
                    INSERT INTO {gpt_table} 
                    (id, company_id, market_sentiment, 
                     writing_quality, date_added)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        company_id = excluded.company_id,
                        market_sentiment = excluded.market_sentiment,
                        writing_quality = excluded.writing_quality,
                        date_added = excluded.date_added
                        """
        c.execute(insert_text, 
                  (entry_id, company_id, market_sentiment, 
                   writing_quality, current_date))
        
        # Commit after each insertion
        conn.commit()
    
    # Close the new database connection
    conn.close()
    
    # Close log file
    print("GPT data processing and storage complete.")


if __name__ == "__main__":
    main()