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

path_gpt_prompt = "/Users/astahl/fin_nlp/gpt_prompt_a.txt"
path_gpt40mini_sys = "/Users/astahl/fin_nlp/gpt40mini_prompt_a.txt"
path_gpt40mini_user = "/Users/astahl/fin_nlp/gpt40mini_prompt_user_a.txt"
desired_prompt = path_gpt40mini_user

# Table names
#submissions_table = 'single_ticker_matches'
submissions_table = 'sample_1'
gpt_table = 'gpt40mini_responses'

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
    
    # Set gpt system prompt
    with open(desired_prompt, 'r') as p:
        sys_prompt = p
            
    # Iterate over each entry in the dictionary
    client = oai(api_key=api_key)
    for entry_id, selftext in subs.set_index('id')['selftext']. \
                                                    to_dict().items():
        
        user_prompt = create_prompt(selftext)
        response = client.chat.completions.create(
          model="gpt-4o-mini",
          messages=[
            {"role": "system", "content": f"{sys_prompt}"},
            {"role": "user", "content": f"{user_prompt}"}
          ]
        )
        
        response_string = response.choices[0].message.content.strip().lower()        
        with open(path_logfile_write, 'a') as lf:
                lf.write(f'Current response: {response_string}\n\n')
                
        response_string = clean_gpt_response(response_string)
        response_text = response_string.split()
        #response_text = response.choices[0].text.strip().split()
        
        # Sometimes extra response text is included; try to delete it
        if len(response_text) > 2: del response_text[2:]
    
        try:
            market_sentiment = int(response_text[0])
            writing_quality = int(response_text[1])
        except ValueError:
            market_sentiment = 0
            writing_quality = 0
        
        # Print parsed GPT response results
        with open(path_logfile_write, 'a') as lf:
                lf.write("Parsed response:\n")
                lf.write(f"market sentiment = {market_sentiment}")
                lf.write(f"writing quality = {writing_quality}\n") 
        
        print(f"selftext = {selftext}")
        print(f"market_sentiment {market_sentiment}")
        print(f"writing_quality {writing_quality}")
        
        # Insert gpt response values into the new database
        insert_text = f"""
                    INSERT INTO {gpt_table} 
                    (id, company_id, market_sentiment, 
                     writing_quality, date_added)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        market_sentiment = excluded.market_sentiment,
                        writing_quality = excluded.writing_quality,
                        date_added = excluded.date_added
                        """
        c.execute(insert_text, 
                  (entry_id, market_sentiment, writing_quality, current_date))
        
        # Commit after each insertion
        conn.commit()
    
    # Close the new database connection
    conn.close()
    
    # Close log file
    print("GPT data processing and storage complete.")


if __name__ == "__main__":
    main()