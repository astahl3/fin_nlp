'''
Using database of qualified reddit submissions, query an OpenAI GPT Endpoint
Completion model to gauge market sentiment, writing quality, and to confirm 
the security under consideration.

Store results in new SQlite submissions database linked to the submission ID
'''

import os
from openai import OpenAI as oai
import pandas as pd
import sqlite3
from datetime import datetime


def create_prompt(selftext):
    ''' 
    Prompt for obtaining consistently formatted sentiment classification and 
    quality ratings. This prompt beats out several dozen alternatives of 
    varying length in terms of the consistency with which it generates the
    desired values and in the desired structure
    '''
        
    return f"""Please read the text passage following this prompt and respond with three values in this exact format: sentiment writing_quality company_name
    sentiment reflects the sentiment of the author toward the main company being discussed, it is an integer ranging from 1 to 10 where 1 is very bearish (author would short sell the company stock) and 10 is very bullish (author would buy the company stock). No negative values.
    writing_quality is an integer ranging from 1 to 10 where 1 is average quality or below, 10 is very high quality
    company_name is the stock ticker (preferred) or name of the main company being discussed
    If a main company can't be determined, return N/A for company_name and 0 for both writing_quality and sentiment
    These three values should be separated only by a single space. Here is an example of what a response should look like: "7 10 MSFT" 
    
    This is the text to be analyzed:
    {selftext}
    """
    

# Configure API key
api_key = os.getenv('OPENAI_API_KEY')

# Connect to the original SQLite database with Reddit submissions
conn = sqlite3.connect('/Users/astahl/fin_nlp_data/reddit/sqlite/DD_submissions_sample.db')
subs = pd.read_sql_query("SELECT * FROM submissions", conn)
conn.close()  # Close connection after loading data

# Create a new SQLite database for storing GPT responses
new_conn = sqlite3.connect('/Users/astahl/fin_nlp_data/reddit/sqlite/GPT_responses27.db')
cursor = new_conn.cursor()

# Create a table in the new database with the desired structure
cursor.execute('''
CREATE TABLE IF NOT EXISTS gpt_responses (
    id TEXT PRIMARY KEY,
    market_sentiment INT,
    writing_quality INT,
    company_id TEXT
)
''')

# Create a logfile of GPT responses
# Open a file and write output within a loop
with open('/Users/astahl/fin_nlp/gpt_logs/logfile.txt', 'a') as logfile:
        logfile.write('\n')

        # Get current date and time
        current_time = datetime.now()
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        logfile.write(f'********** Starting log: {formatted_time} **********\n')


# Iterate over each entry in the dictionary
client = oai(api_key=api_key)
for entry_id, selftext in subs.set_index('id')['selftext'].to_dict().items():
    # Create the prompt
    prompt = create_prompt(selftext)

    # Request sentiment analysis from OpenAI
    response = client.completions.create(
        model="gpt-3.5-turbo-instruct",  # Update engine as per your choice
        prompt=prompt,
        max_tokens=10,
        temperature=1.0
    )
    
    # Response parsing and invalid response error handling
    response_string = response.choices[0].text.strip().lower()
    
    with open('/Users/astahl/fin_nlp/gpt_logs/logfile.txt', 'a') as logfile:
            logfile.write(f'Current response: {response_string}\n')
    
    response_string = response_string.replace('sentiment','')
    response_string = response_string.replace('investment sentiment','')
    response_string = response_string.replace('investment_sentiment','')
    response_string = response_string.replace('quality','')
    response_string = response_string.replace('writing quality','')
    response_string = response_string.replace('writing_quality','')
    response_string = response_string.replace(':','')
    response_text = response_string.split()
    #response_text = response.choices[0].text.strip().split()
    
    # Sometimes company_id given as two or three strings; keep first two
    if len(response_text) > 3:
        combined_name = str(response_text[2]) + ' ' + str(response_text[3])
        response_text[2] = combined_name

    # If result has correct number of strings, populate fields
    if len(response_text) == 3:
        try:
            market_sentiment = int(response_text[0])
            writing_quality = int(response_text[1])
            company_id = response_text[2].upper()
        except ValueError:
            market_sentiment = 0
            writing_quality = 0
            company_id = 'NA'

    with open('/Users/astahl/fin_nlp/gpt_logs/logfile.txt', 'a') as logfile:
            logfile.write(f'Parsed response: {market_sentiment} {writing_quality} {company_id} \n')
            
    print(f'company_id {company_id}')
    print(f'market_sentiment {market_sentiment}')
    print(f'writing_quality {writing_quality}')
    # Insert the response into the new database
    cursor.execute('''
    INSERT INTO gpt_responses (id, company_id, market_sentiment, writing_quality)
    VALUES (?, ?, ?, ?)
    ''', (entry_id, company_id, writing_quality, market_sentiment))
    
    # Commit after each insertion
    new_conn.commit()

# Close the new database connection
new_conn.close()

# Close log file


print("GPT data processing and storage complete.")


