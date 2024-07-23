'''
Functions for querying stock histories via the wrds module, which interfaces
with the Postgre SQL database managed by Wharton Research Data Services (WRDS).
Intended for use with the fin_nlp project, which seeks to assess the merit
of investment-related ideas expressed on network platforms (e.g., reddit, 
twitter|X, discord). 

###########################
######## Stock IDs ########
###########################
Datastream and CRSP both use their own internal identification system for
market securities (i.e., stocks). However, they can be connected via the CUSIP.

Datastream uses ISIN (International Security Identification Number), which is
the country code followed by the CUSIP (Committee on Uniform Securities 
Identification Procedures) followed by a check digit. The CUSIP may also have
its own check digit. The check digit is a number that is calculated from the 8 
CUSIP characters and offers a zeroth-order way to confirm you have the right 
security CUSIP. Here's an example:
    
    Microsoft:
    ISIN = US5949181045
    ---------------------
    US + 59491810 + 4 + 5
    US = country code
    59491810 = CUSIP
    4 = CUSIP check digit
    5 = ISIN check digit
    
There are several functions included in this file that allow conversion between
CUSIP, CUSIP9 (CUSIP + check digit), and ISIN.
 
######################
######## CRSP ########
######################

Useful libraries and tables:
- crsp_q_stock: daily stock data updated quarterly (through 2024-03-28)
    * stocknames: metainfo on stocks
    * stocknames_v2: metainfo with additional fields compared to stocknames
    * dsf: daily stock files
- crsp_a_stock: daily stock data updated annually (through 2023-12-29)
    * stocknames: metainfo on stocks
    * stocknames_v2: metainfo with additional fields compared to stocknames
    * dsf: daily stock files
    
Relevant field definitions:
- permco: permanent unique company id, constant even if ticker changes
- permno: permanent unique security id, a single company can have more than 1
- cfacpr: cumulative price factor that includes dividends and splits
- cfacshr: cumulative price factor that includes only splits
- prc: closing price of a security
- ret: daily return of a security including splits and dividends

Notes regarding stock CRSP metainfo:
- sharetype: NS are new shares; AD are preferred shares

###########################
####### Datastream ########
###########################

- tr_ds_equities: daily stock data updated daily (through current date)
    * wrds_ds_names: metainfo on a particular equity security
    * wrds_ds2security: additional metainfo on a security
    * wrds_ds2dsf: daily stock files
    
Definitions of fields within the WRDS / Datastream database:
- dscode: unique identifier for a security
- ismajorsec: indicates if it's company's primary security (ds2security table)
    
####################################
####### Calculating Returns ########
####################################

There are several ways to get total performance over a desired time range:

WRDS CRSP    
(1) Use (1+ret) * prev_closing_price for each day in the period
(2) Use (last_day_prc / last_day_cfacpr) / (first_day_prc / first_day_cfacpr)

WRDS Datastream


Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io 
'''
import pandas as pd
import wrds
import sqlite3

# Global parameters
username = 'astahl3'
path_stockinfo_db_write = \
    "/Users/astahl/fin_nlp_data/securities/stockinfo.db"

def calculate_cusip_check_digit(cusip):
    weights = [1, 2] * 4
    total = 0
    for i, char in enumerate(cusip):
        if char.isdigit():
            value = int(char)
        else:
            value = ord(char) - 55  # Convert to numbers (A=10, B=11, ...)
        total += value * weights[i]
    check_digit = (10 - (total % 10)) % 10
    return str(check_digit)

def cusip_to_cusip9(cusip):
    check_digit = calculate_cusip_check_digit(cusip)
    return cusip + check_digit

def cusip9_to_isin(cusip9, country_code='US'):
    base_isin = country_code + cusip9
    weights = [1 if i % 2 == 0 else 2 for i in range(len(base_isin))]
    total = 0
    for i, char in enumerate(base_isin):
        if char.isdigit():
            value = int(char)
        else:
            value = ord(char) - 55  # Convert letters to numbers (A=10, B=11, ..., Z=35)
        total += sum(divmod(value * weights[i], 10))
    isin_check_digit = (10 - (total % 10)) % 10
    return base_isin + str(isin_check_digit)

def isin_to_components(isin):
    '''
    Split ISIN ('isin') into components and return the two-digit country code,
    CUSIP, CUSIP check digit, and ISIN check digit
    '''
    if len(isin) != 12: raise ValueError("ISIN must be 12 characters long")
    return isin[0:2], isin[2:10], isin[10], isin[11]
    
def isin_to_cusip(isin):
    if len(isin) != 12: raise ValueError("ISIN must be 12 characters long")
    return isin[2:10]
    
def get_crsp_stockinfo(ticker, start_date=None, end_date=None):
    
    query = f'''
            SELECT *
            FROM crsp_q_stock.stocknames_v2
            WHERE ticker = \'{ticker}\'
            AND namedt <= \'{start_date}\'
            AND nameenddt >= \'{end_date}\'
            '''
            
    return(db.raw_sql(query))
    
def get_ds_stockinfo(ticker, start_date=None, end_date=None):
    
    if start_date is None and end_date is None:
        query = f"""
                SELECT *
                FROM tr_ds_equities.wrds_ds_names
                WHERE ibesticker = '@:{ticker}'
                """
    elif start_date is None:
        query = f"""
                SELECT *
                FROM tr_ds_equities.wrds_ds_names
                WHERE ibesticker = '@:{ticker}'
                AND enddate >= '{end_date}'
                """
    elif end_date is None:
        query = f"""
                SELECT *
                FROM tr_ds_equities.wrds_ds_names
                WHERE ibesticker = '@:{ticker}'
                AND startdate <= '{start_date}'
                """
    else:
        query = f"""
                SELECT *
                FROM tr_ds_equities.wrds_ds_names
                WHERE ibesticker = '@:{ticker}'
                AND startdate <= '{start_date}'
                AND enddate >= '{end_date}'
                """
    return(db.raw_sql(query))
    
def get_stock_prices_from_ticker(ticker, start_date=None, end_date=None):
    # SAMPLE QUERY
    ''' r = db.raw_sql("SELECT date, cusip, prc 
                        FROM crsp_a_stock.dsf 
                        WHERE cusip='83001A10' 
                        AND startdate >= '{start_date}' 
                        AND enddate <= '{end_date}'")
    '''
    
    # Get permno for price query
    #stockinfo = get_permno_from_ticker(ticker, start_date, end_date)
    
    
# Generate table of all US-based securities with market capitalizations above
# the desired minimum threshold, including Datastream and CRSP id fields

# Query parameters
marketdate = '2024-05-01'
mktcap_cutoff = 100000000000 # $100 million
region = 'US'

# Database write location
path_wrds_db_write = \
    "/Users/astahl/fin_nlp_data/wrds/stocks_over_1bln_2024-05-01.db"

# Establish connection
db = wrds.Connection(wrds_username=username)

# Example query sequence
'''
sample_ticker = 'MSFT'
sample_ds_names = db.raw_sql(f"""SELECT * 
                               FROM tr_ds_equities.wrds_ds_names 
                               WHERE ibesticker='@:{sample_ticker}'
                               """)
sample_dscode = sample_ds_names['dscode'].values[0]                                  
sample_isin = sample_ds_names['isin'].values[0]
sample_cusip = isin_to_cusip(sample_isin)
sample_ds_names['cusip'] = sample_ds_names['isin'].apply(isin_to_cusip)

sample_crsp = db.raw_sql(f"""SELECT * 
                             FROM crsp_q_stock.stocknames_v2
                             WHERE cusip = '{sample_cusip}'
                             """)
                             
combined_df = pd.merge(sample_ds_names, sample_crsp, on='cusip', how='inner')
final_df = combined_df[['dscode', 'ticker', 'cusip', 'ismajorsec', 'primeexchmnem', 'startdate', 'enddate', 'permno', 'permco']]



# Not required for stock identification purposes
sample_ds_ds2dsf = db.raw_sql(f"""SELECT * 
                                  FROM tr_ds_equities.wrds_ds2dsf
                                  WHERE dscode='{sample_dscode}'
                                  """)
                                  
samp_ds_names = db.raw_sql(f"SELECT * FROM tr_ds_equities.wrds_ds_names WHERE ibesticker='@:{sample_ticker}'")
samp_crsp = db.raw_sql("SELECT * FROM crsp_q_stock.stocknames_v2 WHERE ticker='{sample_ticker}'")
'''

# Run datastream query to obtain dscode of stocks from wrds_ds2dsf table
query = f""" 
        SELECT  dscode, currency, mktcap, mktcap_usd, region
        FROM    tr_ds_equities.wrds_ds2dsf
        WHERE   marketdate = '{marketdate}'
        AND     region = '{region}'
        AND     mktcap >= '{mktcap_cutoff}'
        ORDER BY mktcap DESC
        """
ds2dsf_df = db.raw_sql(query)

# Using each stock's dscode, query ticker and metainfo via wrds_ds_names table
dscode_list = ds2dsf_df['dscode'].tolist()
dscode_str = "','".join(dscode_list)


query = f""" 
        SELECT  dscode, dsqtname, delistdate, dscmpyname, ismajorsec,
                dssecname, primexchmnem, ibesticker, cmpyctrycode, startdate,
                enddate, ticker, isin
        FROM    tr_ds_equities.wrds_ds_names
        WHERE   dscode IN ('{dscode_str}')
        """
dsnames_df = db.raw_sql(query)

# Get list of CUSIPs 
dsnames_df['cusip'] = dsnames_df['isin'].apply(isin_to_cusip)
cusip_list = dsnames_df['cusip'].tolist()
cusip_str = "','".join(cusip_list)

query = f"""
        SELECT  cusip, cusip9, issuernm, permno, permco, securitybegdt, 
                securityenddt, ticker, sharetype, tradingstatusflg
        FROM    crsp_q_stock.stocknames_v2
        WHERE   cusip IN ('{cusip_str}')
        """
crsp_df = db.raw_sql(query)

# Merge the three tables together, generate final table, save as SQlite db
merged_df_O = pd.merge(ds2dsf_df, dsnames_df, on='dscode', how='outer')
merged_df_I = pd.merge(ds2dsf_df, dsnames_df, on='dscode', how='inner')

totalmerge_df_O = pd.merge(merged_df_O, crsp_df, on='cusip', how='outer')
totalmerge_df_I = pd.merge(merged_df_I, crsp_df, on='cusip', how='inner')
"""
# Generate SQlite security name databases 
# Connect to SQLite database
conn = sqlite3.connect(path_reddit_db_write)
c = conn.cursor()

# Create submissions table if it doesn't already exist
c.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
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
            match_type TEXT,
            is_DD BOOLEAN
            )''')

# Write submissions to database
insert_query = f'''INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

for submission in submissions:
    if submission:  # Check if submission is not None
        c.execute(insert_query, submission)

# Close connection until next batch
conn.commit()
conn.close()


'''
ticker = 'MSFT'
marketdate = '2024-05-22'
stockinfo = get_ds_stockinfo(ticker)
dscode = stockinfo['dscode'].values[0]
# Query for all US-based companies with market caps over required minimum
query = f'''SELECT *
            FROM tr_ds_equities.wrds_ds2dsf
            WHERE dscode = '{dscode}'
            AND marketdate = '{marketdate}'
        '''
queryresult = db.raw_sql(query)
        
ticker = 'FB'
start_date = '2022-01-01'
end_date = '2022-01-31'
stockinfo = get_ds_stockinfo(ticker, start_date, end_date)
stockinfo_crsp = get_crsp_stockinfo(ticker, start_date, end_date)
'''
db.close()

