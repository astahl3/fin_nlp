'''
Functions for querying stock histories via the wrds module, which interfaces
with the Postgre SQL database managed by Wharton Research Data Services (WRDS).
Intended for use with the fin_nlp project, which seeks to assess the merit
of investment-related ideas expressed on network platforms (e.g., reddit, 
twitter|X, discord). 

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

# Global parameters
username = 'astahl3'

def get_crsp_stockinfo(db, ticker, start_date=None, end_date=None):
    
    query = f'''
            SELECT *
            FROM crsp_q_stock.stocknames_v2
            WHERE ticker = \'{ticker}\'
            AND namedt <= \'{start_date}\'
            AND nameenddt >= \'{end_date}\'
            '''
            
    return(db.raw_sql(query))
    
def get_ds_stockinfo(db, ticker, start_date=None, end_date=None):
    
    query = f"""
            SELECT *
            FROM tr_ds_equities.wrds_ds_names
            WHERE ibesticker = '@:{ticker}'
            AND startdate <= '{start_date}'
            AND enddate >= '{end_date}'
            """
    
    return(db.raw_sql(query))
    
def get_stock_prices_from_ticker(db, ticker, start_date=None, end_date=None):
    # SAMPLE QUERY
    ''' r = db.raw_sql("SELECT date, cusip, prc 
                        FROM crsp_a_stock.dsf 
                        WHERE cusip='83001A10' 
                        AND startdate >= '{start_date}' 
                        AND enddate <= '{end_date}'")
    '''
    
    # Get permno for price query
    #stockinfo = get_permno_from_ticker(ticker, start_date, end_date)
    
def main():    

    # Sample query for security metainfo and dscode
    db = wrds.Connection(wrds_username=username)
    ticker = 'FB'
    start_date = '2022-01-01'
    end_date = '2022-01-31'
    stockinfo = get_ds_stockinfo(db, ticker, start_date, end_date)
    stockinfo_crsp = get_crsp_stockinfo(db, ticker, start_date, end_date)
    db.close()

if __name__ == "__main__":
    main()