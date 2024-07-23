'''
Create a performance table for the reddit submission database which provides
returns 



'''
import wrds
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import crsp_performance_db as cpd

############################## GLOBAL PARAMETERS ##############################

# File paths for local databases
path_root = "/Users/astahl/fin_nlp_data"
path_submissions_db = path_root + "/sqlite/reddit/submissions.db"
path_returns_db = path_root + "/sqlite/wrds/stock_performance.db"
path_logfile_write = path_root + "/reddit/logfiles/logfile"

# Database table names
submissions_table = "single_ticker_matches"
security_table = "crsp_securities"
returns_table = "crsp_returns"
performance_table = "crsp_performance"

# WRDS username
username = 'astahl3'

# Latest available date ("lad") for CRSP stock data
lad = datetime(year=2024, month=3, day=28).date()
lad_str = lad.strftime('%Y-%m-%d')

###############################################################################

def main():
    global path_logfile_write

    # Open logfile to print header information
    current_date = datetime.date(datetime.now()).strftime('%Y-%m-%d')
    path_logfile_write = path_logfile_write + '_' + current_date + '.txt'
    with open(path_logfile_write, 'a') as lf:
        lf.write('\n')
        current_time = datetime.now()
        formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        lf.write(f"********** Starting log: {formatted_time}\n")
        lf.write("********** Running program: gen_submission_perf.py\n")
        lf.write(f"********** Destination: {path_submissions_db}\n")
        lf.write(f"********** Table read name: {returns_table}\n")
        lf.write(f"********** Table write name: {performance_table}\n\n")

    # Load in the submissions database and store as a dataframe
    conn_subs = sqlite3.connect(path_submissions_db)
    df = pd.read_sql_query(f"SELECT * FROM {submissions_table}", conn_subs)
    
    # Connect to the local returns database
    conn_crsp = sqlite3.connect(path_returns_db)

    # Prepare to write new table in the submissions db
    c = conn_subs.cursor()
    c.execute(f"""
              CREATE TABLE IF NOT EXISTS {performance_table} (
                  post_id TEXT PRIMARY KEY,
                  ticker TEXT,
                  permno TEXT,
                  return_1mo REAL,
                  return_2mo REAL,
                  return_3mo REAL,
                  return_6mo REAL,
                  return_12mo REAL,
                  date_added DATE)
              """)    
    conn_subs.commit()

    # Connect to WRDS for security metainfo as of post dates
    db = wrds.Connection(wrds_username=username)

    # Iterate over submissions dataframe to populate new table
    count = 0
    for index, row in df.iterrows():
        '''
        Obtain the appropriate permno given the post date, then get the
        performance history from the local CRSP returns database and populate
        the performance table.
        '''
        
        ticker = row['company_match']
        post_id = row['id']
        post_date = datetime.utcfromtimestamp(row['created_utc'])
        post_date_str = post_date.strftime('%Y-%m-%d')
        market_date_dt = cpd.get_nearest_market_date(post_date)
        market_date = market_date_dt.strftime('%Y-%m-%d')
        
        # CRSP query for security metainfo as of post date
        sql_query = f"""
                    SELECT * 
                    FROM crsp_q_stock.stocknames_v2
                    WHERE ticker = '{ticker}'
                    AND namedt <= '{market_date}'
                    AND nameenddt >= '{market_date}'
                    """
        stockinfo_crsp = db.raw_sql(sql_query)
        
        # Skip to next entry if query result is empty; no result implies the
        # ticker is probably invalid or not traded on a conventional exchange
        if stockinfo_crsp.empty:
            print(f"stockinfo_crsp is empty for: {ticker}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("No ticker match in CRSP:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"Post Date = {market_date}\n")
                lf.write(f"Submission ID = {row['id']}\n\n")
            continue

        # TODO: Logic for if the ticker query contains more than one row
        # For now, just select the last row of the query result, as each row 
        # likely contains the same permno given the query's date restriction
        if stockinfo_crsp.shape[0] > 1:
            print(f"stockinfo_crsp has more than one entry for: {ticker}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("More than one ticker match in CRSP:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"Post Date = {market_date}\n")
                lf.write(f"Submission ID = {row['id']}\n\n")
            #stockinfo_crsp = stockinfo_crsp.tail(1).reset_index(drop=True)
            stockinfo_crsp = stockinfo_crsp.tail(1)

        permno_temp = str(stockinfo_crsp['permno'].iloc[0])
        
        # If permno already up-to-date in table, skip to next submission
        local_query = f"""
                        SELECT date_added
                        FROM {performance_table}
                        WHERE post_id = '{post_id}'
                        """
        c.execute(local_query)
        result = c.fetchone()
        
        # Logic sequence for if permno is already populated in performance db
        if result is not None:
            print(f"Performance for post_id = {post_id} already exists")
            lad_temp = datetime.strptime(result[0], '%Y-%m-%d').date()
            with open(path_logfile_write, 'a') as lf:
                lf.write(f"Perf for post_id = {post_id} already exists\n")
                lf.write(f"Last updated on {lad_temp}\n\n")
            
                # If the last time security info was updated was after the 
                # current desired end date range, then don't update security
                if lad_temp >= lad:
                    continue
                
        # Calculate returns starting from the post date for desired ranges
        date_offsets = [30, 60, 90, 182, 365]
        dates_dt = [market_date_dt + timedelta(dt) for dt in date_offsets]
        mdates_dt = [cpd.get_nearest_market_date(d) for d in dates_dt]
        mdates_str = [d.strftime('%Y-%m-%d') for d in mdates_dt]

        ret_dict = {}
        for i, end_date in enumerate(mdates_str):
            perf_query = f"""
                        SELECT ret
                        FROM {returns_table}
                        WHERE permno = '{permno_temp}'
                        AND mkt_date > '{market_date}'
                        AND mkt_date <= '{end_date}'
                        """
            df_temp = pd.read_sql_query(perf_query, conn_crsp)
            
            # Calculate total return over the current period
            totret = 1
            for _, row_temp in df_temp.iterrows():
                ret = row_temp['ret'] if row_temp['ret'] is not None else 0.0
                totret *= (1 + ret)
                
            ret_dict[date_offsets[i]] = totret - 1

        # Insert calculated returns into the performance table
        c.execute(f"""
            INSERT OR REPLACE INTO {performance_table} 
            (post_id, ticker, permno, return_1mo, return_2mo, 
             return_3mo, return_6mo, return_12mo, date_added)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            (post_id, ticker, permno_temp, ret_dict.get(30), ret_dict.get(60), 
             ret_dict.get(90), ret_dict.get(182), ret_dict.get(365), lad_str))
    
        print(f"Populated database for post_id = {post_id}")
        conn_subs.commit()
        count += 1
        if count % 10 == 0: print(f"Submission returns entered: {count}")

    conn_subs.close()
    conn_crsp.close()

if __name__ == "__main__":
    main()