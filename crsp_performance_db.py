'''
This program creates a database of US stock performance for securities that
appear in posts submitted across investing-related subreddits. Performance
history is queried from the Wharton Research Data Services (WRDS) API using
the CRSP security tables (crsp_q_stock: dsf, stocknames_v2).

Resulting database is part of the larger fin_nlp project, where the merit
of investment or trade ideas communicated on social media are assessed.

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io 
'''
import pandas as pd
import pandas_market_calendars as mcal
import wrds
import sqlite3
from datetime import datetime, timedelta

# Global parameters
username = 'astahl3'

# Desired table in local submission database
submissions_table = "single_ticker_matches"
security_table = "crsp_securities"
performance_table = "crsp_returns"
 
# Read and write paths for databases
path_submissions_db_write = \
    "/Users/astahl/fin_nlp_data/sqlite/wrds/stock_performance.db"
path_submissions_db_read = \
    "/Users/astahl/fin_nlp_data/sqlite/reddit/submissions.db"
path_logfile_write = \
    "/Users/astahl/fin_nlp_data/reddit/logfiles/logfile"


# Start date for performance and return data
start_dt = datetime(year=2012, month=1, day=1).strftime('%Y-%m-%d')

# Latest available date ("lad") for CRSP stock data
lad = datetime(year=2024, month=3, day=28).date()
lad_str = lad.strftime('%Y-%m-%d')

def get_current_ticker(db, permno):
    '''
    Return the current ticker for given permno
    '''
    query = f"""
            SELECT *
            FROM crsp_q_stock.stocknames_v2
            WHERE permno = '{permno}'
            AND namedt <= '{lad_str}'
            AND nameenddt >= '{lad_str}'
            """
    
    nameinfo = db.raw_sql(query)
    
    # If no current ticker for permno, security no longer trading, use NA
    if nameinfo.empty: return 'NONE'
    
    # Select last entry if more than one row in query result
    if nameinfo.shape[0] > 1: nameinfo = nameinfo.tail(1)

    return nameinfo['ticker'].iloc[0]    
    
def cusip9_to_isin(cusip9, country_code='US'):
    '''
    Calculate the check digit for the security ISIN using the passed 'cusip9'
    and (optional) 'country_code' via the Luhn algorithm and return the result
    '''

    base_isin = country_code + cusip9

    # Convert letters to their ASCII numbers minus 55 (A = 10, B = 11, ...)
    check_str = ''.join(str(int(char) if char.isdigit() else ord(char) - 55)
                        for char in base_isin)

    # Split into groups, with 1st, 3rd, 5th, ... digits to left group, and the
    # 2nd, 4th, 6th, ... digits to right group. Then double the values in the
    # group with the last digit and sum digits in each group
    left_group = [int(check_str[i]) for i in range(0, len(check_str), 2)]
    right_group = [int(check_str[i]) for i in range(1, len(check_str), 2)]

    if len(check_str) % 2 == 0:
        doubled_group = [sum(divmod(d * 2, 10)) for d in right_group]
        unchanged_group = left_group
    else:
        # Odd length: double the left group
        doubled_group = [sum(divmod(d * 2, 10)) for d in left_group]
        unchanged_group = right_group

    # Sum up digits in both groups, subtract modulus 10 of result for digit
    totsum = sum(doubled_group) + sum(unchanged_group)
    check_digit = (10 - (totsum % 10)) % 10
    return base_isin + str(check_digit)

def get_nearest_market_date(dt):
    """
    Returns the nearest market date given the passed datetime 'dt' using the
    New York Stock Exchange (NYSE) market calendar
    """
    # Load the NYSE calendar
    nyse = mcal.get_calendar('NYSE')

    date_str = dt.strftime('%Y-%m-%d')

    # Get the market schedule for a range around the date
    start_range = (dt - timedelta(days=10)).strftime('%Y-%m-%d')
    end_range = (dt + timedelta(days=10)).strftime('%Y-%m-%d')
    schedule = nyse.schedule(start_date=start_range, end_date=end_range)
    schedule['market_date'] = schedule['market_open'].dt.date

    # Check if the given date is a market day
    if date_str in schedule['market_date']:
        return pd.Timestamp(dt)

    # If given date is not a market day, return next market date if available
    future_schedule = schedule[schedule.index > dt]
    if future_schedule.empty:
        return pd.Timestamp(schedule.index[-1])
    else:
        return pd.Timestamp(future_schedule.index[0])

def are_returns_populated(cn, permno, start_date, end_date):
    '''
    Checks if there is a return in the desired time range; returns true
    if there is a single return within the range
    '''
    
    query = f"""
            SELECT COUNT(*) FROM {performance_table}
            WHERE permno = ? and mkt_date BETWEEN ? AND ?
            """
    c_temp = cn.cursor()
    c_temp.execute(query, (permno, start_date, end_date))
    ret_row = c_temp.fetchone()[0]
    return ret_row > 0

def is_stockinfo_updated(cn, permno, desired_update_dt):
    '''
    Checks if the lastupd field for the security info matches the end date
    '''
    
    query = f""" SELECT lastupd FROM {security_table} WHERE permno = ? """
    c_temp = cn.cursor()
    c_temp.execute(query, (permno))
    
    
    
    return c_temp.fetchone()

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
        lf.write("********** Running program: crsp_performance_db.py\n")
        lf.write(f"********** Destination: {path_submissions_db_write}\n")
        lf.write(f"********** Table read name: {submissions_table}\n")
        lf.write(f"********** Table write name: {performance_table}\n\n")

    # Load in the desired submissions database and store as a dataframe
    conn = sqlite3.connect(path_submissions_db_read)
    df = pd.read_sql_query(f"SELECT * FROM {submissions_table}", conn)
    conn.close()

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=username)

    # Prepare to write to the SQlite security and performance tables
    conn_out = sqlite3.connect(path_submissions_db_write)
    c = conn_out.cursor()
    c.execute(f"""
              CREATE TABLE IF NOT EXISTS {security_table} (
                  permno TEXT PRIMARY KEY,
                  cusip9 TEXT,
                  ticker TEXT,
                  issuernm TEXT,
                  primaryexch TEXT,
                  securitytype TEXT,
                  securitysubtype TEXT,
                  lastupd DATE)
              """)
    
    c.execute(f"""
              CREATE TABLE IF NOT EXISTS {performance_table} (
                  permno TEXT,
                  hsiccd INTEGER,
                  mkt_date DATE,
                  numtrd REAL,
                  ret REAL,
                  shrout REAL,
                  vol REAL,
                  FOREIGN KEY (permno) REFERENCES {security_table}(permno),
                  PRIMARY KEY (permno, mkt_date))
              """)              
    conn_out.commit()

    # Iterate over submissions dataframe to populate new table
    count = 0
    for index, row in df.iterrows():
        ticker = row['company_match']
        post_date = datetime.utcfromtimestamp(row['created_utc'])
        market_date_dt = get_nearest_market_date(post_date)
        market_date = market_date_dt.strftime('%Y-%m-%d')
        update_info = True
        
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

        crsp_permno = str(stockinfo_crsp['permno'].iloc[0])
        
        # If permno already up-to-date in table, skip to next submission
        local_query = f"""
                        SELECT lastupd
                        FROM {security_table}
                        WHERE permno = '{crsp_permno}'
                        """
        c.execute(local_query)
        result = c.fetchone()
        
        # Logic sequence for if permno is already populated in security db
        if result is not None:
            print(f"Stockinfo for permno = {crsp_permno} already exists")

            lastupd_temp = datetime.strptime(result[0], '%Y-%m-%d').date()
            with open(path_logfile_write, 'a') as lf:
                lf.write(f"Info for permno = {crsp_permno} already exists\n")
                lf.write(f"Last updated on {lastupd_temp}\n\n")
            
                # If the last time security info was updated was after the 
                # current desired end date range, then don't update security
                if lastupd_temp >= lad:
                    update_info = False
                    
        # Use transaction to ensure atomicity
        with conn_out:            
        
            # UPDATE SECURITY TABLE
            if update_info: 
                crsp_cusip9 = stockinfo_crsp['cusip9'].iloc[0]
                crsp_primaryexch = stockinfo_crsp['primaryexch'].iloc[0]
                crsp_securitytype = stockinfo_crsp['securitytype'].iloc[0]
                crsp_securitysubtype = stockinfo_crsp['securitysubtype'].iloc[0]
                crsp_issuernm = stockinfo_crsp['issuernm'].iloc[0]
                crsp_currticker = get_current_ticker(db, crsp_permno)        
    
            
                # Commit security info to SQlite table
                insert_text = f"""
                            INSERT INTO {security_table} 
                            (permno, cusip9, ticker, issuernm, primaryexch, 
                             securitytype, securitysubtype, lastupd)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(permno) DO UPDATE SET
                                cusip9 = excluded.cusip9,
                                ticker = excluded.ticker,
                                issuernm = excluded.issuernm,
                                primaryexch = excluded.primaryexch,
                                securitytype = excluded.securitytype,
                                securitysubtype = excluded.securitysubtype,
                                lastupd = excluded.lastupd
                                """
                c.execute(insert_text, 
                          (crsp_permno, crsp_cusip9, crsp_currticker, 
                           crsp_issuernm, crsp_primaryexch, crsp_securitytype, 
                           crsp_securitysubtype, lad))
                
                conn_out.commit()
                
                # Print security write info
                print(f"Wrote stockinfo for permno = {crsp_permno} to db")
                with open(path_logfile_write, 'a') as lf:
                    lf.write(f"Wrote stockinfo for permno = {crsp_permno}:\n")
                    lf.write(f"Updated through: {lad}\n")
                    lf.write(f"Ticker = {ticker}\n")
                    lf.write(f"Updated ticker = {crsp_currticker}\n")
                    lf.write(f"Post Date: {market_date}\n")
                    lf.write(f"Submission ID: {row['id']}\n\n")
            
            # Security info is already up-to-date, don't write to security db              
            else:   
                print(f"Skipped stockinfo for permno = {crsp_permno}:")
                with open(path_logfile_write, 'a') as lf:
                    lf.write(f"Already updated through: {lastupd_temp}\n")
                    lf.write(f"Ticker = {ticker}\n")
                    lf.write(f"Post Date: {market_date}\n")
                    lf.write(f"Submission ID: {row['id']}\n\n")

            # UPDATE PERFORMANCE TABLE
            print(f"Checking if returns for {crsp_permno} are populated...")
            
            # If at least one performance row exists in range, skip iteration
            if are_returns_populated(conn_out, crsp_permno, start_dt, lad_str):
                print(f"Returns for {crsp_permno} already in db, skipping...")
                with open(path_logfile_write, 'a') as lf:
                    lf.write(f"Skipping {crsp_permno}, already in perf db\n\n")
                continue
    
            else:
                
                # Get all returns from start date through current date
                sql_query = f"""
                        SELECT date, hsiccd, numtrd, ret, shrout, vol
                        FROM crsp_q_stock.dsf
                        WHERE permno = '{crsp_permno}'
                        AND date >= '{start_dt}'
                        AND date <= '{lad}'
                        """
            
                returninfo = db.raw_sql(sql_query)
                print(f"Pulled return info for {ticker} on {market_date}")
                
                # Update database with returns
                insert_text = f"""
                                INSERT OR REPLACE INTO {performance_table}
                                (permno, hsiccd, mkt_date, numtrd, ret,
                                 shrout, vol)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """
            
                for idx, r in returninfo.iterrows():
                    c.execute(insert_text, (crsp_permno, r['hsiccd'], r['date'], 
                                            r['numtrd'], r['ret'], r['shrout'], 
                                            r['vol']))    
                
                conn_out.commit()
    
            # Print write info
            print(f"Wrote returns for {ticker} starting on {market_date}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("Wrote returns to database:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"Updated ticker = {crsp_currticker}\n")
                lf.write(f"Post Date: {market_date}\n")
                lf.write(f"Submission ID: {row['id']}\n\n")
            
        count += 1
        print(f"count = {count}")
        #if count == 100:
        #    break

    conn_out.close()



if __name__ == "__main__":
    main()
