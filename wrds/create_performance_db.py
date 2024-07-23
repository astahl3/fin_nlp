'''
Create stock performance database from a desired database of social media posts
using Datastream and CRSP via the Wharton Research Data Services (WRDS) API
platform. 

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
return_table = "stock_performance"

# Read and write paths for databases
path_submissions_db_write = \
    "/Users/astahl/fin_nlp_data/sqlite/reddit/submissions.db"
path_submissions_db_read = \
    "/Users/astahl/fin_nlp_data/sqlite/reddit/submissions.db"
path_logfile_write = \
    "/Users/astahl/fin_nlp_data/reddit/logfiles/logfile"


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
    '''
    Returns the nearest market date given the passed datetime 'dt' using the
    New York Stock Exchange (NYSE) market calendar
    '''
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

def are_returns_populated(conn, permno, start_date, end_date):
    '''
    Checks if there is a return in the desired time range; returns true
    if there is a single return within the range
    '''
    
    query = f"""
            SELECT COUNT(*) FROM {return_table}
            WHERE permno = ? and mkd_date BETWEEN ? AND ?
            """
    c_temp = conn.cursor()
    c_temp.execute(query, (permno, start_date, end_date))
    ret_row = c_temp.fetchone()[0]
    return ret_row > 0

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
        lf.write("********** Running program: create_performance_db.py\n")
        lf.write(f"********** Destination: {path_submissions_db_write}\n")
        lf.write(f"********** Table read name: {submissions_table}\n")
        lf.write(f"********** Table write name: {return_table}\n\n")

    # Load in the desired submissions database and store as a dataframe
    conn = sqlite3.connect(path_submissions_db_read)
    df = pd.read_sql_query(f"SELECT * FROM {submissions_table}", conn)
    conn.close()

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=username)

    # Prepare to write to the SQlite database
    conn_out = sqlite3.connect(path_submissions_db_write)
    c = conn_out.cursor()
    c.execute(f"""
              CREATE TABLE IF NOT EXISTS {return_table} (
                  id TEXT PRIMARY KEY,
                  created_utc TEXT,
                  market_date DATE,
                  ticker TEXT,
                  ticker_final TEXT,
                  dscode TEXT,
                  ismajorsec TEXT,
                  dsname TEXT,
                  dsqtname TEXT,
                  exchange TEXT,
                  ret_1m REAL,
                  ret_3m REAL,
                  ret_1y REAL)
              """)
    conn_out.commit()

    # Iterate over submissions dataframe to populate new table
    for index, row in df.iterrows():
        ticker = row['company_match']
        post_date = datetime.utcfromtimestamp(row['created_utc'])
        market_date_dt = get_nearest_market_date(post_date)
        market_date = market_date_dt.strftime('%Y-%m-%d')
        market_date_day = datetime.date(market_date_dt)

        # Skip row if this entry is already populated with returns
        local_query = f"""
                        SELECT ret_1m, ret_3m, ret_1y
                        FROM '{return_table}'
                        WHERE id = '{row['id']}'
                        """
        c.execute(local_query)
        result = c.fetchone()
        if result is not None and all(result):
            print(f"Returns for {ticker} is already populated")
            continue

        # CRSP query
        sql_query = f"""
                    SELECT * 
                    FROM crsp_q_stock.stocknames_v2
                    WHERE ticker = '{ticker}'
                    AND namedt <= '{market_date}'
                    AND nameenddt >= '{market_date}'
                    """
        stockinfo_crsp = db.raw_sql(sql_query)

        # Skip to next entry if query result is empty; no result implies that
        # the ticker is likely invalid 
        if stockinfo_crsp.empty:
            print(f"stockinfo_crsp is empty for: {ticker}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("No ticker match in CRSP:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"Post Date = {market_date}\n")
                lf.write(f"Submission ID = {row['id']}\n\n")
            continue

        #  Skip to next entry if query result contains more than one row
        if stockinfo_crsp.shape[0] > 1:
            print(f"stockinfo_crsp has more than one entry for: {ticker}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("More than one ticker match in CRSP:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"Post Date = {market_date}\n")
                lf.write(f"Submission ID = {row['id']}\n\n")
            continue

        crsp_cusip9 = stockinfo_crsp['cusip9'][0]
        crsp_isin = cusip9_to_isin(crsp_cusip9)

        # Get dscode for stock returns from Datastream
        sql_query = f"""
                    SELECT *
                    FROM tr_ds_equities.wrds_ds_names
                    WHERE isin = '{crsp_isin}'
                    """
        print(f"current isin = {crsp_isin}")
        stockinfo_ds = db.raw_sql(sql_query)

        # Skip to next entry if query result is empty
        if stockinfo_ds.empty:
            print(f"stockinfo_ds is empty for: {crsp_isin}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("No match in Datastream: wrds_ds_names\n")
                lf.write(f"Ticker: {ticker}\n")
                lf.write(f"Post Date: {market_date}\n")
                lf.write(f"Submission ID: {row['id']}\n\n")
            continue

        #  Skip to next entry if query result contains more than one row
        if stockinfo_ds.shape[0] > 1:
            print(f"stockinfo_ds has more than one entry for: {ticker}")
            with open(path_logfile_write, 'a') as lf:
                lf.write("More than one ticker match in Datastream:\n")
                lf.write(f"Ticker = {ticker}\n")
                lf.write(f"ISIN = {crsp_isin}\n")
                lf.write(f"Post Date = {market_date}\n")
                lf.write(f"Submission ID = {row['id']}\n\n")
            continue

        dscode = stockinfo_ds['dscode'][0]
        ismajorsec = stockinfo_ds['ismajorsec'][0]
        dsname = stockinfo_ds['dscmpyname'][0]
        dsqtname = stockinfo_ds['dsqtname'][0]
        exchange = stockinfo_ds['primexchmnem'][0]
        ticker_upd = stockinfo_ds['ticker'][0]

        if ticker_upd is None:
            ticker_upd = stockinfo_ds['ibesticker'][0]
            if ticker_upd is None:
                ticker_upd = 'None'

        ticker_upd = ticker_upd.replace('@', '')
        ticker_upd = ticker_upd.replace(':', '')
        ticker_upd = ticker_upd.replace(' ', '')

        # Get 1 month, 3 months, 1 year returns
        date_offsets = [30, 90, 365]
        dates_dt = [market_date_dt + timedelta(dt) for dt in date_offsets]
        mdates_dt = [get_nearest_market_date(d) for d in dates_dt]
        mdates_str = [d.strftime('%Y-%m-%d') for d in mdates_dt]
        mdates_str.insert(0, market_date)

        '''
        date_1m = market_date_dt + timedelta(days=30)
        date_3m = market_date_dt + timedelta(days=90)
        date_1y = market_date_dt + timedelta(days=365)
        
        md_1m = get_nearest_market_date(date_1m).date() 
        md_3m = get_nearest_market_date(date_3m).date() 
        md_1y = get_nearest_market_date(date_1y).date() 
        
        md_1m_str = md_1m.strftime('%Y-%m-%d')
        md_3m_str = md_3m.strftime('%Y-%m-%d')
        md_1y_str = md_1y.strftime('%Y-%m-%d')
        
        
        desired_dates = "', '".join([market_date, md_1m_str, 
                                     md_3m_str, md_1y_str])
        
        ri_curr = returninfo.loc[returninfo['marketdate']
                     == market_date_day]['ri'].values[0]
        ri_1m = returninfo.loc[returninfo['marketdate'] 
                     == md_1m]['ri'].values[0]
        ri_3m = returninfo.loc[returninfo['marketdate']
                     == md_3m]['ri'].values[0]
        ri_1y = returninfo.loc[returninfo['marketdate']
                     == md_1y]['ri'].values[0]
        
        ret_1m = ri_1m/ri_curr - 1
        ret_3m = ri_3m/ri_curr - 1
        ret_1y = ri_1y/ri_curr - 1
        '''

        # Retrieve return indices over desired date range
        sql_query = f"""
                    SELECT ri, ri_usd, marketdate, close
                    FROM tr_ds_equities.wrds_ds2dsf
                    AND marketdate >= '{market_date}'
                    AND marketdate <= '{mdates_str[-1]}'
                    """

        # Treat each date individually; returns may only exist for some ranges
        retidx = []
        for k in range(0, len(mdates_str)):

            temp_date = mdates_str[k]

            # Fetch performance data
            print(f"Fetching data for {ticker} on {temp_date}...")
            sql_query = f"""
                        SELECT ri, ri_usd, marketdate, close
                        FROM tr_ds_equities.wrds_ds2dsf
                        WHERE dscode = '{dscode}'
                        AND marketdate = '{temp_date}'
                        """

            returninfo = db.raw_sql(sql_query)

            # Skip to next entry if stock return query result is empty
            if returninfo.empty:
                print(f"Return info is empty for {ticker} at offset {k}")
                with open(path_logfile_write, 'a') as lf:
                    lf.write("No match in Datastream: wrds_ds2dsf\n")
                    lf.write(f"Ticker = {ticker}\n")
                    lf.write(f"Post Date = {market_date}\n")
                    lf.write(f"Submission ID = {row['id']}\n\n")

                # Check last date available
                sql_query = f"""
                            SELECT ri, ri_usd, marketdate, close
                            FROM tr_ds_equities.wrds_ds2dsf
                            WHERE dscode = '{dscode}'
                            AND marketdate <= '{temp_date}'
                            AND marketdate >= '{mdates_str[0]}'
                            ORDER BY marketdate DESC
                            LIMIT 1
                            """
                returninfo = db.raw_sql(sql_query)
                if returninfo.empty: ret_temp='Error'
                else: ret_temp = returninfo['ri'][0]
             
                for i in range(k, len(mdates_str)):
                    retidx.append(ret_temp)
                    with open(path_logfile_write, 'a') as lf:
                        lf.write("Incomplete return data:\n")
                        lf.write(f"Ticker = {ticker}\n")
                        lf.write(f"Post Date = {market_date}\n")
                        last_date = returninfo['marketdate'][0]
                        lf.write(f"Last Available Date (LAD) = {last_date}\n")
                        lf.write(f"Entering LAD for offset {i}\n\n")
                continue

            retidx.append(returninfo['ri'][0])

        # Calculate returns
        if retidx[1] is None: ret_1m = 'Error'
        if retidx[2] is None: ret_3m = 'Error'
        if retidx[3] is None: ret_1y = 'Error'
        ret_1m = retidx[1]/retidx[0] - 1
        ret_3m = retidx[2]/retidx[0] - 1
        ret_1y = retidx[3]/retidx[0] - 1

        # Insert the data into the SQLite database
        c.execute(f"""INSERT OR REPLACE INTO {return_table} 
                 (id, created_utc, market_date, ticker, ticker_final, dscode, 
                  ismajorsec, dsname, dsqtname, exchange, 
                  ret_1m, ret_3m, ret_1y) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (row['id'], row['created_utc'], market_date, ticker,
                   ticker_upd, dscode, ismajorsec, dsname, dsqtname, exchange,
                   ret_1m, ret_3m, ret_1y))
        
        conn_out.commit()

        # Print write info
        print(f"wrote entry for {ticker} on {market_date}")
        with open(path_logfile_write, 'a') as lf:
            lf.write("Wrote returns to database:\n")
            lf.write(f"Ticker = {ticker}\n")
            lf.write(f"Updated ticker = {ticker_upd}\n")
            lf.write(f"ISIN = {crsp_isin}\n")
            lf.write(f"1m = {ret_1m}, 3m = {ret_3m}, 1y = {ret_1y}\n")
            lf.write(f"Post Date: {market_date}\n")
            lf.write(f"Submission ID: {row['id']}\n\n")

    conn_out.close()


if __name__ == "__main__":
    main()
