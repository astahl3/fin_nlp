'''
Simple script for checking and debugging Wharton Research Data Services (WRDS)
datasets, focusing on US-listed stock names and returns in Datastream and CRSP.

Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io 
'''
import wrds
import create_performance_db as pdb
from datetime import datetime, timedelta

# Global parameters
username = 'astahl3'

# Connect to the WRDS database
db = wrds.Connection(wrds_username=username)

# IDs to investigate with sample queries for CRSP and Datastream
ticker = 'CXW'
isin = 'US26658A1079'
cusip9 = '26658A107'
cusip = '26658A10'
post_date = "2013-01-01"
post_date_dt = datetime(year=2013, month=3, day=14)

# CRSP sample name / stockinfo query via ticker
query_crsp_names = f"""
                    SELECT *
                    FROM crsp_q_stock.stocknames_v2
                    WHERE ticker='{ticker}'
                    """
crsp_names = db.raw_sql(query_crsp_names)
crsp_permno = crsp_names['permno'][0]

# CRSP sample name / stockinfo query via cusip9
query_crsp_names = f"""
                    SELECT *
                    FROM crsp_q_stock.stocknames_v2
                    WHERE cusip9='{cusip9}'
                    """
crsp_names = db.raw_sql(query_crsp_names)
crsp_permno = crsp_names['permno'][0]

query_crsp_names_permno = f"""
                            SELECT *
                            FROM crsp_q_stock.stocknames_v2
                            WHERE permno='{crsp_permno}'
                            """
crsp_names_permno = db.raw_sql(query_crsp_names)

# Datastream sample name / stockinfo query
isin = 'US' + crsp_names['cusip9']
query_ds_names = f"""
                    SELECT *
                    FROM tr_ds_equities.wrds_ds_names
                    WHERE isin = '{isin}'
                    """
ds_names = db.raw_sql(query_ds_names)
dscode = ds_names['dscode'][0]

# Datastream sample stock return query
query_ds_returns = f"""
                    SELECT ri, ri_usd, marketdate, close
                    FROM tr_ds_equities.wrds_ds2dsf
                    WHERE dscode='{dscode}'
                    """
ds_returns = db.raw_sql(query_ds_returns)

# CRSP sample stock return query
query_crsp_returns = f"""
                        SELECT *
                        FROM crsp_q_stock.dsf
                        WHERE cusip = '{cusip}'
                        """
crsp_returns = db.raw_sql(query_crsp_returns)

# CRSP security events 
query_crsp_events = f"""
                    SELECT *
                    FROM crsp_q_stock.mse
                    WHERE cusip='{cusip}'
                    """
crsp_events = db.raw_sql(query_crsp_events)