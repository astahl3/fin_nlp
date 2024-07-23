'''
Helper functions for screening submission titles based on ticker and/or
alias matching. For use with submission_data_to_db.py.


Author: Aaron M. Stahl (2024) // aaron.m.stahl@gmail.com // astahl3.github.io
'''
import pandas as pd
import re

# Global screening parameters
single_match = True

# File paths for screening
path_problem_tickers_db_read = \
    "/Users/astahl/fin_nlp_data/ticker_lists/problem_tickers.csv"
path_etf_tickers_db_read = \
    "/Users/astahl/fin_nlp_data/ticker_lists/etf_tickers.csv"

    
# Create sets for problem tickers (common acronyms, words) and ETF tickers
problem_tickers_df = pd.read_csv(path_problem_tickers_db_read)
etf_tickers_df = pd.read_csv(path_etf_tickers_db_read)
problem_tickers = set(problem_tickers_df['stock_ticker'])
etf_tickers = set(etf_tickers_df['ticker'])

# Characters to remove from title strings
chars_to_remove = {'~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', 
                   '-', '[', ']', '{', '}', '>', '<', '.', ',', '=', '|',
                   '/', ':', ';', '\\', '"', "'", '?'}

def ticker_match_with_symbol(submission, fields, tickers=None):
    ''' 
    Checks for tickers with leading $ symbol in title string that match a
    ticker in the tickers set (e.g., $GME).
    '''
    
    title_string = submission.get('title')
    word_A = None
    
    # Remove common special characters from title to improve the
    # reliability of ticker matching (e.g., "buy $GME!!!")
    chars_to_remove_temp = chars_to_remove.copy()
    chars_to_remove_temp.remove('$')
    for char in chars_to_remove_temp:
        title_string = title_string.replace(char, '')
    
    # Remove undesired tickers from ticker matching list
    '''
    for t in problem_tickers:
        if t in tickers: 
            tickers.remove(t)
    '''
    
    # Check for ticker match in title string where $ symbol precedes ticker
    tickers_with_sym = {'$' + ticker for ticker in tickers}
    for word in title_string.split():
        if word in tickers_with_sym:
            word_A = word
            break # match found, break loop

    if word_A is None: return None

    if single_match:
        
        title_string_reduced = title_string.replace(word_A,'')
        for word in title_string_reduced.split():
            
            # If there's another match, return immediately
            if word in tickers_with_sym: return 'multiple_matches'
            
            # If there's another match to regex pattern, return immediately
            #ticker_pattern = r'\$[A-Z]{1,5}\b' # no periods
            ticker_pattern = r'\$[A-Z]{1,4}\.[A-Z]|\$[A-Z]{1,5}\b' # periods
            word_B = re.search(ticker_pattern, title_string_reduced)
            if word_B: return 'multiple_matches'
            
    word_A = word_A.replace('$','')
    submission['company_match'] = word_A
    
    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)
    
    # Set qualification type
    submission['match_type'] = 'ticker_with_symbol'                     
    
    return submission
    
def ticker_nomatch_with_symbol(submission, fields):
    ''' 
    Checks for tickers with leading $ symbol in title string where no match is
    required with tickers set (e.g., $GME).
    '''
        
    title_string = submission.get('title')
    match_A = None
    
    matches_to_redirect = {'GOOG': 'GOOGL'}
    
    # Use regex to find instances of $ followed by 1 to 5 uppercase letters
    ticker_pattern = r'\$[A-Z]{1,4}\.[A-Z]|\$[A-Z]{1,5}\b' # allow periods
    #ticker_pattern = r'\$[A-Z]{1,5}\b' # don't allow periods
    match_A = re.search(ticker_pattern, title_string)
    
    if match_A: matched_ticker = match_A.group(0)
    else: return None

    # Ignore ETF tickers
    if matched_ticker.replace('$','') in etf_tickers:
        return None
        
    if single_match:
        title_string_reduced = title_string.replace(matched_ticker,'')
        match_B = re.search(ticker_pattern, title_string_reduced)
        if match_B: return 'multiple_matches'
    
    # Fix commonly mistyped tickers
    if match_A in matches_to_redirect: match_A = matches_to_redirect[match_A]
    
    # Add ticker to submission company_match field
    matched_ticker = matched_ticker.replace('$','')
    submission['company_match'] = matched_ticker

    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)

    # Set qualification type
    submission['match_type'] = 'symbol_no_match'

    return submission

def ticker_match_with_parenthesis(submission, fields, tickers=None):
    ''' 
    Checks for tickers in parenthesis in title string where a match is
    required with tickers set (e.g., (GME)).
    '''
        
    title_string = submission.get('title')
    word_A = None
    
    # Remove common special characters from title to improve the
    # reliability of ticker matching (e.g., "buy $GME!!!")
    chars_to_remove_temp = chars_to_remove.copy()
    chars_to_remove_temp.remove('(')
    chars_to_remove_temp.remove(')')
    for char in chars_to_remove_temp:
        title_string = title_string.replace(char, '')
    
    # Check for ticker match in title string where $ symbol precedes ticker
    tickers_with_sym = {'(' + ticker + ')' for ticker in tickers}
    for word in title_string.split():
        if word in tickers_with_sym:
            word_A = word
            break # match found, break loop

    if word_A is None: return None

    if single_match:
        
        title_string_reduced = title_string.replace(word_A,'')
        for word in title_string_reduced.split():
            
            # If there's another match, return immediately
            if word in tickers_with_sym: return 'multiple_matches'
            
            # If another unmatched ticker with symbol, return immediately
            ticker_pattern = r'\$[A-Z]{1,4}\.[A-Z]|\$[A-Z]{1,5}\b'
            word_B = re.search(ticker_pattern, title_string_reduced)
            if word_B: return 'multiple_matches'
            
            # If another unmatched ticker in parenthesis, return immediately
            ticker_pattern = r'\([A-Z]{1,5}(?:\.[A-Z])?\)'
            word_B = re.search(ticker_pattern, title_string_reduced)
            if word_B: return 'multiple_matches'

            
    word_A = word_A.replace('(','')
    word_A = word_A.replace(')','')
    submission['company_match'] = word_A
    
    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)
    
    # Set qualification type
    submission['match_type'] = 'ticker_with_symbol'                     
    
    return submission

def ticker_nomatch_with_parenthesis(submission, fields):
    ''' 
    Checks for tickers in parenthesis in title string where a match is
    required with tickers set (e.g., (GME)).
    '''
        
    title_string = submission.get('title')
    match_A = None
    
    matches_to_redirect = {'GOOG': 'GOOGL'}
    
    # Use regex to find instances of $ followed by 1 to 5 uppercase letters
    ticker_pattern = r'\$[A-Z]{1,4}\.[A-Z]|\$[A-Z]{1,5}\b' # allow periods
    #ticker_pattern = r'\$[A-Z]{1,5}\b' # don't allow periods
    match_A = re.search(ticker_pattern, title_string)
    
    if match_A: matched_ticker = match_A.group(0)
    else: return None

    # Ignore ETF tickers
    if matched_ticker.replace('$','') in etf_tickers:
        return None
        
    if single_match:
        title_string_reduced = title_string.replace(matched_ticker,'')
        match_B = re.search(ticker_pattern, title_string_reduced)
        if match_B: return 'multiple_matches'
    
    # Fix commonly mistyped tickers
    if match_A in matches_to_redirect: match_A = matches_to_redirect[match_A]
    
    # Add ticker to submission company_match field
    matched_ticker = matched_ticker.replace('$','')
    submission['company_match'] = matched_ticker

    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)

    # Set qualification type
    submission['match_type'] = 'symbol_no_match'

    return submission


def ticker_match_no_symbol(submission, fields, tickers=None):
    ''' 
    Checks for tickers without a leading $ symbol in title string that match
    an entry in the tickers set (e.g., GME).
    '''
        
    match_A = None
    title_string = submission.get('title')
    
    for char in chars_to_remove:
        title_string = title_string.replace(char, '')
    
    # Check for ticker match in title string
    tickers_no_sym = tickers.copy()
    for t in problem_tickers:
        tickers_no_sym.remove(t)
   
    for word in title_string.split():
        if word in tickers_no_sym:
            match_A = word
            break
        
    if match_A is None: return None
    
    if single_match:
        
        title_string_reduced = title_string.replace(match_A,'')
        for word in title_string_reduced.split():
            
            # If there's another match, return False immediately
            if word in tickers_no_sym: return 'multiple_matches' 
            
    submission['company_match'] = match_A
    
    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)
    
    # Set qualification type
    submission['match_type'] = 'ticker_no_symbol'
    
    return submission

def alias_match(submission, fields, aliases=None):
    ''' 
    Checks for company name and alias matches using the aliases regex string.
    '''
    
    title_string = submission.get('title')
    chars_to_remove_temp = chars_to_remove.copy()
    chars_to_remove_temp.remove('-') # accommodate dashes (e.g., Coca-Cola)
    for char in chars_to_remove_temp:
        title_string = title_string.replace(char, '')
            
    match_A = re.search(aliases, title_string, re.IGNORECASE)
    if match_A is None: return None
    
    matched_alias = match_A.group(0)    
    
    if single_match:
        
        title_string_reduced = title_string.replace(matched_alias, '')
        match_B = re.search(aliases, title_string_reduced, re.IGNORECASE)
        if match_B: return None
                
    submission['company_match'] = matched_alias

    # Check if this is a due diligence post
    submission['is_DD'] = check_if_DD(title_string)
    
    # Set qualification type
    submission['match_type'] = 'alias'
    
    return submission

def check_if_DD(input_string):
    ''' Return True if title indicates a due diligence post '''
    
    for char in chars_to_remove:
        input_string = input_string.replace(char, '')
    
    if 'dd' in input_string.lower().split() or \
        'due diligence' in input_string.lower():
        return True
    else:
        return False
