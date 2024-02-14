#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import sqlite3
import pathlib

def process_submission(submission, fields):
    if (submission.get('domain') == 'self.investing' and 
       submission.get('selftext') != '[removed]' and
       submission.get('selftext') != '' and
       submission.get('selftext') != '[deleted]'):
        return tuple(submission.get(field) for field in fields)
    return None

def process_submissions_file(submissions_file, fields, batch_size=1000):
    """Process the submissions file in batches and extract desired fields."""
    with open(submissions_file, "r", encoding="utf-8") as file:
        submissions = []
        batch = []
        batch_count = 0
        for line in file:
            submission = parse_submission_line(line.strip(), fields)
            processed_submission = process_submission(submission, fields)
            if processed_submission:
                batch.append(processed_submission)
                if len(batch) >= batch_size:
                    processed_batch = process_batch(batch)
                    submissions.extend(processed_batch)
                    batch = []
                    batch_count += 1
                    print(f"Processed {batch_count * batch_size} submissions.")
        # Process the remaining submissions in the last batch
        if batch:
            processed_batch = process_batch(batch)
            submissions.extend(processed_batch)
            batch_count += 1
            print(f"Processed {batch_count * batch_size + len(batch)} submissions.")
    return submissions

def process_batch(batch):
    """Filter and process a batch of submissions."""
    processed_batch = []
    for submission in batch:
        if submission:
            processed_batch.append(submission)
    return processed_batch

def parse_submission_line(line, fields):
    """Parse a single line representing a submission and extract desired fields."""
    submission_data = json.loads(line)
    submission = {field: submission_data.get(field, None) for field in fields}
    return submission

def write_submissions_to_database(submissions, batch_size=1000):
    """Write submissions to SQLite database in batches."""
    # Connect to SQLite database
    conn = sqlite3.connect("submissions.db")
    c = conn.cursor()

    # Create submissions table if not exists
    c.execute('''CREATE TABLE IF NOT EXISTS submissions (
                author TEXT,
                author_created_utc INTEGER,
                author_fullname TEXT,
                created_utc INTEGER,
                domain TEXT,
                id TEXT,
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
                upvote_ratio REAL
                )''')

    # Write submissions in batches
    batch_count = 0
    for submission in submissions:
        if submission:  # Check if submission is not None
            c.execute('''INSERT OR IGNORE INTO submissions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      submission)
            batch_count += 1
            if batch_count % batch_size == 0:
                conn.commit()
                print(f"Processed {batch_count} submissions.")
    conn.commit()
    conn.close()


def main():
    # Path to the submissions file
    submissions_file = pathlib.Path("/Users/astahl/fin_nlp_data/investing_submissions.txt").expanduser()
    
    # Define the fields to extract
    fields = ["author", "author_created_utc", "author_fullname", "created_utc", "domain", "id",
              "is_created_from_ads_ui", "is_crosspostable", "is_video", "num_comments", "num_crossposts",
              "over_18", "pinned", "retrieved_on", "score", "selftext", "send_replies", "subreddit",
              "subreddit_id", "subreddit_subscribers", "title", "upvote_ratio"]
    
    # Process submissions file and extract desired fields in batches
    submissions = process_submissions_file(submissions_file, fields)

    # Write submissions to SQLite database
    write_submissions_to_database(submissions)

if __name__ == "__main__":
    main()




