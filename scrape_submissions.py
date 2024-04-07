import asyncio
import asyncpraw
import json
import logging
import argparse
from datetime import datetime
from tqdm.asyncio import tqdm
import os

def setup_logging(outfile):
    # Set up logging
    log_file = f"log/{os.path.splitext(os.path.basename(outfile))[0]}_scrapelog.log"
    if os.path.exists(log_file):
        os.rename(log_file, log_file + '.old')

    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set up error logging
    error_log_file = f"log/{os.path.splitext(os.path.basename(outfile))[0]}_errorlog.log"
    if os.path.exists(error_log_file):
        os.rename(error_log_file, error_log_file + '.old')

    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)

    return error_logger

async def scrape_submissions(ids_file, auth_file, outfile, batch_size=100):
    error_logger = setup_logging(outfile)

    # Read auth data from AUTH.json file
    with open(auth_file) as f:
        auth_data = json.load(f)

    # Read submission IDs from file
    with open(ids_file) as f:
        submission_ids = [f't3_{line.strip()}' for line in f]

    # Initialize asyncPRAW Reddit instance
    reddit = asyncpraw.Reddit(
        client_id=auth_data['client_id'],
        client_secret=auth_data['client_secret'],
        user_agent=auth_data['user_agent'],
        username=auth_data['username'],
        password=auth_data['password']
    )

    # Open the output file for writing
    with open(outfile, 'w') as f:
        for i in tqdm(range(0, len(submission_ids), batch_size), desc=f"{os.path.basename(ids_file)}"):
            batch_ids = submission_ids[i:i+batch_size]
            try:
                # Fetch submissions in batches using reddit.info()
                async for submission in reddit.info(fullnames=batch_ids):
                    
                    # Get all fields that are already loaded using vars()
                    submission_data = vars(submission)

                    # Handle non-serializable objects
                    if '_reddit' in submission_data:
                        submission_data.pop('_reddit')
                    if 'comments' in submission_data:
                        submission_data.pop('comments')
                    if 'selftext_html' in submission_data:
                        submission_data.pop('selftext_html')
                    if 'subreddit' in submission_data:
                        submission_data['subreddit'] = submission_data['subreddit'].display_name
                    if 'author' in submission_data:
                        submission_data['author'] = submission_data['author'].name if submission_data['author'] else None

                    # Check for non-serializable objects and log them
                    non_serializable_keys = []
                    for key, value in submission_data.items():
                        try:
                            json.dumps(value)
                        except TypeError:
                            non_serializable_keys.append(key)

                    # Remove non-serializable objects from the submission data
                    for key in non_serializable_keys:
                        submission_data.pop(key)

                    if non_serializable_keys:
                        error_logger.error(f"Non-serializable objects found in submission {submission.id}: {', '.join(non_serializable_keys)}")                        

                    # Add retrieved_utc_new field with the current timestamp
                    submission_data['retrieved_utc_new'] = int(datetime.now().timestamp())

                    # Write the submission data to the output file
                    f.write(json.dumps(submission_data) + '\n')

                # Flush data to disk after each batch
                f.flush()

            except Exception as e:
                error_logger.error(f"Error scraping batch starting from {batch_ids[0]}: {str(e)}")

    await reddit.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Reddit submissions using asyncPRAW')
    parser.add_argument('--datasets', type=str, required=True, help='Datasets to scrape submissions for')
    parser.add_argument('--auth', type=str, default='AUTH.json', help='File containing Reddit API authentication data')
    parser.add_argument('--datatype', type=str, choices=['submissions', 'comments'], default='submissions', help='Type of data to scrape')
    args = parser.parse_args()

    datasets = args.datasets.split(',')

    for dataset in datasets:
        if args.datatype == 'submissions':
            ids_file = f'data/ids/submission_ids_{dataset}.csv'
            outfile = f'data/submissions_{dataset}.ndjson'
            asyncio.run(scrape_submissions(ids_file, args.auth, outfile))

        elif args.datatype == 'comments':
            raise NotImplementedError("Comment scraping not implemented yet")

        print(f"Finished scraping {dataset}")