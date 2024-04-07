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
        submission_ids = [line.strip() for line in f]

    # Create the ids_processed folder if it doesn't exist
    ids_processed_folder = os.path.join(os.path.dirname(ids_file), 'processed')
    os.makedirs(ids_processed_folder, exist_ok=True)

    # Read processed IDs from file if it exists
    processed_ids_file = os.path.join(ids_processed_folder, f"{os.path.splitext(os.path.basename(ids_file))[0]}_processed.csv")
    if os.path.exists(processed_ids_file):
        with open(processed_ids_file) as f:
            processed_ids = set(line.strip() for line in f)
    else:
        processed_ids = set()

    # Get the total number of submission IDs
    submission_count_total = len(submission_ids)

    # Remove already processed IDs from submission_ids
    submission_ids = [id for id in submission_ids if id not in processed_ids]

    # Add the 't3_' prefix to the remaining submission IDs
    submission_ids = [f't3_{id}' for id in submission_ids]

    # Get the total number of submission IDs after removing already processed IDs
    submission_count = len(submission_ids)

    # Print the total number of submission IDs and the number of IDs already processed
    print(f"Total submission IDs: {submission_count}. Skipping {submission_count_total - submission_count} already processed IDs.")

    # Remove already processed IDs from submission_ids
    submission_ids = [id for id in submission_ids if id not in processed_ids]

    # Initialize asyncPRAW Reddit instance
    reddit = asyncpraw.Reddit(
        client_id=auth_data['client_id'],
        client_secret=auth_data['client_secret'],
        user_agent=auth_data['user_agent'],
        username=auth_data['username'],
        password=auth_data['password']
    )

    # Open the output file for writing
    with open(outfile, 'a') as f:
        with tqdm(total=len(submission_ids), desc=f"{os.path.basename(ids_file)}") as pbar:
            for i in range(0, len(submission_ids), batch_size):
                batch_ids = submission_ids[i:i+batch_size]
                processed_batch_ids = []
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
                        if 'poll_data' in submission_data:
                            poll_data = submission_data['poll_data']
                            if poll_data:
                                submission_data['poll_data'] = {
                                    'options': [
                                        {
                                            'text': option.text,
                                            'vote_count': option.vote_count,
                                            'id': option.id
                                        }
                                        for option in poll_data.options
                                    ] if hasattr(poll_data, 'options') else [],
                                    'total_vote_count': poll_data.total_vote_count if hasattr(poll_data, 'total_vote_count') else None,
                                    'user_selection': poll_data.user_selection.text if hasattr(poll_data, 'user_selection') and poll_data.user_selection else None,
                                    'voting_end_timestamp': poll_data.voting_end_timestamp if hasattr(poll_data, 'voting_end_timestamp') else None
                                }
                            else:
                                submission_data['poll_data'] = None


                        # Check for non-serializable objects
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
                            continue  # Skip writing to file and adding to processed IDs

                        # Add retrieved_utc_new field with the current timestamp
                        submission_data['retrieved_utc'] = int(datetime.now().timestamp())

                        # Write the submission data to the output file
                        f.write(json.dumps(submission_data) + '\n')

                        # Add the successfully processed ID to the list
                        processed_batch_ids.append(submission.id)

                    # Flush data to disk after each batch
                    f.flush()

                    # Write processed IDs to file only if there were no errors
                    with open(processed_ids_file, 'a') as processed_f:
                        for id in processed_batch_ids:
                            processed_f.write(f"{id}\n")

                    # Update the progress bar with the batch size
                    pbar.update(len(batch_ids))

                except Exception as e:
                    error_message = str(e)
                    if "Cannot connect to host" in error_message or "Connect call failed" in error_message:
                        # Skip adding IDs to processed file if there was an internet outage error
                        error_logger.error(f"Connection error scraping batch starting from {batch_ids[0]}: {error_message}")
                    else:
                        # Log the error with the specific submission ID
                        submission_id = submission.id if 'submission' in locals() else 'Unknown'
                        error_logger.error(f"Error scraping submission {submission_id}: {error_message}")
    await reddit.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Reddit submissions using asyncPRAW')
    parser.add_argument('--datasets', type=str, required=True, help='Datasets to scrape submissions for')
    parser.add_argument('--auth', type=str, default='auth/AUTH.json', help='File containing Reddit API authentication data')
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