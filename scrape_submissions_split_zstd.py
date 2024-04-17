import asyncio
import asyncpraw
import json
import logging
import argparse
from datetime import datetime
from tqdm.asyncio import tqdm
import os
from itertools import islice
import zstandard as zstd

def setup_logging(base_folder, dataset, split_file):
    log_folder = os.path.join(base_folder, f"log/submissions_{dataset}")
    os.makedirs(log_folder, exist_ok=True)

    log_file = os.path.join(log_folder, f"{os.path.splitext(os.path.basename(split_file))[0]}_scrapelog.log")
    old_log_file = log_file + '.old'
    if os.path.exists(old_log_file):
        os.remove(old_log_file)
    if os.path.exists(log_file):
        os.rename(log_file, old_log_file)

    logging.basicConfig(filename=log_file, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    error_log_file = os.path.join(log_folder, f"{os.path.splitext(os.path.basename(split_file))[0]}_errorlog.log")
    old_error_log_file = error_log_file + '.old'
    if os.path.exists(old_error_log_file):
        os.remove(old_error_log_file)
    if os.path.exists(error_log_file):
        os.rename(error_log_file, old_error_log_file)

    error_logger = logging.getLogger('error_logger')
    error_logger.setLevel(logging.ERROR)
    error_handler = logging.FileHandler(error_log_file)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    error_handler.setFormatter(error_formatter)
    error_logger.addHandler(error_handler)

    return error_logger


async def scrape_submissions(base_folder, dataset, split_file, auth_file, batch_size=1000, reddit_batch_size=100):
    error_logger = setup_logging(base_folder, dataset, split_file)

    with open(auth_file) as f:
        auth_data = json.load(f)

    ids_processed_folder = os.path.join(os.path.dirname(split_file), 'processed')
    os.makedirs(ids_processed_folder, exist_ok=True)

    processed_ids_file = os.path.join(ids_processed_folder, f"{os.path.splitext(os.path.basename(split_file))[0]}_processed.csv")
    if os.path.exists(processed_ids_file):
        os.remove(processed_ids_file)  # Delete the processed IDs file before starting

    processed_ids = set()

    reddit = asyncpraw.Reddit(
        client_id=auth_data['client_id'],
        client_secret=auth_data['client_secret'],
        user_agent=auth_data['user_agent'],
        username=auth_data['username'],
        password=auth_data['password']
    )

    outfile = os.path.join(base_folder, f'data/submissions_{dataset}', f"{os.path.splitext(os.path.basename(split_file))[0]}.ndjson.zst")
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    # Rename the existing zst file to .zst.old if it exists
    if os.path.exists(outfile):
        old_outfile = outfile + '.old'
        if os.path.exists(old_outfile):
            os.remove(old_outfile)
        os.rename(outfile, old_outfile)

    compressor = zstd.ZstdCompressor(level=3)

    with open(outfile, 'wb') as f:
        with compressor.stream_writer(f) as writer:
            with open(split_file) as ids_f:
                total_ids = sum(1 for _ in ids_f)
                ids_f.seek(0)

                with tqdm(total=total_ids, desc=f"{os.path.basename(split_file)}") as pbar:
                    while True:
                        batch_ids = [line.strip() for line in islice(ids_f, batch_size)]
                        
                        if not batch_ids:
                            break

                        batch_ids = [id for id in batch_ids if id not in processed_ids]
                        batch_ids = [f't3_{id}' for id in batch_ids]
                        
                        processed_batch_ids = []  

                        try:
                            for i in range(0, len(batch_ids), reddit_batch_size):
                                reddit_batch = batch_ids[i:i+reddit_batch_size]
                                async for submission in reddit.info(fullnames=reddit_batch):
                                    submission_data = vars(submission)

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

                                    non_serializable_keys = []
                                    for key, value in submission_data.items():
                                        try:
                                            json.dumps(value)
                                        except TypeError:
                                            non_serializable_keys.append(key)

                                    for key in non_serializable_keys:
                                        submission_data.pop(key)

                                    if non_serializable_keys:
                                        error_logger.error(f"Non-serializable objects found in submission {submission.id}: {', '.join(non_serializable_keys)}")
                                        continue

                                    submission_data['retrieved_utc'] = int(datetime.now().timestamp())

                                    writer.write(json.dumps(submission_data).encode('utf-8'))
                                    writer.write(b'\n')

                                    processed_batch_ids.append(submission.id)

                                writer.flush()
                                
                                with open(processed_ids_file, 'a') as processed_f:
                                    for id in processed_batch_ids:
                                        processed_f.write(f"{id}\n")

                                pbar.update(len(reddit_batch))
                                processed_batch_ids = []

                        except Exception as e:
                            error_message = str(e)
                            if "Cannot connect to host" in error_message or "Connect call failed" in error_message:
                                error_logger.error(f"Connection error scraping batch starting from {batch_ids[0]}: {error_message}")
                            else:
                                submission_id = submission.id
                                error_logger.error(f"Error scraping submission {submission_id}: {error_message}")
    await reddit.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Reddit submissions using asyncPRAW')
    parser.add_argument('--dataset', type=str, required=True, help='Dataset to scrape submissions for')
    parser.add_argument('--auth', type=str, default='auth/AUTH.json', help='File containing Reddit API authentication data')
    parser.add_argument('--datatype', type=str, choices=['submissions', 'comments'], default='submissions', help='Type of data to scrape')
    parser.add_argument('--basefolder', type=str, default='', help='Base folder for data writing')
    parser.add_argument('--split_range', type=str, default=None, help='Range of split files to process (e.g., "1,5")')
    args = parser.parse_args()

    dataset = args.dataset

    if args.datatype == 'submissions':
        split_folder = os.path.join(args.basefolder, f'data/ids/batched/submission_ids_{dataset}')

        if args.split_range:
            start, end = map(int, args.split_range.split(','))
            split_files = [os.path.join(split_folder, f'submission_ids_{dataset}_{i}.txt') for i in range(start, end+1)]
        else:
            split_files = [os.path.join(split_folder, f) for f in os.listdir(split_folder) if f.endswith('.txt')]

        for split_file in split_files:
            asyncio.run(scrape_submissions(args.basefolder, dataset, split_file, args.auth))

    elif args.datatype == 'comments':
        raise NotImplementedError("Comment scraping not implemented yet")

    print(f"Finished scraping {dataset}")
