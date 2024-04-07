import psycopg
import pandas as pd
from typing import List
import sys

def fetch_ids(
    host: str,
    port: str,
    dbname: str,
    table_name: str,
    subreddit: str,
    output_filepath: str
) -> None:
    """
    Performs grouping and aggregation operations on a database table, grouping by week based on created_utc,
    and saves the results to a CSV file.
    """
    # Establish a connection to the database
    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user="postgres",
    )

    # Create a cursor object
    cur = conn.cursor()

    # Construct the SQL query, replacing 'group_by_param' with a date_trunc operation on 'created_utc'
    query = f"""
    SELECT id
    FROM {table_name}
    WHERE dataset = %s
    """

    # Execute the query
    cur.execute(query, (dataset,))

    # Fetch all the results
    results = cur.fetchall()

    # Convert the results to a pandas DataFrame
    df = pd.DataFrame(results)

    # Save the DataFrame to a CSV file
    df.to_csv(output_filepath, index=False)

    # Close the cursor and the connection
    cur.close()
    conn.close()

if __name__ == "__main__":

    host = "localhost"
    port = "5432"
    dbname = "datasets"

    dataset = sys.argv[1].split(',')

    for dataset in dataset:
        fetch_ids(host, port, dbname, 'reddit.submissions_arctic', dataset, f'data/ids/submission_ids_{dataset}.csv')
        fetch_ids(host, port, dbname, 'reddit.comments_arctic', dataset, f'data/ids/comment_ids_{dataset}.csv')
        print(f"Done: {dataset}")
