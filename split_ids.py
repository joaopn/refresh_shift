import argparse
import csv
import os

def read_ids(filename):
    """Read IDs from a CSV file."""
    with open(filename, mode='r', newline='') as file:
        reader = csv.reader(file)
        ids = [row[0] for row in reader if row]  # assuming each line has a single ID
    return ids

def filter_ids(ids, remove_file):
    """Filter out IDs listed in the remove_file."""
    remove_ids = set(read_ids(remove_file))
    return [id_ for id_ in ids if id_ not in remove_ids]

def split_ids(ids, original_filename, batch_size):
    """Split IDs into multiple files and save in a specific directory."""
    base_dir = os.path.dirname(original_filename)
    base_name = os.path.basename(original_filename).replace('.csv', '')
    batch_dir = os.path.join(base_dir, 'batched', base_name)
    
    os.makedirs(batch_dir, exist_ok=True)
    
    for i in range(0, len(ids), batch_size):
        batch_filename = os.path.join(batch_dir, f"{base_name}_%i.txt" % ((i // batch_size) + 1))
        with open(batch_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows([[id_] for id_ in ids[i:i+batch_size]])

def main():
    parser = argparse.ArgumentParser(description="Process Reddit IDs from a CSV file.")
    parser.add_argument('--filename', type=str, help='The CSV file containing Reddit IDs')
    parser.add_argument('--remove', type=str, help='Optional file with IDs to remove', required=False)
    parser.add_argument('--batch_size', type=int, default=1000000, help='Number of IDs per file (default: 1,000,000)')
    
    args = parser.parse_args()
    
    ids = read_ids(args.filename)
    if args.remove:
        ids = filter_ids(ids, args.remove)
    
    split_ids(ids, args.filename, args.batch_size)

if __name__ == "__main__":
    main()
