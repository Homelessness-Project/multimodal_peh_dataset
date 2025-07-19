import pandas as pd
import re
from tqdm import tqdm
import spacy
import os
from utils import load_spacy_model, deidentify_text
import argparse
import multiprocessing
import math

def deidentify_file(input_file, output_file, columns_to_deidentify, nlp, n_process=None, exclude_columns=None):
    if n_process is None:
        n_process = max(1, multiprocessing.cpu_count() - 1)
    if not os.path.exists(input_file):
        print(f"Skipping {input_file} - file not found")
        return
    if os.path.exists(output_file):
        print(f"Skipping {input_file} - deidentified file already exists at {output_file}")
        return
    print(f"\nProcessing {input_file}...")
    df = pd.read_csv(input_file)
    deidentified_df = pd.DataFrame()
    deidentified_cols = []
    # Deidentify specified columns
    for col in columns_to_deidentify:
        if exclude_columns and col in exclude_columns:
            continue
        if col in df.columns:
            new_col = f"Deidentified_{col}"
            print(f"Deidentifying {col} ...")
            texts = df[col].astype(str).tolist()
            n_rows = len(texts)
            if n_rows < 500:
                batch_size = 100
            elif n_rows < 5000:
                batch_size = 500
            elif n_rows < 10000:
                batch_size = 1000
            else:
                batch_size = 2000
            n_batches = math.ceil(n_rows / batch_size)
            n_proc_used = min(n_process, n_batches)
            print(f"Using batch_size={batch_size} for {n_rows} rows and n_process={n_proc_used} (max batches: {n_batches}).")
            deidentified = []
            for doc in tqdm(nlp.pipe(texts, batch_size=batch_size, n_process=n_proc_used), total=n_rows):
                deidentified.append(deidentify_text(doc.text, nlp))
            deidentified_df[new_col] = deidentified
            deidentified_cols.append(col)
        else:
            print(f"Column {col} not found in {input_file}, skipping deidentification for this column.")
    # Include all original columns except those deidentified or excluded
    keep_columns = [col for col in df.columns if (not exclude_columns or col not in exclude_columns) and col not in deidentified_cols]
    output_df = pd.concat([df[keep_columns].reset_index(drop=True), deidentified_df.reset_index(drop=True)], axis=1)
    output_df.to_csv(output_file, index=False)
    print(f"Saved deidentified data to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Deidentify comments for specified data types.")
    default_n_process = max(1, multiprocessing.cpu_count() - 1)
    parser.add_argument('--type', choices=['reddit', 'x', 'news', 'all'], default='all', help='Type of data to deidentify (reddit, x, news, or all)')
    parser.add_argument('--n_process', type=int, default=default_n_process, help='Number of processes for spaCy nlp.pipe (parallelism)')
    args = parser.parse_args()

    print(f"Loading spaCy model... Using n_process={args.n_process}")
    nlp = load_spacy_model()

    data_dir = "data"
    cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

    # Configuration for each data type
    file_configs = [
        {
            'name': 'reddit',
            'input_pattern': "data/{city}/reddit/filtered_comments.csv",
            'columns': ['Submission Title', 'Comment'],
            'exclude_columns': ['Submission URL']
        },
        {
            'name': 'x',
            'input_pattern': "data/{city}/x/posts_english_2015-2025_rt.csv",
            'columns': ['text'],
            'exclude_columns': ["id", "author_id"]
        },
        {
            'name': 'news',
            'input_pattern': "data/{city}/newspaper/{city}_filtered.csv",
            'columns': ['article_title', 'paragraph_text'],
            'exclude_columns': []
        }
    ]

    if args.type != 'all':
        file_configs = [cfg for cfg in file_configs if cfg['name'] == args.type]

    for city in cities:
        print(f"\nProcessing {city}...")
        for config in file_configs:
            input_file = config['input_pattern'].format(city=city)
            output_file = input_file.replace('.csv', '_deidentified.csv')
            deidentify_file(input_file, output_file, config['columns'], nlp, n_process=args.n_process, exclude_columns=config.get('exclude_columns', []))

if __name__ == "__main__":
    """
    Example command line usage:

    # Deidentify all data types (default)
    python scripts/deidentify_text.py
    python scripts/deidentify_text.py --type all

    # Deidentify only Reddit data
    python scripts/deidentify_text.py --type reddit

    # Deidentify only X (Twitter) data
    python scripts/deidentify_text.py --type x

    # Deidentify only News data
    python scripts/deidentify_text.py --type news
    """
    main() 