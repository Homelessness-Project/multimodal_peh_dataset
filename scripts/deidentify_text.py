import pandas as pd
import re
from tqdm import tqdm
import spacy
import os
from utils import load_spacy_model, deidentify_text
import argparse

def deidentify_file(input_file, output_file, columns_to_deidentify, nlp):
    if not os.path.exists(input_file):
        print(f"Skipping {input_file} - file not found")
        return
    if os.path.exists(output_file):
        print(f"Skipping {input_file} - deidentified file already exists at {output_file}")
        return
    print(f"\nProcessing {input_file}...")
    df = pd.read_csv(input_file)
    for col in columns_to_deidentify:
        if col in df.columns:
            new_col = f"Deidentified_{col}"
            print(f"Deidentifying {col} ...")
            df[new_col] = [deidentify_text(val, nlp) for val in tqdm(df[col])]
        else:
            print(f"Column {col} not found in {input_file}, skipping deidentification for this column.")
    df.to_csv(output_file, index=False)
    print(f"Saved deidentified data to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Deidentify comments for specified data types.")
    parser.add_argument('--type', choices=['reddit', 'x', 'news', 'all'], default='all', help='Type of data to deidentify (reddit, x, news, or all)')
    args = parser.parse_args()

    print("Loading spaCy model...")
    nlp = load_spacy_model()

    data_dir = "data"
    cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]

    # Configuration for each data type
    file_configs = [
        {
            'name': 'reddit',
            'input_pattern': "data/{city}/reddit/filtered_comments.csv",
            'columns': ['Submission Title', 'Comment']
        },
        {
            'name': 'x',
            'input_pattern': "data/{city}/x/posts_english_2015-2025.csv",
            'columns': ['text']
        },
        {
            'name': 'news',
            'input_pattern': "data/{city}/newspaper/{city}_filtered.csv",
            'columns': ['article_title', 'paragraph_text']
        }
    ]

    if args.type != 'all':
        file_configs = [cfg for cfg in file_configs if cfg['name'] == args.type]

    for city in cities:
        print(f"\nProcessing {city}...")
        for config in file_configs:
            input_file = config['input_pattern'].format(city=city)
            output_file = input_file.replace('.csv', '_deidentified.csv')
            deidentify_file(input_file, output_file, config['columns'], nlp)

if __name__ == "__main__":
    """
    Example command line usage:

    # Deidentify all data types (default)
    python scripts/deidentify_comments.py
    python scripts/deidentify_comments.py --type all

    # Deidentify only Reddit data
    python scripts/deidentify_comments.py --type reddit

    # Deidentify only X (Twitter) data
    python scripts/deidentify_comments.py --type x

    # Deidentify only News data
    python scripts/deidentify_comments.py --type news
    """
    main() 