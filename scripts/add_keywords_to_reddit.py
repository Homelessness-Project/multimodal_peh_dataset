import os
import glob
import pandas as pd
from utils import CITY_MAP, KEYWORDS

def find_keywords(text):
    text_lc = str(text).lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lc]

def add_keywords_to_reddit(base_data_dir='data'):
    for city_dir in CITY_MAP.values():
        reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
        orig_path = os.path.join(reddit_dir, 'filtered_comments.csv')
        deid_path = os.path.join(reddit_dir, 'filtered_comments_deidentified.csv')
        if not os.path.isfile(orig_path):
            print(f"Original file not found: {orig_path}")
            continue
        try:
            # Process original file
            df_orig = pd.read_csv(orig_path)
            if 'Comment' not in df_orig.columns:
                print(f"Warning: 'Comment' column not found in {orig_path}. Skipping.")
                continue
            df_orig['keywords_matched'] = df_orig['Comment'].apply(lambda x: ', '.join(find_keywords(x)))
            df_orig.to_csv(orig_path, index=False)
            print(f"Updated: {orig_path}")
            # Process deidentified file, copy keywords_matched from original
            if os.path.isfile(deid_path):
                df_deid = pd.read_csv(deid_path)
                if len(df_deid) != len(df_orig):
                    print(f"Warning: Row count mismatch between {orig_path} and {deid_path}. Skipping deidentified update.")
                    continue
                df_deid['keywords_matched'] = df_orig['keywords_matched']
                df_deid.to_csv(deid_path, index=False)
                print(f"Updated: {deid_path}")
            else:
                print(f"Deidentified file not found: {deid_path}")
        except Exception as e:
            print(f"Error processing {orig_path} or {deid_path}: {e}")

if __name__ == '__main__':
    add_keywords_to_reddit() 