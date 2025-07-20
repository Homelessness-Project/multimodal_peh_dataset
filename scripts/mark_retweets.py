import pandas as pd
import os
import glob
import re
from utils import CITY_MAP, KEYWORDS

KEYWORD_PATTERNS = [
    re.compile(rf"\\b{re.escape(kw)}\\b", re.IGNORECASE) if ' ' not in kw else re.compile(re.escape(kw), re.IGNORECASE)
    for kw in KEYWORDS
]

def find_keywords(text):
    text_lc = str(text).lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lc]

def mark_retweets_in_all_x_dirs(base_data_dir='data'):
    for city_dir in CITY_MAP.values():
        city_x_dir = os.path.join(base_data_dir, city_dir, 'x')
        if not os.path.isdir(city_x_dir):
            print(f"Directory does not exist: {city_x_dir}")
            continue
        # Find all posts_english_2015-2025.csv files in the x directory
        pattern = os.path.join(city_x_dir, 'posts_english_2015-2025.csv')
        for csv_path in glob.glob(pattern):
            print(f"Processing {csv_path}")
            try:
                df = pd.read_csv(csv_path)
                if 'text' not in df.columns:
                    print(f"Warning: 'text' column not found in {csv_path}. Skipping.")
                    continue
                df['is_retweet'] = df['text'].astype(str).str.startswith('RT')
                df['keywords_matched'] = df['text'].apply(lambda x: ', '.join(find_keywords(x)))
                base, ext = os.path.splitext(csv_path)
                output_path = f"{base}_rt{ext}"
                df.to_csv(output_path, index=False)
                print(f"Output written to {output_path}")
            except Exception as e:
                print(f"Error processing {csv_path}: {e}")

if __name__ == "__main__":
    mark_retweets_in_all_x_dirs() 