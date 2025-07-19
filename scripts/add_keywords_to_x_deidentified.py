import os
import glob
import pandas as pd
from utils import CITY_MAP, KEYWORDS

def find_keywords(text):
    text_lc = str(text).lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lc]

def add_keywords_to_x_deidentified(base_data_dir='data'):
    for city_dir in CITY_MAP.values():
        city_x_dir = os.path.join(base_data_dir, city_dir, 'x')
        if not os.path.isdir(city_x_dir):
            print(f"Directory does not exist: {city_x_dir}")
            continue
        pattern = os.path.join(city_x_dir, 'posts_english_2015-2025_rt_deidentified.csv')
        for csv_path in glob.glob(pattern):
            print(f"Processing {csv_path}")
            try:
                df = pd.read_csv(csv_path)
                if 'Deidentified_text' not in df.columns:
                    print(f"Warning: 'Deidentified_text' column not found in {csv_path}. Skipping.")
                    continue
                df['keywords_matched'] = df['Deidentified_text'].apply(lambda x: ', '.join(find_keywords(x)))
                df.to_csv(csv_path, index=False)
                print(f"Output written to {csv_path}")
            except Exception as e:
                print(f"Error processing {csv_path}: {e}")

if __name__ == '__main__':
    add_keywords_to_x_deidentified() 