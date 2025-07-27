"""
Filter Existing Reddit Comments
==============================

This script filters existing all_comments.csv files to create filtered_comments.csv
for cities that have all_comments.csv but are missing the filtered version.

USAGE:
======
python scripts/filter_existing_reddit.py --city kzoo
"""

import os
import pandas as pd
import argparse
from utils import KEYWORDS

def filter_existing_reddit_comments(city, city_dir, base_data_dir='data'):
    """Filter existing Reddit comments for a specific city."""
    reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
    all_comments_path = os.path.join(reddit_dir, 'all_comments.csv')
    filtered_comments_path = os.path.join(reddit_dir, 'filtered_comments.csv')
    
    if not os.path.isfile(all_comments_path):
        print(f"File not found: {all_comments_path}")
        return None
    
    try:
        df = pd.read_csv(all_comments_path)
        if df.empty:
            print(f"No comments in {all_comments_path}")
            return None
        
        print(f"Processing {city}: {len(df)} total comments")
        
        # Check if Comment column exists
        if 'Comment' not in df.columns:
            print(f"Error: 'Comment' column not found in {all_comments_path}")
            return None
        
        # Filter comments that contain keywords
        filtered_df = df[df['Comment'].str.lower().str.contains('|'.join(KEYWORDS), case=False, na=False)]
        
        if filtered_df.empty:
            print(f"No comments matched keywords for {city}")
            return None
        
        # Save filtered comments
        filtered_df.to_csv(filtered_comments_path, index=False)
        print(f"Saved {len(filtered_df)} filtered comments to {filtered_comments_path}")
        
        return filtered_df
        
    except Exception as e:
        print(f"Error processing {city}: {e}")
        return None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Filter existing Reddit comments from all_comments.csv')
    
    parser.add_argument('--city', type=str, required=True,
                       help='City to process (e.g., kzoo)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    print("=== Filter Existing Reddit Comments ===")
    print(f"City: {args.city}")
    print(f"Data directory: {args.data_dir}")
    
    # Map city name to directory
    city_map = {
        'kzoo': 'kzoo',
        'kalamazoo': 'kzoo',
        'southbend': 'southbend',
        'south bend': 'southbend',
        'portland': 'portland',
        'baltimore': 'baltimore',
        'buffalo': 'buffalo',
        'elpaso': 'elpaso',
        'el paso': 'elpaso',
        'fayetteville': 'fayetteville',
        'rockford': 'rockford',
        'sanfrancisco': 'sanfrancisco',
        'san francisco': 'sanfrancisco',
        'scranton': 'scranton'
    }
    
    city_dir = city_map.get(args.city.lower())
    if not city_dir:
        print(f"City '{args.city}' not found in city map")
        exit(1)
    
    filter_existing_reddit_comments(args.city, city_dir, args.data_dir) 