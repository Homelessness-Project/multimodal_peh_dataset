"""
Filter Reddit Comments
=====================

This script filters Reddit comments from all_comments.csv to create filtered_comments.csv
for cities that have all_comments.csv but are missing the filtered version.

USAGE:
======
python scripts/filter_reddit_comments.py --city kzoo
python scripts/filter_reddit_comments.py --cities kzoo,portland
python scripts/filter_reddit_comments.py (processes all cities)
"""

import os
import pandas as pd
import argparse
import csv
from utils import CITY_MAP, KEYWORDS

def filter_reddit_comments_for_city(city, city_dir, base_data_dir='data'):
    """Filter Reddit comments for a specific city."""
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
        filtered_comments = []
        total_comments = len(df)
        filtered_count = 0
        
        for _, row in df.iterrows():
            comment_text = str(row['Comment']).lower()
            # Check if comment contains any keywords
            if any(keyword.lower() in comment_text for keyword in KEYWORDS):
                filtered_comments.append(row)
                filtered_count += 1
        
        # Create filtered dataframe
        filtered_df = pd.DataFrame(filtered_comments)
        
        if filtered_df.empty:
            print(f"No comments matched keywords for {city}")
            return None
        
        # Save filtered comments
        filtered_df.to_csv(filtered_comments_path, index=False)
        print(f"Saved {len(filtered_df)} filtered comments to {filtered_comments_path}")
        
        # Update statistics
        stats_path = os.path.join(reddit_dir, 'statistics.csv')
        update_statistics(stats_path, total_comments, len(filtered_df))
        
        return filtered_df
        
    except Exception as e:
        print(f"Error processing {city}: {e}")
        return None

def update_statistics(stats_path, total_comments, filtered_comments):
    """Update statistics file with new counts."""
    stats = {
        'Total Comments': total_comments,
        'Total Filtered Comments': filtered_comments,
        'Percentage of Comments Filtered': (filtered_comments / total_comments * 100) if total_comments > 0 else 0
    }
    
    # Read existing stats if file exists
    existing_stats = {}
    if os.path.exists(stats_path):
        try:
            with open(stats_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 2:
                        existing_stats[row[0]] = row[1]
        except Exception as e:
            print(f"Error reading existing statistics: {e}")
    
    # Update with new stats
    existing_stats.update(stats)
    
    # Write updated stats
    try:
        with open(stats_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for key, value in existing_stats.items():
                writer.writerow([key, value])
        print(f"Updated statistics: {stats_path}")
    except Exception as e:
        print(f"Error writing statistics: {e}")

def filter_all_cities(base_data_dir='data', cities=None):
    """Filter Reddit comments for all cities or specified cities."""
    if cities:
        cities_to_process = [city.strip() for city in cities.split(',')]
    else:
        cities_to_process = list(CITY_MAP.keys())
    
    print(f"Filtering Reddit comments for {len(cities_to_process)} cities")
    print("=" * 50)
    
    total_processed = 0
    total_filtered = 0
    
    for city in cities_to_process:
        if city not in CITY_MAP:
            print(f"City '{city}' not found in CITY_MAP, skipping...")
            continue
            
        city_dir = CITY_MAP[city]
        filtered_df = filter_reddit_comments_for_city(city, city_dir, base_data_dir)
        
        if filtered_df is not None:
            total_processed += 1
            total_filtered += len(filtered_df)
    
    print("\n" + "=" * 50)
    print("FILTERING SUMMARY:")
    print(f"Cities processed: {total_processed}")
    print(f"Total filtered comments: {total_filtered}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Filter Reddit comments from all_comments.csv to create filtered_comments.csv')
    
    parser.add_argument('--city', type=str, default=None,
                       help='Specific city to process (e.g., kzoo)')
    
    parser.add_argument('--cities', type=str, default=None,
                       help='Comma-separated list of cities to process (e.g., kzoo,portland)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    print("=== Reddit Comments Filtering Script ===")
    print(f"Data directory: {args.data_dir}")
    
    if args.city:
        print(f"Processing single city: {args.city}")
        if args.city in CITY_MAP:
            city_dir = CITY_MAP[args.city]
            filter_reddit_comments_for_city(args.city, city_dir, args.data_dir)
        else:
            print(f"City '{args.city}' not found in CITY_MAP")
    elif args.cities:
        print(f"Processing cities: {args.cities}")
        filter_all_cities(args.data_dir, args.cities)
    else:
        print("Processing all cities")
        filter_all_cities(args.data_dir) 