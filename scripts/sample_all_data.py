"""
Data Sampling and Copying Script
================================

This script samples or copies all data types (Twitter posts, meeting minutes, 
Reddit comments, newspaper articles) from cities in the dataset.

USAGE EXAMPLES:
==============

1. Sample 50 posts per city (default):
   python scripts/sample_all_data.py

2. Sample 100 posts per city:
   python scripts/sample_all_data.py --samples-per-city 100

3. Copy ALL data (not samples) to separate files:
   python scripts/sample_all_data.py --mode all --output-dir complete_dataset

4. Copy all data to custom directory:
   python scripts/sample_all_data.py --mode all --output-dir all_data_combined

5. Sample 25 posts per city to custom directory:
   python scripts/sample_all_data.py --samples-per-city 25 --output-dir small_sample

6. Use different data directory:
   python scripts/sample_all_data.py --data-dir /path/to/data --samples-per-city 75

OUTPUT FILES:
============

When sampling (--mode sample):
- gold_standard/sampled_twitter_posts.csv
- gold_standard/sampled_meeting_minutes.csv  
- gold_standard/sampled_reddit_comments.csv
- gold_standard/sampled_newspaper_articles.csv
- gold_standard/combined_sample.csv (with data_type column)

When copying all data (--mode all):
- output_dir/all_twitter_posts.csv
- output_dir/all_meeting_minutes.csv
- output_dir/all_reddit_comments.csv
- output_dir/all_newspaper_articles.csv

COMMAND LINE ARGUMENTS:
======================

--mode: 'sample' (default) or 'all'
--samples-per-city: Number of samples per city (default: 50)
--data-dir: Base data directory (default: data)
--output-dir: Output directory (default: gold_standard for samples, all_data for all data)

For help: python scripts/sample_all_data.py --help
"""

import os
import pandas as pd
import argparse
from utils import CITY_MAP

def create_output_directories(output_dirs):
    """Create output directories if they don't exist."""
    for dir_path in output_dirs:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Created directory: {dir_path}")

def sample_twitter_posts(base_data_dir='data', samples_per_city=50, output_file='gold_standard/sampled_twitter_posts.csv'):
    """Sample Twitter posts from each city."""
    all_samples = []
    for city, city_dir in CITY_MAP.items():
        x_dir = os.path.join(base_data_dir, city_dir, 'x')
        posts_path = os.path.join(x_dir, 'posts_english_2015-2025_rt_deidentified.csv')
        if not os.path.isfile(posts_path):
            print(f"File not found: {posts_path}")
            continue
        try:
            df = pd.read_csv(posts_path)
            if 'is_retweet' not in df.columns:
                print(f"'is_retweet' column not found in {posts_path}. Skipping.")
                continue
            non_rt = df[df['is_retweet'] == False]
            if non_rt.empty:
                print(f"No non-retweet tweets in {posts_path}.")
                continue
            sample = non_rt.sample(n=min(samples_per_city, len(non_rt)), random_state=42)
            sample['city'] = city
            sample['data_type'] = 'twitter'
            all_samples.append(sample)
        except Exception as e:
            print(f"Error processing {posts_path}: {e}")
    if all_samples:
        combined = pd.concat(all_samples, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"Twitter sample saved to {output_file} ({len(combined)} total samples)")
    else:
        print("No Twitter samples collected.")

def sample_meeting_minutes(base_data_dir='data', samples_per_city=50, output_file='gold_standard/sampled_meeting_minutes.csv'):
    """Sample meeting minutes from each city."""
    all_samples = []
    for city, city_dir in CITY_MAP.items():
        meeting_minutes_dir = os.path.join(base_data_dir, city_dir, 'meeting_minutes')
        meeting_minutes_path = os.path.join(meeting_minutes_dir, 'meeting_minutes_lexicon_matches_deidentified.csv')
        if not os.path.isfile(meeting_minutes_path):
            print(f"File not found: {meeting_minutes_path}")
            continue
        try:
            df = pd.read_csv(meeting_minutes_path)
            if 'Deidentified_paragraph' not in df.columns:
                print(f"'Deidentified_paragraph' column not found in {meeting_minutes_path}. Skipping.")
                continue
            if df.empty:
                print(f"No meeting minutes data in {meeting_minutes_path}.")
                continue
            sample = df.sample(n=min(samples_per_city, len(df)), random_state=42)
            sample['city'] = city
            sample['data_type'] = 'meeting_minutes'
            all_samples.append(sample)
        except Exception as e:
            print(f"Error processing {meeting_minutes_path}: {e}")
    if all_samples:
        combined = pd.concat(all_samples, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"Meeting minutes sample saved to {output_file} ({len(combined)} total samples)")
    else:
        print("No meeting minutes samples collected.")

def sample_reddit_comments(base_data_dir='data', samples_per_city=50, output_file='gold_standard/sampled_reddit_comments.csv'):
    """Sample Reddit comments from each city."""
    all_samples = []
    for city, city_dir in CITY_MAP.items():
        reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
        reddit_path = os.path.join(reddit_dir, 'filtered_comments_deidentified.csv')
        if not os.path.isfile(reddit_path):
            print(f"File not found: {reddit_path}")
            continue
        try:
            df = pd.read_csv(reddit_path)
            if 'Deidentified_Comment' not in df.columns:
                print(f"'Deidentified_Comment' column not found in {reddit_path}. Skipping.")
                continue
            if df.empty:
                print(f"No Reddit comments in {reddit_path}.")
                continue
            sample = df.sample(n=min(samples_per_city, len(df)), random_state=42)
            sample['city'] = city
            sample['data_type'] = 'reddit'
            all_samples.append(sample)
        except Exception as e:
            print(f"Error processing {reddit_path}: {e}")
    if all_samples:
        combined = pd.concat(all_samples, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"Reddit comments sample saved to {output_file} ({len(combined)} total samples)")
    else:
        print("No Reddit samples collected.")

def sample_newspaper_articles(base_data_dir='data', samples_per_city=50, output_file='gold_standard/sampled_newspaper_articles.csv'):
    """Sample newspaper articles from each city."""
    all_samples = []
    for city, city_dir in CITY_MAP.items():
        news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
        news_path = os.path.join(news_dir, f'{city_dir}_processed_articles_deidentified.csv')
        if not os.path.isfile(news_path):
            print(f"File not found: {news_path}")
            continue
        try:
            df = pd.read_csv(news_path)
            if df.empty:
                print(f"No rows in {news_path}.")
                continue
            sample = df.sample(n=min(samples_per_city, len(df)), random_state=42)
            sample['city'] = city
            sample['data_type'] = 'newspaper'
            all_samples.append(sample)
        except Exception as e:
            print(f"Error processing {news_path}: {e}")
    if all_samples:
        combined = pd.concat(all_samples, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"Newspaper articles sample saved to {output_file} ({len(combined)} total samples)")
    else:
        print("No newspaper samples collected.")

def copy_all_data(base_data_dir='data', output_dir='all_data'):
    """Copy all data (not samples) to separate CSV files by data type."""
    
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    # Twitter posts
    print("Copying all Twitter posts...")
    twitter_all = []
    for city, city_dir in CITY_MAP.items():
        x_dir = os.path.join(base_data_dir, city_dir, 'x')
        posts_path = os.path.join(x_dir, 'posts_english_2015-2025_rt_deidentified.csv')
        if os.path.isfile(posts_path):
            try:
                df = pd.read_csv(posts_path)
                if 'is_retweet' in df.columns:
                    non_rt = df[df['is_retweet'] == False]
                    if not non_rt.empty:
                        non_rt['city'] = city
                        twitter_all.append(non_rt)
                        print(f"  {city}: {len(non_rt)} Twitter posts")
            except Exception as e:
                print(f"Error processing Twitter for {city}: {e}")
    
    if twitter_all:
        twitter_combined = pd.concat(twitter_all, ignore_index=True)
        twitter_output = os.path.join(output_dir, 'all_twitter_posts.csv')
        twitter_combined.to_csv(twitter_output, index=False)
        print(f"Twitter posts saved to {twitter_output} ({len(twitter_combined)} total)")
    
    # Meeting minutes
    print("\nCopying all meeting minutes...")
    meeting_all = []
    for city, city_dir in CITY_MAP.items():
        meeting_minutes_dir = os.path.join(base_data_dir, city_dir, 'meeting_minutes')
        meeting_minutes_path = os.path.join(meeting_minutes_dir, 'meeting_minutes_lexicon_matches_deidentified.csv')
        if os.path.isfile(meeting_minutes_path):
            try:
                df = pd.read_csv(meeting_minutes_path)
                if 'Deidentified_paragraph' in df.columns and not df.empty:
                    df['city'] = city
                    meeting_all.append(df)
                    print(f"  {city}: {len(df)} meeting minutes")
            except Exception as e:
                print(f"Error processing meeting minutes for {city}: {e}")
    
    if meeting_all:
        meeting_combined = pd.concat(meeting_all, ignore_index=True)
        meeting_output = os.path.join(output_dir, 'all_meeting_minutes.csv')
        meeting_combined.to_csv(meeting_output, index=False)
        print(f"Meeting minutes saved to {meeting_output} ({len(meeting_combined)} total)")
    
    # Reddit comments
    print("\nCopying all Reddit comments...")
    reddit_all = []
    for city, city_dir in CITY_MAP.items():
        reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
        reddit_path = os.path.join(reddit_dir, 'filtered_comments_deidentified.csv')
        if os.path.isfile(reddit_path):
            try:
                df = pd.read_csv(reddit_path)
                if 'Deidentified_Comment' in df.columns and not df.empty:
                    df['city'] = city
                    reddit_all.append(df)
                    print(f"  {city}: {len(df)} Reddit comments")
            except Exception as e:
                print(f"Error processing Reddit for {city}: {e}")
    
    if reddit_all:
        reddit_combined = pd.concat(reddit_all, ignore_index=True)
        reddit_output = os.path.join(output_dir, 'all_reddit_comments.csv')
        reddit_combined.to_csv(reddit_output, index=False)
        print(f"Reddit comments saved to {reddit_output} ({len(reddit_combined)} total)")
    
    # Newspaper articles
    print("\nCopying all newspaper articles...")
    newspaper_all = []
    for city, city_dir in CITY_MAP.items():
        news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
        news_path = os.path.join(news_dir, f'{city_dir}_processed_articles_deidentified.csv')
        if os.path.isfile(news_path):
            try:
                df = pd.read_csv(news_path)
                if not df.empty:
                    df['city'] = city
                    newspaper_all.append(df)
                    print(f"  {city}: {len(df)} newspaper articles")
            except Exception as e:
                print(f"Error processing newspaper for {city}: {e}")
    
    if newspaper_all:
        newspaper_combined = pd.concat(newspaper_all, ignore_index=True)
        newspaper_output = os.path.join(output_dir, 'all_newspaper_articles.csv')
        newspaper_combined.to_csv(newspaper_output, index=False)
        print(f"Newspaper articles saved to {newspaper_output} ({len(newspaper_combined)} total)")
    
    # Summary
    print("\n" + "="*50)
    print("SUMMARY:")
    total_files = 0
    total_rows = 0
    
    if twitter_all:
        print(f"Twitter posts: {len(twitter_combined)} rows")
        total_files += 1
        total_rows += len(twitter_combined)
    
    if meeting_all:
        print(f"Meeting minutes: {len(meeting_combined)} rows")
        total_files += 1
        total_rows += len(meeting_combined)
    
    if reddit_all:
        print(f"Reddit comments: {len(reddit_combined)} rows")
        total_files += 1
        total_rows += len(reddit_combined)
    
    if newspaper_all:
        print(f"Newspaper articles: {len(newspaper_combined)} rows")
        total_files += 1
        total_rows += len(newspaper_combined)
    
    print(f"Total files created: {total_files}")
    print(f"Total rows across all files: {total_rows}")
    print(f"Output directory: {output_dir}")
    
    if total_files == 0:
        print("No data collected.")

def sample_all_data(base_data_dir='data', samples_per_city=50):
    """Sample all data types and create individual files plus a combined file."""
    
    # Define output directories and files
    output_dirs = ['gold_standard']
    output_files = [
        'gold_standard/sampled_twitter_posts.csv',
        'gold_standard/sampled_meeting_minutes.csv',
        'gold_standard/sampled_reddit_comments.csv',
        'gold_standard/sampled_newspaper_articles.csv',
        'gold_standard/combined_sample.csv'
    ]
    
    # Create output directories
    create_output_directories(output_dirs)
    
    # Sample each data type
    print(f"Sampling {samples_per_city} samples per city...")
    print("\nSampling Twitter posts...")
    sample_twitter_posts(base_data_dir, samples_per_city, output_files[0])
    
    print("\nSampling meeting minutes...")
    sample_meeting_minutes(base_data_dir, samples_per_city, output_files[1])
    
    print("\nSampling Reddit comments...")
    sample_reddit_comments(base_data_dir, samples_per_city, output_files[2])
    
    print("\nSampling newspaper articles...")
    sample_newspaper_articles(base_data_dir, samples_per_city, output_files[3])
    
    # Create combined file
    print("\nCreating combined sample file...")
    create_combined_sample(base_data_dir, samples_per_city, output_files[4])

def create_combined_sample(base_data_dir='data', samples_per_city=50, output_file='gold_standard/combined_sample.csv'):
    """Create a combined sample with all data types."""
    all_combined = []
    
    # Calculate samples per data type for balanced representation
    samples_per_data_type = samples_per_city // 4
    
    # Twitter posts
    for city, city_dir in CITY_MAP.items():
        x_dir = os.path.join(base_data_dir, city_dir, 'x')
        posts_path = os.path.join(x_dir, 'posts_english_2015-2025_rt_deidentified.csv')
        if os.path.isfile(posts_path):
            try:
                df = pd.read_csv(posts_path)
                if 'is_retweet' in df.columns:
                    non_rt = df[df['is_retweet'] == False]
                    if not non_rt.empty:
                        sample = non_rt.sample(n=min(samples_per_data_type, len(non_rt)), random_state=42)
                        sample['city'] = city
                        sample['data_type'] = 'twitter'
                        all_combined.append(sample)
            except Exception as e:
                print(f"Error processing Twitter for {city}: {e}")
    
    # Meeting minutes
    for city, city_dir in CITY_MAP.items():
        meeting_minutes_dir = os.path.join(base_data_dir, city_dir, 'meeting_minutes')
        meeting_minutes_path = os.path.join(meeting_minutes_dir, 'meeting_minutes_lexicon_matches_deidentified.csv')
        if os.path.isfile(meeting_minutes_path):
            try:
                df = pd.read_csv(meeting_minutes_path)
                if 'Deidentified_paragraph' in df.columns and not df.empty:
                    sample = df.sample(n=min(samples_per_data_type, len(df)), random_state=42)
                    sample['city'] = city
                    sample['data_type'] = 'meeting_minutes'
                    all_combined.append(sample)
            except Exception as e:
                print(f"Error processing meeting minutes for {city}: {e}")
    
    # Reddit comments
    for city, city_dir in CITY_MAP.items():
        reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
        reddit_path = os.path.join(reddit_dir, 'filtered_comments_deidentified.csv')
        if os.path.isfile(reddit_path):
            try:
                df = pd.read_csv(reddit_path)
                if 'Deidentified_Comment' in df.columns and not df.empty:
                    sample = df.sample(n=min(samples_per_data_type, len(df)), random_state=42)
                    sample['city'] = city
                    sample['data_type'] = 'reddit'
                    all_combined.append(sample)
            except Exception as e:
                print(f"Error processing Reddit for {city}: {e}")
    
    # Newspaper articles
    for city, city_dir in CITY_MAP.items():
        news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
        news_path = os.path.join(news_dir, f'{city_dir}_processed_articles_deidentified.csv')
        if os.path.isfile(news_path):
            try:
                df = pd.read_csv(news_path)
                if not df.empty:
                    sample = df.sample(n=min(samples_per_data_type, len(df)), random_state=42)
                    sample['city'] = city
                    sample['data_type'] = 'newspaper'
                    all_combined.append(sample)
            except Exception as e:
                print(f"Error processing newspaper for {city}: {e}")
    
    if all_combined:
        combined = pd.concat(all_combined, ignore_index=True)
        combined.to_csv(output_file, index=False)
        print(f"Combined sample saved to {output_file}")
        print(f"Total samples: {len(combined)}")
        print("Data type distribution:")
        print(combined['data_type'].value_counts())
        print("\nCity distribution:")
        print(combined['city'].value_counts())
    else:
        print("No samples collected for combined file.")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Sample or copy all data types from cities')
    
    parser.add_argument('--mode', choices=['sample', 'all'], default='sample',
                       help='Mode: "sample" for sampling data, "all" for copying all data (default: sample)')
    
    parser.add_argument('--samples-per-city', type=int, default=50,
                       help='Number of samples per city (default: 50)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    parser.add_argument('--output-dir', default=None,
                       help='Output directory (default: gold_standard for samples, complete_dataset for all data)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    # Set default output directory based on mode
    if args.output_dir is None:
        if args.mode == 'all':
            args.output_dir = 'complete_dataset'
        else:
            args.output_dir = 'gold_standard'
    
    print("=== Data Processing Script ===")
    print(f"Mode: {args.mode}")
    print(f"Base data directory: {args.data_dir}")
    
    if args.mode == 'all':
        print(f"Output directory: {args.output_dir}")
        print("=" * 30)
        copy_all_data(args.data_dir, args.output_dir)
    else:
        print(f"Samples per city: {args.samples_per_city}")
        print(f"Output directory: {args.output_dir}")
        print("=" * 30)
        sample_all_data(args.data_dir, args.samples_per_city) 