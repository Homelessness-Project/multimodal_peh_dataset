"""
Sample LexisNexis News Articles
===============================

This script samples news articles from processed deidentified articles,
while trying to keep the 5 few-shot examples from the original dataset.

USAGE:
======
python scripts/sample_news_with_processed_articles.py --samples-per-city 50
python scripts/sample_news_with_processed_articles.py --cities baltimore,portland
python scripts/sample_news_with_processed_articles.py --output-dir custom_samples

OUTPUT:
=======
- gold_standard/sampled_lexisnexis_news.csv
- gold_standard/few_shot_news_examples.csv (preserved examples)
"""

import os
import pandas as pd
import argparse
import random
from utils import CITY_MAP

# The 5 few-shot examples to search for in processed articles
FEW_SHOT_EXAMPLES = [
    "We applaud this important first step to assure the long-term resolution of homelessness.",
    "60 million for programs to support homeless veterans including 20 million for [ORGANIZATION]. The President proposed to eliminate the program.",
    "[ORGANIZATION] county commissioners on [ORGANIZATION] weighed options for creating a migrant support services center while city emergency managers opened a busing hub, as dozens of migrants remained in homeless conditions [LOCATION].",
    "About 1 in 3 people who are homeless in [ORGANIZATION] report having a mental illness or a substance use disorder, and the combination of homelessness and substance use or untreated mental illness has led to very public tragedies.",
    "I would imagine she is not being delusional about being unsafe on the streets, [ORGANIZATION], executive director of [ORGANIZATION], told [ORGANIZATION]. [PERSON] specializes in treating mentally ill homeless people. Somewhere in all of this is a hook around the fear she has of being unsafe, especially as a woman who is homeless, and that is not uncommon. There should be a real conversation about that, and it could be very useful for figuring out whats going on with her."
]

def create_output_directories(output_dirs):
    """Create output directories if they don't exist."""
    for dir_path in output_dirs:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Created directory: {dir_path}")

def find_few_shot_examples_in_articles(base_data_dir='data'):
    """Search for the few-shot examples in processed articles across all cities."""
    found_examples = []
    
    for city, city_dir in CITY_MAP.items():
        news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
        processed_path = os.path.join(news_dir, f'{city_dir}_processed_articles_deidentified.csv')
        
        if os.path.isfile(processed_path):
            try:
                processed_df = pd.read_csv(processed_path)
                if not processed_df.empty:
                    for _, row in processed_df.iterrows():
                        article_text = row['Deidentified_paragraph_text']
                        for example_text in FEW_SHOT_EXAMPLES:
                            # Clean both texts for comparison (remove extra whitespace, normalize)
                            clean_article = ' '.join(article_text.split())
                            clean_example = ' '.join(example_text.split())
                            
                            if clean_example in clean_article:
                                found_examples.append({
                                    'city': row['city'],
                                    'article_date': row['article_date'],
                                    'article_source': row['article_source'],
                                    'city_source': row['city_source'],
                                    'keywords_matched': row['keywords_matched'],
                                    'Deidentified_article_title': row['Deidentified_article_title'],
                                    'Deidentified_paragraph_text': article_text,
                                    'matched_example': example_text
                                })
                                print(f"Found few-shot example in {city}: {example_text[:100]}...")
                                break  # Found this example, move to next article
            except Exception as e:
                print(f"Error reading processed articles for {city}: {e}")
    
    return found_examples

def sample_news_with_processed_articles(base_data_dir='data', samples_per_city=50, 
                                      output_file='gold_standard/sampled_lexisnexis_news.csv',
                                      few_shot_file='gold_standard/few_shot_news_examples.csv'):
    """Sample LexisNexis news articles from processed deidentified articles."""
    
    # Search for few-shot examples in processed articles
    print("Searching for few-shot examples in processed articles...")
    few_shot_examples = find_few_shot_examples_in_articles(base_data_dir)
    print(f"Found {len(few_shot_examples)} few-shot examples in processed articles")
    
    # Save few-shot examples if found
    if few_shot_examples:
        few_shot_df = pd.DataFrame(few_shot_examples)
        few_shot_df.to_csv(few_shot_file, index=False)
        print(f"Few-shot examples saved to {few_shot_file}")
    else:
        print("No few-shot examples found in processed articles")
    
    all_samples = []
    total_processed = 0
    total_reddit = 0
    
    for city, city_dir in CITY_MAP.items():
        print(f"\nProcessing {city}...")
        
        # Check for processed articles
        news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
        processed_path = os.path.join(news_dir, f'{city_dir}_processed_articles_deidentified.csv')
        
        processed_df = None
        if os.path.isfile(processed_path):
            try:
                processed_df = pd.read_csv(processed_path)
                if not processed_df.empty:
                    total_processed += len(processed_df)
                    print(f"  Found {len(processed_df)} processed articles")
            except Exception as e:
                print(f"Error reading processed articles for {city}: {e}")
        
        # Check for Reddit comments
        reddit_dir = os.path.join(base_data_dir, city_dir, 'reddit')
        reddit_path = os.path.join(reddit_dir, 'filtered_comments_deidentified.csv')
        
        reddit_comments = []
        if os.path.isfile(reddit_path):
            try:
                reddit_df = pd.read_csv(reddit_path)
                if 'Deidentified_Comment' in reddit_df.columns and not reddit_df.empty:
                    reddit_comments = reddit_df['Deidentified_Comment'].dropna().tolist()
                    total_reddit += len(reddit_comments)
                    print(f"  Found {len(reddit_comments)} Reddit comments")
            except Exception as e:
                print(f"Error reading Reddit comments for {city}: {e}")
        
        # Sample from processed articles
        city_samples = []
        
        # Sample from processed articles
        if processed_df is not None and not processed_df.empty:
            article_sample_size = min(samples_per_city, len(processed_df))
            if article_sample_size > 0:
                sampled_rows = processed_df.sample(n=article_sample_size, random_state=42)
                for _, row in sampled_rows.iterrows():
                    city_samples.append({
                        'city': row['city'],
                        'article_date': row['article_date'],
                        'article_source': row['article_source'],
                        'city_source': row['city_source'],
                        'keywords_matched': row['keywords_matched'],
                        'Deidentified_article_title': row['Deidentified_article_title'],
                        'Deidentified_paragraph_text': row['Deidentified_paragraph_text']
                    })
        
        if city_samples:
            all_samples.extend(city_samples)
            print(f"  Sampled {len(city_samples)} items for {city}")
        else:
            print(f"  No samples collected for {city}")
    
    # Combine all samples
    if all_samples:
        combined_df = pd.DataFrame(all_samples)
        combined_df.to_csv(output_file, index=False)
        print(f"\nLexisNexis news sample saved to {output_file}")
        print(f"Total samples: {len(combined_df)}")
        print(f"Breakdown by article_source:")
        print(combined_df['article_source'].value_counts())
        print(f"Breakdown by city:")
        print(combined_df['city'].value_counts())
    else:
        print("No samples collected.")
    
    print(f"\nSummary:")
    print(f"Total processed articles found: {total_processed}")
    print(f"Few-shot examples found in processed articles: {len(few_shot_examples)}")
    print(f"Few-shot examples searched for: {len(FEW_SHOT_EXAMPLES)}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Sample LexisNexis news articles')
    
    parser.add_argument('--samples-per-city', type=int, default=50,
                       help='Number of samples per city (default: 50)')
    
    parser.add_argument('--cities', type=str, default=None,
                       help='Comma-separated list of cities to process (default: all cities)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    parser.add_argument('--output-dir', default='gold_standard',
                       help='Output directory (default: gold_standard)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    # Set random seed for reproducibility
    random.seed(42)
    
    # Create output directories
    create_output_directories([args.output_dir])
    
    # Determine cities to process
    if args.cities:
        cities_to_process = [city.strip() for city in args.cities.split(',')]
        # Filter CITY_MAP to only include specified cities
        filtered_city_map = {city: city_dir for city, city_dir in CITY_MAP.items() 
                           if city.lower() in [c.lower() for c in cities_to_process]}
        if not filtered_city_map:
            print(f"No valid cities found in: {args.cities}")
            exit(1)
        CITY_MAP_TO_USE = filtered_city_map
    else:
        CITY_MAP_TO_USE = CITY_MAP
    
    print("=== LexisNexis News Sampling ===")
    print(f"Samples per city: {args.samples_per_city}")
    print(f"Cities to process: {list(CITY_MAP_TO_USE.keys())}")
    print(f"Data directory: {args.data_dir}")
    print(f"Output directory: {args.output_dir}")
    print("=" * 50)
    
    # Run sampling
    output_file = os.path.join(args.output_dir, 'sampled_lexisnexis_news.csv')
    few_shot_file = os.path.join(args.output_dir, 'few_shot_news_examples.csv')
    
    sample_news_with_processed_articles(
        base_data_dir=args.data_dir,
        samples_per_city=args.samples_per_city,
        output_file=output_file,
        few_shot_file=few_shot_file
    ) 