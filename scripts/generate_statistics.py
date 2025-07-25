import pandas as pd
import os
import re
from collections import Counter
import json
from datetime import datetime
import numpy as np
from utils import KEYWORDS

def analyze_lexicon_matches(file_path):
    """Analyze lexicon matches in meeting minutes files"""
    if not os.path.exists(file_path):
        return None
    
    df = pd.read_csv(file_path)
    
    # Count total matches
    total_matches = len(df)
    
    # Analyze matched words
    all_matched_words = []
    for words in df['matched_words'].dropna():
        if pd.notna(words) and words.strip():
            # Split by semicolon and clean up
            word_list = [word.strip().lower() for word in words.split(';') if word.strip()]
            all_matched_words.extend(word_list)
    
    word_counts = Counter(all_matched_words)
    
    # Get unique files
    unique_files = df['filename'].nunique() if 'filename' in df.columns else 0
    
    return {
        'total_matches': int(total_matches),
        'unique_files': int(unique_files),
        'word_counts': dict(word_counts),
        'total_unique_words': int(len(word_counts))
    }

def count_keyword_matches(text_column, keywords):
    """Count how many entries contain each keyword"""
    if text_column is None or text_column.empty:
        return {}
    
    keyword_counts = {}
    for keyword in keywords:
        # Case insensitive search
        matches = text_column.str.contains(keyword, case=False, na=False)
        count = matches.sum()
        keyword_counts[keyword] = int(count)
    
    return keyword_counts

def create_subfolder_statistics(city_name, source_name):
    """Create statistics.csv for a specific subfolder"""
    subfolder_path = f"data/{city_name}/{source_name}"
    if not os.path.exists(subfolder_path):
        return
    
    stats_rows = []
    
    # Analyze each CSV file in the subfolder
    for file in os.listdir(subfolder_path):
        if file.endswith('.csv'):
            file_path = os.path.join(subfolder_path, file)
            
            # Basic file stats
            file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)
            df = pd.read_csv(file_path)
            
            row = {
                'file': file,
                'file_size_mb': file_size_mb,
                'total_rows': int(len(df))
            }
            
            # Count keyword matches for text columns
            text_columns = []
            if source_name == 'reddit':
                text_columns = ['Submission Title', 'Comment']
            elif source_name == 'x':
                text_columns = ['text']
            elif source_name == 'newspaper':
                text_columns = ['article_title', 'paragraph_text']
            elif source_name == 'meeting_minutes':
                text_columns = ['paragraph']
            
            for col in text_columns:
                if col in df.columns:
                    keyword_counts = count_keyword_matches(df[col], KEYWORDS)
                    for keyword, count in keyword_counts.items():
                        row[f'{col}_{keyword}_count'] = count
            
            # Special handling for lexicon matches files
            if 'lexicon_matches' in file:
                lexicon_stats = analyze_lexicon_matches(file_path)
                if lexicon_stats:
                    row.update({
                        'total_matches': lexicon_stats['total_matches'],
                        'unique_files': lexicon_stats['unique_files'],
                        'total_unique_words': lexicon_stats['total_unique_words']
                    })
                    
                    # Add top 10 most frequent words
                    if lexicon_stats['word_counts']:
                        top_words = sorted(lexicon_stats['word_counts'].items(), 
                                         key=lambda x: x[1], reverse=True)[:10]
                        for i, (word, count) in enumerate(top_words):
                            row[f'word_{i+1}'] = word
                            row[f'word_{i+1}_count'] = count
            
            stats_rows.append(row)
    
    if stats_rows:
        df_stats = pd.DataFrame(stats_rows)
        output_file = os.path.join(subfolder_path, "statistics.csv")
        df_stats.to_csv(output_file, index=False)
        print(f"Statistics saved to: {output_file}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate statistics for city data sources")
    parser.add_argument('--city', type=str, help='Specific city to analyze (e.g., southbend)')
    parser.add_argument('--all', action='store_true', help='Analyze all cities')
    
    args = parser.parse_args()
    
    if args.city:
        # Analyze specific city
        city_name = args.city
        sources = ['reddit', 'x', 'newspaper', 'meeting_minutes']
        
        for source in sources:
            create_subfolder_statistics(city_name, source)
    
    elif args.all:
        # Analyze all cities
        data_dir = "data"
        cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
        sources = ['reddit', 'x', 'newspaper', 'meeting_minutes']
        
        for city in cities:
            print(f"\nProcessing {city}...")
            for source in sources:
                create_subfolder_statistics(city, source)
    
    else:
        # Default to all cities
        print("Analyzing all cities by default...")
        data_dir = "data"
        cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
        sources = ['reddit', 'x', 'newspaper', 'meeting_minutes']
        
        for city in cities:
            print(f"\nProcessing {city}...")
            for source in sources:
                create_subfolder_statistics(city, source)

if __name__ == "__main__":
    main() 