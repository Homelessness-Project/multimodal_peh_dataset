"""
Twitter Resampling with Few-Shot Examples
========================================

This script resamples Twitter data while keeping 5 specific few-shot examples
that are important for the analysis.

USAGE:
======
python scripts/resample_twitter_with_fewshot.py --samples-per-city 50 --output-file gold_standard/resampled_twitter_with_fewshot.csv

The script will:
1. Find the 5 specific few-shot examples in the data
2. Remove them from the sampling pool
3. Sample the remaining data
4. Add the 5 examples back to the final output
"""

import os
import pandas as pd
import argparse
from utils import CITY_MAP

# The 5 few-shot examples to preserve
FEW_SHOT_EXAMPLES = [
    "[PERSON] awarded $100,000 to [PERSON] (ORG3) to enhance employment and education-related skills for [DATE] and migrant farmworkers. The award was part of a $300,000 discretionary fund award under the CSBG Program. [PERSON]",
    
    "\"Did your Black flunky mayor get the🐀[ORGANIZATION]'s memo 2 stick it 2 Rump instead of serving you by refusing 2 deport migrants + give them Black taxpayers'💰 4 shelter+food while Black citizens go homeless? [ORGANIZATION] mayors did. Charity starts at 🏠. [URL]\"",
    
    "PERSON0 Instead of peacocking on social media for your next job, how about you concentrate on the gaggles of homeless people in [ORGANIZATION]?",
    
    "[ORGANIZATION] Just what [ORGANIZATION] needs...another beggar.",
    
    "[ORGANIZATION] area in [ORGANIZATION] is facing a housing crisis. 40% of people in this area live in poverty, and the city lacks 20,000 affordable housing units. Initiatives like [ORGANIZATION] to fix old housing, but progress depends on securing funding. [URL]"
]

def find_few_shot_examples(df, few_shot_texts, similarity_threshold=0.9):
    """Find the first unique few-shot example in the dataframe using 90% similarity matching."""
    found_indices = []
    found_texts = []
    
    for i, target_text in enumerate(few_shot_texts):
        best_match_idx = None
        best_similarity = 0
        
        # Calculate similarity with each row in the dataframe
        for idx, row in df.iterrows():
            # Skip if this index is already used
            if idx in found_indices:
                continue
                
            actual_text = row['Deidentified_text']
            similarity = calculate_similarity(target_text, actual_text)
            
            if similarity > best_similarity and similarity >= similarity_threshold:
                best_similarity = similarity
                best_match_idx = idx
        
        if best_match_idx is not None:
            found_indices.append(best_match_idx)
            found_texts.append(target_text)
            print(f"Found few-shot example {i+1} (similarity: {best_similarity:.2f}): {target_text[:100]}...")
        else:
            print(f"Warning: Few-shot example {i+1} not found (best similarity: {best_similarity:.2f})")
    
    return found_indices, found_texts

def remove_twitter_duplicates(df, similarity_threshold=0.4):
    """Remove duplicate tweets that are not retweets using fuzzy matching."""
    if df.empty:
        return df
    
    # For tweets that are not retweets, we want to remove similar duplicates
    # For retweets, we'll keep them as they represent different instances
    # Handle both string and boolean values for is_retweet
    non_retweet_mask = df['is_retweet'].isin([False, 'False'])
    
    # Find duplicates among non-retweets
    non_retweet_df = df[non_retweet_mask].copy()
    retweet_df = df[~non_retweet_mask].copy()
    
    if non_retweet_df.empty:
        return df
    
    # First, remove exact duplicates based on Deidentified_text
    print(f"Before exact deduplication: {len(non_retweet_df)} non-retweets")
    non_retweet_df = non_retweet_df.drop_duplicates(subset=['Deidentified_text'], keep='first')
    print(f"After exact deduplication: {len(non_retweet_df)} non-retweets")
    
    # Remove fuzzy duplicates from non-retweets
    to_keep = []
    to_remove = set()
    
    for i, row1 in non_retweet_df.iterrows():
        if i in to_remove:
            continue
            
        # Check if this tweet is similar to any we've already decided to keep
        should_keep = True
        for keep_idx in to_keep:
            row2 = non_retweet_df.loc[keep_idx]
            similarity = calculate_similarity(row1['Deidentified_text'], row2['Deidentified_text'])
            if similarity >= similarity_threshold:
                should_keep = False
                break
        
        if should_keep:
            to_keep.append(i)
        else:
            to_remove.add(i)
    
    # Keep only the first occurrence of each similar group
    non_retweet_deduplicated = non_retweet_df.loc[to_keep].copy()
    
    # Combine retweets and deduplicated non-retweets
    deduplicated_df = pd.concat([retweet_df, non_retweet_deduplicated], ignore_index=True)
    
    # Sort by created_at to maintain chronological order
    deduplicated_df = deduplicated_df.sort_values('created_at')
    
    return deduplicated_df

def calculate_similarity(text1, text2):
    """Calculate similarity between two texts using word-based comparison."""
    if pd.isna(text1) or pd.isna(text2):
        return 0
    
    text1 = str(text1).strip()
    text2 = str(text2).strip()
    
    if not text1 or not text2:
        return 0
    
    # Convert to lowercase and split into words
    text1_lower = text1.lower()
    text2_lower = text2.lower()
    
    # Remove common placeholders that don't contribute to meaningful similarity
    placeholders = ['[organization]', '[user]', '[url]', '[person]', '[location]', '[time]', '[date]', '[address]', '[street]']
    for placeholder in placeholders:
        text1_lower = text1_lower.replace(placeholder, '')
        text2_lower = text2_lower.replace(placeholder, '')
    
    # Split into words and remove empty strings
    words1 = [w for w in text1_lower.split() if w]
    words2 = [w for w in text2_lower.split() if w]
    
    if not words1 or not words2:
        return 0
    
    # Calculate word-level similarity
    common_words = set(words1) & set(words2)
    total_unique_words = len(set(words1) | set(words2))
    
    if total_unique_words == 0:
        return 0
    
    return len(common_words) / total_unique_words

def resample_twitter_with_fewshot(base_data_dir='data', samples_per_city=50, 
                                output_file='gold_standard/resampled_twitter_with_fewshot.csv',
                                few_shot_examples=FEW_SHOT_EXAMPLES, similarity_threshold=0.9):
    """Resample Twitter posts while preserving specific few-shot examples."""
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    
    # First, collect all non-retweet data from all cities
    all_non_rt_data = []
    city_data_mapping = {}  # Track which data belongs to which city
    
    print("Collecting all non-retweet data...")
    for city, city_dir in CITY_MAP.items():
        x_dir = os.path.join(base_data_dir, city_dir, 'x')
        posts_path = os.path.join(x_dir, 'posts_english_2015-2025_rt_deidentified.csv')
        
        if not os.path.isfile(posts_path):
            print(f"File not found: {posts_path}")
            continue
            
        try:
            df = pd.read_csv(posts_path)
            
            if 'is_retweet' not in df.columns or 'Deidentified_text' not in df.columns:
                print(f"Required columns not found in {posts_path}. Skipping.")
                continue
            
            # Convert is_retweet to string to handle boolean values properly
            df['is_retweet'] = df['is_retweet'].astype(str)
            
            # Filter for non-retweets
            non_rt = df[df['is_retweet'] != 'True']
            
            if not non_rt.empty:
                non_rt['city'] = city
                all_non_rt_data.append(non_rt)
                city_data_mapping[city] = non_rt
                print(f"  {city}: {len(non_rt)} non-retweet posts")
            
        except Exception as e:
            print(f"Error processing {posts_path}: {e}")
    
    if not all_non_rt_data:
        print("No non-retweet data found.")
        return
    
    # Combine all data
    all_data = pd.concat(all_non_rt_data, ignore_index=True)
    print(f"\nTotal non-retweet posts across all cities: {len(all_data)}")
    
    # Find few-shot examples globally
    print("\nFinding few-shot examples globally...")
    few_shot_indices, found_texts = find_few_shot_examples(all_data, few_shot_examples, similarity_threshold)
    
    # Report few-shot coverage
    few_shot_found = len(few_shot_indices)
    if few_shot_found == 5:
        print(f"✓ All 5 few-shot examples found")
    elif few_shot_found > 0:
        print(f"⚠ {few_shot_found}/5 few-shot examples found")
    else:
        print(f"✗ No few-shot examples found")
    
    # Remove few-shot examples from all data
    remaining_data = all_data.drop(few_shot_indices)
    print(f"Remaining posts after removing few-shot examples: {len(remaining_data)}")
    
    # Sample per city from remaining data
    all_samples = []
    for city, city_dir in CITY_MAP.items():
        if city not in city_data_mapping:
            continue
            
        # Get data for this city (excluding few-shot examples)
        city_data = remaining_data[remaining_data['city'] == city]
        
        if len(city_data) > 0:
            sample_size = min(samples_per_city, len(city_data))
            sample = city_data.sample(n=sample_size, random_state=42)
            all_samples.append(sample)
            print(f"  {city}: {len(sample)} sampled posts")
        else:
            print(f"  {city}: No data available for sampling")
    
    # Combine all samples
    if all_samples:
        combined_samples = pd.concat(all_samples, ignore_index=True)
        print(f"\nTotal sampled posts: {len(combined_samples)}")
    else:
        combined_samples = pd.DataFrame()
        print("No samples collected.")
    
    # Get few-shot examples
    if few_shot_indices:
        few_shot_data = all_data.loc[few_shot_indices].copy()
        print(f"Few-shot examples: {len(few_shot_data)}")
        
        # Combine samples and few-shot examples
        final_combined = pd.concat([combined_samples, few_shot_data], ignore_index=True)
    else:
        final_combined = combined_samples
        print("No few-shot examples found.")
    
    # Save to file
    if not final_combined.empty:
        # Remove duplicates from final output with lower threshold for testing
        original_count = len(final_combined)
        print(f"\nApplying fuzzy deduplication (threshold: 0.8)...")
        final_combined = remove_twitter_duplicates(final_combined, similarity_threshold=0.8)
        deduplicated_count = len(final_combined)
        removed_count = original_count - deduplicated_count
        
        if removed_count > 0:
            print(f"✓ Removed {removed_count} duplicate tweets from final output")
        else:
            print("No duplicates found with current threshold")
        
        final_combined.to_csv(output_file, index=False)
        print(f"\nResampled Twitter data saved to {output_file}")
        print(f"Total posts: {len(final_combined)}")
        print(f"Sampled posts: {len(combined_samples)}")
        print(f"Few-shot examples: {len(few_shot_data) if few_shot_indices else 0}")
        
        # Show distribution
        if 'city' in final_combined.columns:
            print("\nCity distribution:")
            print(final_combined['city'].value_counts())
        
        # Show few-shot summary
        if few_shot_indices:
            print(f"\nFew-shot summary:")
            print(f"  Total few-shot examples: {len(few_shot_data)}")
            print(f"  Cities with few-shot examples: {few_shot_data['city'].nunique()}")
    else:
        print("No data to save.")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Resample Twitter data while preserving few-shot examples')
    
    parser.add_argument('--samples-per-city', type=int, default=50,
                       help='Number of samples per city (default: 50)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    parser.add_argument('--output-file', default='gold_standard/resampled_twitter_with_fewshot.csv',
                       help='Output file path (default: gold_standard/resampled_twitter_with_fewshot.csv)')
    
    parser.add_argument('--similarity-threshold', type=float, default=0.9,
                       help='Similarity threshold for finding few-shot examples (default: 0.9)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    print("=== Twitter Resampling with Few-Shot Examples ===")
    print(f"Samples per city: {args.samples_per_city}")
    print(f"Base data directory: {args.data_dir}")
    print(f"Output file: {args.output_file}")
    print(f"Similarity threshold: {args.similarity_threshold}")
    print("=" * 50)
    
    resample_twitter_with_fewshot(
        base_data_dir=args.data_dir,
        samples_per_city=args.samples_per_city,
        output_file=args.output_file,
        similarity_threshold=args.similarity_threshold
    ) 