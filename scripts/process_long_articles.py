"""
Process Long Newspaper Articles
==============================

This script processes newspaper articles that are too long by searching for lexicon words
and creating shorter, focused versions using spaCy sentence segmentation and paragraph
context detection. This is useful when articles don't break by paragraph and need to be 
shortened for analysis.

USAGE:
======
python scripts/process_long_articles.py --max-paragraphs 1
python scripts/process_long_articles.py --cities baltimore,portland
python scripts/process_long_articles.py --output-dir processed_articles
"""

import os
import pandas as pd
import argparse
import re
import spacy
from tqdm import tqdm
from utils import CITY_MAP, KEYWORDS

def load_spacy_model():
    """Load spaCy model for sentence segmentation."""
    try:
        nlp = spacy.load("en_core_web_sm")
        return nlp
    except OSError:
        print("Downloading spaCy model...")
        import subprocess
        subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
        return spacy.load("en_core_web_sm")

def detect_paragraphs(text):
    """Detect paragraph breaks in text using multiple methods."""
    if not isinstance(text, str):
        return []
    
    text = str(text).strip()
    if not text:
        return []
    
    # Method 1: Split by double newlines (most common paragraph break)
    paragraphs = re.split(r'\n\s*\n', text)
    
    # Method 2: If no double newlines, try single newlines
    if len(paragraphs) <= 1:
        paragraphs = re.split(r'\n+', text)
    
    # Method 3: If still no breaks, return None to indicate no paragraph breaks found
    if len(paragraphs) <= 1:
        return None
    
    # Clean up paragraphs
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    return paragraphs

def find_keyword_paragraphs(text, keywords, max_paragraphs=1, max_sentences=5, nlp=None):
    """Find paragraphs with keywords and return shortened version."""
    if not isinstance(text, str):
        return [""]
    
    text = str(text).strip()
    if not text:
        return [""]
    
    # Detect paragraphs
    paragraphs = detect_paragraphs(text)
    
    # If no paragraph breaks detected, fall back to sentence-based approach
    if paragraphs is None:
        return find_keyword_sentences(text, keywords, max_sentences, nlp)
    
    if len(paragraphs) <= max_paragraphs:
        return [text]
    
    # Find paragraphs with keywords
    keyword_paragraphs = []
    for i, para in enumerate(paragraphs):
        para_text = para.strip()
        # Check if paragraph contains any keywords
        for keyword in keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', para_text, re.IGNORECASE):
                keyword_paragraphs.append((i, para_text))
                break
    
    if not keyword_paragraphs:
        # If no keywords found, return first max_paragraphs
        return ['\n\n'.join(paragraphs[:max_paragraphs])]
    
    # Sort by paragraph index
    keyword_paragraphs.sort()
    
    # If we have multiple keyword paragraphs that are far apart, create separate segments
    segments = []
    current_group = [keyword_paragraphs[0]]
    
    for i in range(1, len(keyword_paragraphs)):
        current_idx = keyword_paragraphs[i-1][0]
        next_idx = keyword_paragraphs[i][0]
        
        # If paragraphs are more than max_paragraphs apart, start a new segment
        if next_idx - current_idx > max_paragraphs:
            # Create segment from current group
            if current_group:
                segment = create_segment_from_paragraphs(current_group, paragraphs, max_paragraphs)
                if segment:
                    segments.append(segment)
            current_group = [keyword_paragraphs[i]]
        else:
            current_group.append(keyword_paragraphs[i])
    
    # Add the last group
    if current_group:
        segment = create_segment_from_paragraphs(current_group, paragraphs, max_paragraphs)
        if segment:
            segments.append(segment)
    
    # Return list of segments instead of concatenated string
    if len(segments) > 1:
        return segments
    elif len(segments) == 1:
        return segments
    else:
        # Fallback to original logic if no segments created
        single_segment = create_single_segment_from_paragraphs(keyword_paragraphs, paragraphs, max_paragraphs)
        return [single_segment] if single_segment else [""]

def create_segment_from_paragraphs(keyword_paragraphs, paragraphs, max_paragraphs):
    """Create a single segment from a group of keyword paragraphs."""
    if not keyword_paragraphs:
        return ""
    
    # Find the range that covers all keyword paragraphs in this group
    keyword_indices = [idx for idx, _ in keyword_paragraphs]
    min_idx = min(keyword_indices)
    max_idx = max(keyword_indices)
    
    # Include some context around the keyword range
    start_idx = max(0, min_idx - 1)
    end_idx = min(len(paragraphs), max_idx + 2)
    
    # Get paragraphs in this window
    window_paragraphs = paragraphs[start_idx:end_idx]
    return '\n\n'.join(window_paragraphs)

def create_single_segment_from_paragraphs(keyword_paragraphs, paragraphs, max_paragraphs):
    """Create a single segment using the original logic."""
    # Find the best segment that includes keyword paragraphs
    best_segment = ""
    best_keyword_count = 0
    
    for i, (para_idx, para_text) in enumerate(keyword_paragraphs):
        # Calculate window around this keyword paragraph
        start_idx = max(0, para_idx - max_paragraphs // 2)
        end_idx = min(len(paragraphs), para_idx + max_paragraphs // 2)
        
        # Get paragraphs in this window
        window_paragraphs = paragraphs[start_idx:end_idx]
        segment_text = '\n\n'.join(window_paragraphs)
        
        # Count keywords in this segment
        keyword_count = sum(1 for k in KEYWORDS if re.search(r'\b' + re.escape(k) + r'\b', segment_text, re.IGNORECASE))
        
        if keyword_count > best_keyword_count:
            best_keyword_count = keyword_count
            best_segment = segment_text
    
    # If we found a good segment, return it
    if best_segment:
        return best_segment
    
    # Fallback: return first max_paragraphs that contain keywords
    result_paragraphs = []
    keyword_found = False
    
    for para in paragraphs[:max_paragraphs]:
        para_text = para.strip()
        has_keyword = any(re.search(r'\b' + re.escape(k) + r'\b', para_text, re.IGNORECASE) for k in KEYWORDS)
        
        if has_keyword:
            keyword_found = True
            result_paragraphs.append(para)
        elif keyword_found:
            # If we already found a keyword, we can include non-keyword paragraphs
            result_paragraphs.append(para)
        # If no keyword found yet, skip this paragraph
    
    if result_paragraphs:
        return '\n\n'.join(result_paragraphs)
    
    # If still no keywords, return first max_paragraphs
    return '\n\n'.join(paragraphs[:max_paragraphs])

def find_keyword_sentences(text, keywords, max_sentences=5, nlp=None):
    """Find sentences with keywords and return shortened version (fallback method)."""
    if not isinstance(text, str):
        return [""]
    
    text = str(text).strip()
    if not text:
        return [""]
    
    # Load spaCy model if not provided
    if nlp is None:
        nlp = load_spacy_model()
    
    # Process text with spaCy
    doc = nlp(text)
    sentences = list(doc.sents)
    
    if len(sentences) <= max_sentences:
        return [text]
    
    # Find sentences with keywords
    keyword_sentences = []
    for i, sent in enumerate(sentences):
        sent_text = sent.text.strip()
        # Check if sentence contains any keywords
        for keyword in keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', sent_text, re.IGNORECASE):
                keyword_sentences.append((i, sent_text))
                break
    
    if not keyword_sentences:
        # If no keywords found, return first max_sentences
        return [' '.join([sent.text.strip() for sent in sentences[:max_sentences]])]
    
    # Sort by sentence index
    keyword_sentences.sort()
    
    # If we have multiple keyword sentences that are far apart, create separate segments
    segments = []
    current_group = [keyword_sentences[0]]
    
    for i in range(1, len(keyword_sentences)):
        current_idx = keyword_sentences[i-1][0]
        next_idx = keyword_sentences[i][0]
        
        # If sentences are more than max_sentences apart, start a new segment
        if next_idx - current_idx > max_sentences:
            # Create segment from current group
            if current_group:
                segment = create_segment_from_sentences(current_group, sentences, max_sentences)
                if segment:
                    segments.append(segment)
            current_group = [keyword_sentences[i]]
        else:
            current_group.append(keyword_sentences[i])
    
    # Add the last group
    if current_group:
        segment = create_segment_from_sentences(current_group, sentences, max_sentences)
        if segment:
            segments.append(segment)
    
    # Return list of segments instead of concatenated string
    if len(segments) > 1:
        return segments
    elif len(segments) == 1:
        return segments
    else:
        # Fallback to original logic if no segments created
        single_segment = create_single_segment_from_sentences(keyword_sentences, sentences, max_sentences)
        return [single_segment] if single_segment else [""]

def create_segment_from_sentences(keyword_sentences, sentences, max_sentences):
    """Create a single segment from a group of keyword sentences."""
    if not keyword_sentences:
        return ""
    
    # Find the range that covers all keyword sentences in this group
    keyword_indices = [idx for idx, _ in keyword_sentences]
    min_idx = min(keyword_indices)
    max_idx = max(keyword_indices)
    
    # Include some context around the keyword range
    start_idx = max(0, min_idx - 1)
    end_idx = min(len(sentences), max_idx + 2)
    
    # Get sentences in this window
    window_sentences = sentences[start_idx:end_idx]
    return ' '.join([sent.text.strip() for sent in window_sentences])

def create_single_segment_from_sentences(keyword_sentences, sentences, max_sentences):
    """Create a single segment using the original logic."""
    # Find the best segment that includes keyword sentences
    best_segment = ""
    best_keyword_count = 0
    
    for i, (sent_idx, sent_text) in enumerate(keyword_sentences):
        # Calculate window around this keyword sentence
        start_idx = max(0, sent_idx - max_sentences // 2)
        end_idx = min(len(sentences), sent_idx + max_sentences // 2)
        
        # Get sentences in this window
        window_sentences = sentences[start_idx:end_idx]
        segment_text = ' '.join([sent.text.strip() for sent in window_sentences])
        
        # Count keywords in this segment
        keyword_count = sum(1 for k in KEYWORDS if re.search(r'\b' + re.escape(k) + r'\b', segment_text, re.IGNORECASE))
        
        if keyword_count > best_keyword_count:
            best_keyword_count = keyword_count
            best_segment = segment_text
    
    # If we found a good segment, return it
    if best_segment:
        return best_segment
    
    # Fallback: return first max_sentences that contain keywords
    result_sentences = []
    keyword_found = False
    
    for sent in sentences[:max_sentences]:
        sent_text = sent.text.strip()
        has_keyword = any(re.search(r'\b' + re.escape(k) + r'\b', sent_text, re.IGNORECASE) for k in KEYWORDS)
        
        if has_keyword:
            keyword_found = True
            result_sentences.append(sent_text)
        elif keyword_found:
            # If we already found a keyword, we can include non-keyword sentences
            result_sentences.append(sent_text)
        # If no keyword found yet, skip this sentence
    
    if result_sentences:
        return ' '.join(result_sentences)
    
    # If still no keywords, return first max_sentences
    return ' '.join([sent.text.strip() for sent in sentences[:max_sentences]])

def process_articles_for_city(city, city_dir, base_data_dir, max_paragraphs, max_sentences, nlp=None):
    """Process articles for a specific city."""
    news_dir = os.path.join(base_data_dir, city_dir, 'newspaper')
    news_path = os.path.join(news_dir, f'{city_dir}_filtered.csv')
    
    if not os.path.isfile(news_path):
        print(f"File not found: {news_path}")
        return None
    
    try:
        df = pd.read_csv(news_path)
        if df.empty:
            print(f"No articles in {news_path}")
            return None
        
        print(f"Processing {city}: {len(df)} articles")
        
        # Find the content column (could be 'content', 'text', 'article_text', etc.)
        content_col = None
        for col in ['paragraph_text', 'content', 'text', 'article_text', 'body', 'article_body']:
            if col in df.columns:
                content_col = col
                break
        
        if not content_col:
            print(f"No content column found in {news_path}")
            return None
        
        # Process each article with progress bar
        processed_articles = []
        shortened_count = 0
        paragraph_method_count = 0
        sentence_method_count = 0
        new_entries_count = 0  # Track how many new entries created
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {city}"):
            original_text = row[content_col]
            processed_segments = find_keyword_paragraphs(original_text, KEYWORDS, max_paragraphs, max_sentences, nlp)
            
            # Detect which method was used
            paragraphs = detect_paragraphs(str(original_text))
            used_sentence_method = paragraphs is None
            
            # Count paragraphs/sentences in original text
            if used_sentence_method:
                original_doc = nlp(str(original_text))
                original_count = len(list(original_doc.sents))
                sentence_method_count += 1
            else:
                original_paragraphs = detect_paragraphs(str(original_text))
                original_count = len(original_paragraphs) if original_paragraphs else 0
                paragraph_method_count += 1
            
            # Create a separate row for each segment
            for segment_idx, processed_text in enumerate(processed_segments):
                # Count paragraphs/sentences in processed text
                if used_sentence_method:
                    processed_doc = nlp(processed_text)
                    processed_count = len(list(processed_doc.sents))
                else:
                    processed_paragraphs = detect_paragraphs(processed_text)
                    processed_count = len(processed_paragraphs) if processed_paragraphs else 0
                
                # Create new row with processed text
                new_row = row.copy()
                new_row[content_col] = processed_text
                new_row['original_count'] = original_count
                new_row['processed_count'] = processed_count
                new_row['was_shortened'] = original_count > (max_paragraphs if not used_sentence_method else max_sentences)
                new_row['used_sentence_method'] = used_sentence_method
                new_row['segment_count'] = len(processed_segments)  # Total segments for this article
                new_row['segment_index'] = segment_idx  # Which segment this is (0-based)
                
                processed_articles.append(new_row)
                
                if original_count > (max_paragraphs if not used_sentence_method else max_sentences):
                    shortened_count += 1
            
            # Count additional segments created (beyond the original 1)
            if len(processed_segments) > 1:
                new_entries_count += len(processed_segments) - 1
        
        # Create processed dataframe
        processed_df = pd.DataFrame(processed_articles)
        
        # Save to same directory as input file
        output_path = os.path.join(news_dir, f'{city_dir}_processed_articles.csv')
        processed_df.to_csv(output_path, index=False)
        
        print(f"  Processed: {len(processed_df)} articles")
        print(f"  Shortened: {shortened_count} articles")
        print(f"  Used paragraph method: {paragraph_method_count} articles")
        print(f"  Used sentence method: {sentence_method_count} articles")
        print(f"  New entries created: {new_entries_count} additional segments")
        print(f"  Total segments: {len(processed_df)}")
        print(f"  Saved to: {output_path}")
        
        return processed_df
        
    except Exception as e:
        print(f"Error processing {news_path}: {e}")
        return None

def process_all_articles(base_data_dir='data', max_paragraphs=1, max_sentences=5, cities=None):
    """Process articles for all cities or specified cities."""
    
    # Load spaCy model once for all processing
    print("Loading spaCy model...")
    nlp = load_spacy_model()
    
    # Determine which cities to process
    if cities:
        cities_to_process = [city.strip() for city in cities.split(',')]
    else:
        cities_to_process = list(CITY_MAP.keys())
    
    print(f"Processing articles for {len(cities_to_process)} cities")
    print(f"Maximum paragraphs: {max_paragraphs}")
    print(f"Maximum sentences (fallback): {max_sentences}")
    print("=" * 50)
    
    all_processed = []
    total_articles = 0
    total_shortened = 0
    total_new_entries = 0
    
    for city in cities_to_process:
        if city not in CITY_MAP:
            print(f"City '{city}' not found in CITY_MAP, skipping...")
            continue
            
        city_dir = CITY_MAP[city]
        processed_df = process_articles_for_city(city, city_dir, base_data_dir, max_paragraphs, max_sentences, nlp)
        
        if processed_df is not None:
            all_processed.append(processed_df)
            total_articles += len(processed_df)
            total_shortened += processed_df['was_shortened'].sum()
            # Count new entries by counting segments with segment_index > 0
            if 'segment_index' in processed_df.columns:
                total_new_entries += len(processed_df[processed_df['segment_index'] > 0])
    
    # Create combined file if multiple cities processed
    if len(all_processed) > 1:
        combined_df = pd.concat(all_processed, ignore_index=True)
        combined_path = os.path.join(base_data_dir, 'all_processed_articles.csv')
        combined_df.to_csv(combined_path, index=False)
        print(f"\nCombined file saved to: {combined_path}")
    
    # Summary statistics
    print("\n" + "=" * 50)
    print("PROCESSING SUMMARY:")
    print(f"Total articles processed: {total_articles}")
    print(f"Articles shortened: {total_shortened}")
    print(f"New entries created: {total_new_entries} additional segments")
    print(f"Total segments available: {total_articles}")
    print(f"Shortening rate: {total_shortened/total_articles*100:.1f}%" if total_articles > 0 else "No articles processed")
    print(f"Segmentation rate: {total_new_entries/total_articles*100:.1f}%" if total_articles > 0 else "No articles processed")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process long newspaper articles by shortening them around keywords using paragraph context detection with sentence fallback')
    
    parser.add_argument('--max-paragraphs', type=int, default=1,
                       help='Maximum number of paragraphs per article (default: 1)')
    
    parser.add_argument('--max-sentences', type=int, default=5,
                       help='Maximum number of sentences per article when no paragraph breaks found (default: 5)')
    
    parser.add_argument('--cities', type=str, default=None,
                       help='Comma-separated list of cities to process (default: all cities)')
    
    parser.add_argument('--data-dir', default='data',
                       help='Base data directory (default: data)')
    
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    
    print("=== Long Article Processing Script (Paragraph Context + Sentence Fallback) ===")
    print(f"Maximum paragraphs: {args.max_paragraphs}")
    print(f"Maximum sentences (fallback): {args.max_sentences}")
    print(f"Data directory: {args.data_dir}")
    if args.cities:
        print(f"Cities to process: {args.cities}")
    else:
        print("Processing all cities")
    print("=" * 30)
    
    process_all_articles(args.data_dir, args.max_paragraphs, args.max_sentences, args.cities) 