import os
import csv
import re
import argparse
from collections import defaultdict
from xml.etree import ElementTree as ET
from glob import glob
from utils import KEYWORDS, CITY_MAP

# Increase CSV field size limit
csv.field_size_limit(2147483647)  # Maximum value for 32-bit systems

# Compile regex for keywords (case-insensitive, word boundaries for single words)
KEYWORD_PATTERNS = [
    re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE) if ' ' not in kw else re.compile(re.escape(kw), re.IGNORECASE)
    for kw in KEYWORDS
]

def extract_paragraphs(xml_text):
    try:
        # Remove any leading/trailing quotes and whitespace
        xml_text = xml_text.strip('"').strip()
        
        # First try to find <bodyText>...</bodyText>
        body_match = re.search(r'<bodyText>(.*?)</bodyText>', xml_text, re.DOTALL)
        if not body_match:
            # If no bodyText tags, try to find any <p> tags in the text
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', xml_text, re.DOTALL)
        else:
            body_text = body_match.group(1)
            # Find all <p> tags within bodyText
            paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', body_text, re.DOTALL)
        
        # Clean up paragraphs
        cleaned_paragraphs = []
        for p in paragraphs:
            # Remove any remaining XML tags
            p = re.sub(r'<[^>]+>', '', p)
            # Remove URLs
            p = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', p)
            # Remove extra whitespace and normalize spaces
            p = ' '.join(p.split())
            # Remove any remaining special characters
            p = re.sub(r'[^\w\s.,!?-]', '', p)
            if p.strip() and len(p.strip()) > 10:  # Only keep non-empty paragraphs with reasonable length
                cleaned_paragraphs.append(p.strip())
        
        return cleaned_paragraphs
    except Exception as e:
        print(f"Error extracting paragraphs: {str(e)}")
        return []

def find_keywords(paragraph):
    matches = [kw for kw, pat in zip(KEYWORDS, KEYWORD_PATTERNS) if pat.search(paragraph)]
    return matches

def process_file(city, input_path, output_path, stats):
    try:
        with open(input_path, newline='', encoding='utf-8') as infile, \
             open(output_path, 'w', newline='', encoding='utf-8') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = ['city', 'article_title', 'article_date', 'article_source', 'city_source', 'paragraph_text', 'keywords_matched']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            article_count = 0
            article_with_match = 0
            paragraph_match_count = 0
            keyword_counter = defaultdict(int)
            error_count = 0
            empty_text_count = 0

            for row in reader:
                try:
                    article_count += 1
                    title = row.get('Title', '').strip()
                    date = row.get('Date', '').strip()
                    source = row.get('Source', '').strip()
                    city_source = row.get('City Source', '')
                    full_text = row.get('Full Text', '')
                    
                    if not full_text:
                        empty_text_count += 1
                        continue
                        
                    paragraphs = extract_paragraphs(full_text)
                    found_in_article = False
                    
                    for para in paragraphs:
                        keywords = find_keywords(para)
                        if keywords:
                            found_in_article = True
                            paragraph_match_count += 1
                            for kw in keywords:
                                keyword_counter[kw] += 1
                            writer.writerow({
                                'city': city,
                                'article_title': title,
                                'article_date': date,
                                'article_source': source,
                                'city_source': city_source,
                                'paragraph_text': para,
                                'keywords_matched': ', '.join(keywords)
                            })
                    
                    if found_in_article:
                        article_with_match += 1
                        
                except Exception as e:
                    error_count += 1
                    print(f"Error processing article {article_count} in {city}: {str(e)}")
                    continue

            stats[city] = {
                'articles_processed': article_count,
                'articles_with_match': article_with_match,
                'paragraphs_matched': paragraph_match_count,
                'keyword_counts': dict(keyword_counter),
                'error_count': error_count,
                'empty_text_count': empty_text_count
            }
            
    except Exception as e:
        print(f"Error processing file {input_path}: {str(e)}")
        stats[city] = {
            'articles_processed': 0,
            'articles_with_match': 0,
            'paragraphs_matched': 0,
            'keyword_counts': {},
            'error_count': 1,
            'empty_text_count': 0
        }

def main():
    parser = argparse.ArgumentParser(description='Filter LexisNexis CSVs by paragraph and keywords.')
    parser.add_argument('--cities', nargs='*', default=None, help='Cities to process (default: all with lexisnexis.csv)')
    parser.add_argument('--data_dir', default='data', help='Base data directory')
    args = parser.parse_args()

    # Find all lexisnexis.csv files
    if args.cities:
        city_files = [(city, os.path.join(args.data_dir, CITY_MAP.get(city.lower(), city.lower()), 'newspaper', 'lexisnexis.csv')) for city in args.cities]
    else:
        city_files = []
        for path in glob(os.path.join(args.data_dir, '*', 'newspaper', 'lexisnexis.csv')):
            city = os.path.basename(os.path.dirname(os.path.dirname(path)))
            city_files.append((city, path))

    stats = {}
    for city, input_path in city_files:
        if not os.path.exists(input_path):
            print(f"File not found: {input_path}")
            continue
        print(f"Processing {city}...")

        # Map city to directory name
        city_dir = CITY_MAP.get(city.lower(), city.lower())
        output_dir = os.path.join(args.data_dir, city_dir, 'newspaper')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f'{city_dir}_filtered.csv')

        process_file(city, input_path, output_path, stats)
        print(f"  Output: {output_path}")

    # Print summary stats
    print("\nSummary statistics:")
    for city, s in stats.items():
        print(f"City: {city}")
        print(f"  Articles processed: {s['articles_processed']}")
        print(f"  Articles with at least one match: {s['articles_with_match']}")
        print(f"  Matching paragraphs: {s['paragraphs_matched']}")
        print(f"  Error count: {s.get('error_count', 0)}")
        print(f"  Empty text count: {s.get('empty_text_count', 0)}")
        print(f"  Keyword counts: {s['keyword_counts']}")
        print()

    # Write summary stats to CSV in data/data_summary
    summary_dir = os.path.join(args.data_dir, 'data_summary')
    os.makedirs(summary_dir, exist_ok=True)
    summary_path = os.path.join(summary_dir, 'lexisnexis_paragraph_filter_summary_stats.csv')
    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(['City', 'Articles Processed', 'Articles With Match', 'Matching Paragraphs', 'Error Count', 'Empty Text Count'] + KEYWORDS)
        
        # Write data rows
        for city, s in stats.items():
            row = [
                city,
                s['articles_processed'],
                s['articles_with_match'],
                s['paragraphs_matched'],
                s.get('error_count', 0),
                s.get('empty_text_count', 0)
            ]
            # Add keyword counts
            for kw in KEYWORDS:
                row.append(s['keyword_counts'].get(kw, 0))
            writer.writerow(row)
    
    print(f"\nSummary statistics written to: {summary_path}")

if __name__ == '__main__':
    main() 