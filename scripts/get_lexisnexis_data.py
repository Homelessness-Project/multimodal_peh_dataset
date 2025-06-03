from utils import LexisNexisAPI
from config import LEXISNEXIS_API_ID, LEXISNEXIS_API_KEY
import urllib.parse
import os
import argparse

# Dictionary of city sources
CITY_SOURCES = {
    'sanfrancisco': [
        'San Francisco Chronicle',
        'San Francisco Examiner',
        'San Francisco Standard',
        'SF Public Press',
        '48 Hills',
        'SFist'
    ],
    'southbend': [
        'South Bend Tribune'
    ],
}

# List of keywords
KEYWORDS = [
    'homeless', 'homelessness', 'housing crisis',
    'affordable housing', 'unhoused', 'houseless',
    'housing insecurity', 'beggar', 'squatter', 'panhandler', 'soup kitchen'
]

def get_lexisnexis_data(city):
    if city not in CITY_SOURCES:
        raise ValueError(f"City '{city}' not found in CITY_SOURCES dictionary")
    
    # Get sources for the specified city
    city_sources = CITY_SOURCES[city]
    
    # Construct the source part of the query
    source_query = " OR ".join([f'"{source}"' for source in city_sources])
    
    # Construct the keyword part of the query
    keyword_query = " OR ".join([f'"{keyword}"' for keyword in KEYWORDS])
    
    # Define the search query combining keywords and sources
    query = f'({keyword_query}) AND source({source_query})'
    
    # Print the query in a URL-friendly format
    encoded_query = urllib.parse.quote(query)
    print(f"\nQuery for LexisNexis API ({city}):")
    print(f"https://solutions.nexis.com/wsapi/news-and-directories/news/?Search={encoded_query}")
    
    # Initialize the API client
    api = LexisNexisAPI(LEXISNEXIS_API_ID, LEXISNEXIS_API_KEY)
    
    # Search for articles
    articles = api.search_articles(query)
    print(f"\nTotal unique articles found: {len(articles)}")
    
    # Filter articles by date
    filtered_articles = api.filter_articles_by_date(articles)
    print(f"Articles in date range: {len(filtered_articles)}")
    
    # Create output directory if it doesn't exist
    output_dir = f"data/{city}/newspaper"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save results to CSV
    output_path = os.path.join(output_dir, "lexisnexis.csv")
    api.save_to_csv(filtered_articles, filename=output_path)
    print(f"\nData saved to: {output_path}")

def process_all_cities():
    """Process data for all cities in CITY_SOURCES"""
    for city in CITY_SOURCES.keys():
        print(f"\nProcessing data for {city}...")
        try:
            get_lexisnexis_data(city)
        except Exception as e:
            print(f"Error processing {city}: {str(e)}")
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fetch LexisNexis data for cities')
    parser.add_argument('city', nargs='?', choices=list(CITY_SOURCES.keys()) + ['all'], 
                       default='all', help='City to fetch data for (default: all cities)')
    args = parser.parse_args()
    
    if args.city == 'all':
        process_all_cities()
    else:
        get_lexisnexis_data(args.city) 