from utils import LexisNexisAPI, CITY_MAP
from config import LEXISNEXIS_API_ID, LEXISNEXIS_API_KEY
import urllib.parse
import os
import argparse
from datetime import datetime

# Dictionary of city sources
CITY_SOURCES = {
    # Small Cities
    'southbend': [
        'South Bend Tribune',
        'WVPE',
        'WSBT',
        'ABC57',
        'WNDU',
        'South Bend Voice'
    ],
    'rockford': [
        'Rockford Register Star',
        'WREX',
        'WTVO',
        'WIFR',
        'WROK',
        'Rockford Scanner'
    ],
    'kalamazoo': [
        'Kalamazoo Gazette',
        'WMUK',
        'WKZO',
        'WWMT',
        'Western Herald',
        'Kalamazoo College Index'
    ],
    'scranton': [
        'The Times-Tribune',
        'WILK',
        'WBRE',
        'WNEP',
        'The Scranton Times',
        'The University of Scranton Aquinas'
    ],
    'fayetteville': [
        'Northwest Arkansas Democrat-Gazette',
        'KUAF',
        'KNWA',
        'KFSM',
        'The Arkansas Traveler',
        'Fayetteville Flyer'
    ],
        # Large Cities
    'sanfrancisco': [
        'San Francisco Chronicle',
        'San Francisco Examiner',
        'San Francisco Standard',
        'SF Public Press',
        '48 Hills',
        'SFist',
        'KQED',
        'KPIX',
        'KGO',
        'KRON',
        'The San Francisco Bay View'
    ],
    'portland': [
        'The Oregonian',
        'Portland Mercury',
        'Willamette Week',
        'OPB',
        'KGW',
        'KOIN',
        'KATU',
        'Portland Tribune',
        'The Portland Observer'
    ],
    'buffalo': [
        'The Buffalo News',
        'WBFO',
        'WGRZ',
        'WIVB',
        'WKBW',
        'WNY', #The Public to broad, WNY alternative
        'Buffalo Rising'
    ],
    'baltimore': [
        'The Baltimore Sun',
        'Baltimore City Paper',
        'WYPR',
        'WBAL',
        'WJZ',
        'WMAR',
        'The Baltimore Times',
        'The Baltimore Afro-American'
    ],
    'elpaso': [
        'El Paso Times',
        'KTEP',
        'KFOX',
        'KDBC',
        'KTSM',
        'El Paso Herald-Post',
        'El Paso Inc.'
    ]
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
    
    city_sources = CITY_SOURCES[city]
    keyword_query = " OR ".join([f'"{keyword}"' for keyword in KEYWORDS])
    
    api = LexisNexisAPI(LEXISNEXIS_API_ID, LEXISNEXIS_API_KEY)
    all_articles = []
    seen_articles = set()
    for source in city_sources:
        # Query for each source
        query = f'({keyword_query}) AND source("{source}")'
        encoded_query = urllib.parse.quote(query)
        print(f"\nQuery for LexisNexis API ({city}, {source}):")
        print(f"https://solutions.nexis.com/wsapi/news-and-directories/news/?Search={encoded_query}")
        articles = api.search_articles(query)
        for article in articles:
            title = article.get('Title', '')
            date = article.get('Date', '')
            unique_id = f"{title}|{date}"
            if unique_id not in seen_articles:
                seen_articles.add(unique_id)
                # Add city source to article
                article['City Source'] = source
                all_articles.append(article)
    print(f"\nTotal unique articles found: {len(all_articles)}")
    
    # Filter articles by date (between 1/1/2015 and 1/1/2025)
    start_date = datetime(2015, 1, 1)
    end_date = datetime(2025, 1, 1)
    filtered_articles = api.filter_articles_by_date(all_articles, start_date=start_date, end_date=end_date)
    print(f"Articles in date range: {len(filtered_articles)}")
    
    # Create output directory if it doesn't exist
    mapped_city = CITY_MAP.get(city, city)
    output_dir = f"data/{mapped_city}/newspaper"
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