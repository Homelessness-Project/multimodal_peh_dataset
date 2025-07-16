import requests
import time
import pandas as pd
from datetime import datetime, timezone
from tqdm import tqdm
from utils import KEYWORDS, CITY_MAP
import os
from dotenv import load_dotenv
import sys
import argparse
import csv

load_dotenv()
BEARER_TOKEN = os.getenv('BEARER_TOKEN')
if not BEARER_TOKEN:
    print('Error: BEARER_TOKEN not found in .env file.')
    sys.exit(1)

SEARCH_URL = 'https://api.twitter.com/2/tweets/search/all'

HEADERS = {
    'Authorization': f'Bearer {BEARER_TOKEN}',
}

START_DATE = "2015-01-01T00:00:00Z"
END_DATE = "2025-01-01T00:00:00Z"


# Geolocation mapping: city name (lowercase) to (longitude, latitude)
CITY_GEO = {
    'south bend':      (-86.25199, 41.6764),
    'rockford':        (-89.0940, 42.2711),
    'kalamazoo':       (-85.5872, 42.2917),
    'scranton':        (-75.6624, 41.4089),
    'fayetteville':    (-94.1574, 36.0626),
    'san francisco':   (-122.4194, 37.7749),
    'portland':        (-122.6765, 45.5231),
    'buffalo':         (-78.8784, 42.8864),
    'baltimore':       (-76.6122, 39.2904),
    'el paso':         (-106.4850, 31.7619),
}

# === Query Constructors ===
def geo_query(city):
    coords = CITY_GEO.get(city)
    if coords:
        lon, lat = coords
        # Match tweets with geo OR city name in text
        return f'({" OR ".join(KEYWORDS)}) (point_radius:[{lon} {lat} 20km] OR "{city}")'
    else:
        return f'({" OR ".join(KEYWORDS)}) ("{city}")'

def keyword_query():
    return f'({" OR ".join(KEYWORDS)})'

# === Rate Limit Check ===
def get_rate_limits():
    response = requests.get(SEARCH_URL, headers=HEADERS)
    return {
        'limit': response.headers.get("x-rate-limit-limit"),
        'remaining': response.headers.get("x-rate-limit-remaining"),
        'reset': (
            datetime.fromtimestamp(int(response.headers.get("x-rate-limit-reset", 0)), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            if response.headers.get("x-rate-limit-reset") else "unknown"
        )
    }

# === Tweet Count ===
def get_tweet_count(query, start_time, end_time):
    url = "https://api.twitter.com/2/tweets/counts/all"
    params = {
        'query': query,
        'start_time': start_time,
        'end_time': end_time,
        'granularity': 'day'
    }
    print(f"Calling tweet count API with params: {params}")
    response = requests.get(url, headers=HEADERS, params=params)
    print(f"API response status: {response.status_code}")
    if response.status_code != 200:
        print(f"API error: {response.status_code} {response.text}")
        return 0
    data = response.json()
    return sum([int(item['tweet_count']) for item in data.get('data', [])])

# === Tweet Fetching ===
def fetch_tweets(query, start_time, end_time, max_tweets=1000):
    tweets = []
    next_token = None
    count = 0

    with tqdm(total=max_tweets, desc=f"Fetching {query[:30]}...") as pbar:
        while count < max_tweets:
            params = {
                'query': query,
                'start_time': start_time,
                'end_time': end_time,
                'max_results': 100,
                'tweet.fields': 'id,text,created_at,author_id,geo',
                'expansions': 'author_id,geo.place_id',
                'user.fields': 'id,username,location',
                'place.fields': 'full_name,id,country,country_code,geo,name,place_type'
            }
            if next_token:
                params['next_token'] = next_token

            response = requests.get(SEARCH_URL, headers=HEADERS, params=params)
            if response.status_code == 429:
                print(f"429 Rate limit error. Response headers: {response.headers}")
                print(f"429 Response body: {response.text}")
                reset_time = response.headers.get('x-rate-limit-reset')
                if reset_time:
                    reset_timestamp = int(reset_time)
                    now = int(time.time())
                    wait_seconds = max(reset_timestamp - now, 0) + 5  # Add buffer
                    reset_dt = datetime.fromtimestamp(reset_timestamp, tz=timezone.utc)
                    print(f"Rate limit hit. Waiting until {reset_dt} UTC (about {wait_seconds} seconds)...")
                    time.sleep(wait_seconds)
                    continue  # Retry the same request
                else:
                    print("Rate limit hit, but no reset time provided. Waiting 60 seconds...")
                    time.sleep(60)
                    continue
            if response.status_code != 200:
                print(f"Error: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response body: {response.text}")
                break
          
            result = response.json()
            data = result.get('data', [])
            includes = result.get('includes', {})

            users = {u['id']: u for u in includes.get('users', [])}
            places = {p['id']: p for p in includes.get('places', [])}

            for tweet in data:
                author = users.get(tweet['author_id'], {})
                place = places.get(tweet.get('geo', {}).get('place_id', ''), {})

                tweets.append({
                    'id': tweet['id'],
                    'text': tweet['text'],
                    'created_at': tweet['created_at'],
                    'author_id': tweet['author_id'],
                    'user_location': author.get('location'),
                    'tweet_geo': place.get('full_name'),
                    'tweet_country': place.get('country'),
                    'place_type': place.get('place_type'),
                })

            count += len(data)
            pbar.update(len(data))
            next_token = result.get('meta', {}).get('next_token')
            if not next_token:
                break

            time.sleep(1)

    return tweets

# === Prompting Function ===
def prompt_fetch_amount(total):
    print(f"🔎 Total matching tweets: {total}")
    max_fetch = input(f"How many tweets would you like to fetch? (Enter a number or 'all'): ").strip()
    if max_fetch.lower() == 'all':
        return total
    try:
        return min(int(max_fetch), total)
    except ValueError:
        print("⚠️ Invalid input. Defaulting to 1000.")
        return min(1000, total)

def check_bearer_token():
    # Use a lightweight endpoint to check token validity
    url = 'https://api.twitter.com/2/tweets/counts/all'
    params = {'query': 'test', 'start_time': START_DATE, 'end_time': END_DATE, 'granularity': 'day'}
    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code == 401 or resp.status_code == 403:
        print('Error: Twitter API Bearer Token is invalid or unauthorized.')
        print(f"Twitter API response: {resp.text}")
        sys.exit(1)
    elif resp.status_code != 200:
        print(f'Error: Unexpected response from Twitter API (status {resp.status_code}): {resp.text}')
        sys.exit(1)
    # Otherwise, token is valid

def main():
    print("Script started")
    check_bearer_token()
    parser = argparse.ArgumentParser(description="Twitter scraper for city homelessness keywords.")
    parser.add_argument('--city', type=str, help='City name (e.g., "san francisco"). If not provided, all cities will be processed.')
    parser.add_argument('--count-only', action='store_true', help='Only get tweet count, do not scrape posts')
    parser.add_argument('--max-tweets', nargs='?', const=1000, type=int, default=100000, help='Maximum number of tweets to fetch per city (default: 100,000; if used with no value, 1,000)')
    args = parser.parse_args()
    print(f"Parsed args: {args}")

    if args.city:
        cities_to_process = [args.city.strip().lower()]
    else:
        print("No city specified. Will process all cities:")
        cities_to_process = list(CITY_GEO.keys())
        for city in cities_to_process:
            print(f"- {city.title()}")

    summary = []
    geo_summary = []  # For count-only summary
    for city_input in cities_to_process:
        print(f"Processing city: {city_input}")
        if city_input not in CITY_MAP:
            print(f"City '{city_input}' not recognized. Skipping.")
            continue
        city_dir = CITY_MAP[city_input]

        print(f"\n=== {city_input.title()} ===")
        print("Checking rate limits...")
        limits = get_rate_limits()
        print(f"Rate Limit: {limits['limit']}, Remaining: {limits['remaining']}, Resets: {limits['reset']} UTC")

        # Use geo query for all cities if possible
        query = geo_query(city_input)
        print(f"Query for city {city_input}: {query}")

        print(f"[{city_input.title()} QUERY] Getting tweet count...")
        total = get_tweet_count(query, START_DATE, END_DATE)
        print(f"Total matching tweets for {city_input.title()} (2015-2025): {total}")

        # New: For count-only, show geo vs non-geo counts
        if args.count_only:
            coords = CITY_GEO.get(city_input)
            if coords:
                lon, lat = coords
                geo_query_str = f'({" OR ".join(KEYWORDS)}) point_radius:[{lon} {lat} 20km]'
                geo_count = get_tweet_count(geo_query_str, START_DATE, END_DATE)
                text_query_str = f'({" OR ".join(KEYWORDS)}) "{city_input}" -point_radius:[{lon} {lat} 20km]'
                text_count = get_tweet_count(text_query_str, START_DATE, END_DATE)
                print(f"Geo-tagged tweets: {geo_count}")
                print(f"Non-geo (city name in text only): {text_count}")
                geo_summary.append({
                    'city': city_input.title(),
                    'total': total,
                    'geo_tagged': geo_count,
                    'non_geo': text_count
                })
            else:
                print(f"No geolocation for {city_input}, only total count available.")
                geo_summary.append({
                    'city': city_input.title(),
                    'total': total,
                    'geo_tagged': '',
                    'non_geo': ''
                })
            continue
        summary.append({'city': city_input.title(), 'total_tweets': total})

        if args.count_only:
            print("No tweets will be scraped. This only uses the count endpoint and does not consume tweet retrieval quota.")
            continue

        # Prepare output path
        output_dir = os.path.join('data', city_dir, 'x')
        os.makedirs(output_dir, exist_ok=True)
        date_range = '2015-2025'
        lang = 'english'
        posts_filename = f'posts_{lang}_{date_range}.csv'
        stats_filename = f'statistics_twitter_{lang}_{date_range}.csv'
        by_day_filename = f'statistics_twitter_by_day_{lang}_{date_range}.csv'
        output_path = os.path.join(output_dir, posts_filename)
        if os.path.exists(output_path):
            print(f"Data for {city_input.title()} already exists at '{output_path}'. Skipping scraping.")
            continue

        max_to_fetch = min(args.max_tweets, total)
        print("Initiial 5 second wait")
        time.sleep(5) 
        print(f"Fetching up to {max_to_fetch} tweets for {city_input}")
        tweets = fetch_tweets(query, START_DATE, END_DATE, max_tweets=max_to_fetch)

        df = pd.DataFrame(tweets)
        df.to_csv(output_path, index=False)
        print(f"\n✅ Fetched {len(df)} unique tweets. Saved to '{output_path}'")

        # Output statistics
        stats = {
            'city': city_input.title(),
            'total_tweets': len(df),
            'start_date': START_DATE,
            'end_date': END_DATE,
        }
        # Optionally, add more stats (e.g., by day)
        if not df.empty and 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])
            by_day = df['created_at'].dt.date.value_counts().sort_index()
            # Save daily counts as a separate CSV
            by_day_df = by_day.rename_axis('date').reset_index(name='tweet_count')
            by_day_df.to_csv(os.path.join(output_dir, by_day_filename), index=False)
        # Save summary stats
        stats_path = os.path.join(output_dir, stats_filename)
        pd.DataFrame([stats]).to_csv(stats_path, index=False)
        print(f"Statistics saved to '{stats_path}'")

    # Print summary
    if args.count_only:
        print("\nSummary:")
        print(f"{'City':<20}{'Total':>10}{'Geo-tagged':>15}{'Non-geo':>15}")
        for row in geo_summary:
            print(f"{row['city']:<20}{row['total']:>10}{str(row['geo_tagged']):>15}{str(row['non_geo']):>15}")
        # Save to CSV
        os.makedirs('data/data_summary', exist_ok=True)
        summary_path = os.path.join('data', 'data_summary', 'twitter_count_summary.csv')
        with open(summary_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['city', 'total', 'geo_tagged', 'non_geo'])
            writer.writeheader()
            for row in geo_summary:
                writer.writerow(row)
        print(f"\nSummary written to {summary_path}")

if __name__ == '__main__':
    """
    Command line arguments:
      --city CITY_NAME         (optional) Name of the city (e.g., "san francisco", "portland", etc.)
      --count-only            (optional) Only get tweet count, do not scrape posts
      --max-tweets MAX_TWEETS  (optional) Maximum number of tweets to fetch per city (default: 100,000; if used with no value, 1,000)

    Example usage:
      python3 scripts/get_twitter_data.py --city "san francisco"
      python3 scripts/get_twitter_data.py --city "south bend" --count-only
    """
    main()

