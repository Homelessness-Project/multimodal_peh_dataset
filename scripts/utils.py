import spacy
from pydeidentify import Deidentifier
import requests
from requests.auth import HTTPBasicAuth
import json
import urllib.parse
from datetime import datetime
from tqdm import tqdm
import time
import re


KEYWORDS = [
    'homeless', 'homelessness', 'housing crisis',
    'affordable housing', 'unhoused', 'houseless',
    'housing insecurity', 'beggar', 'squatter', 'panhandler', 'soup kitchen'
]

CITY_MAP = {
    'south bend': 'southbend',
    'rockford': 'rockford',
    'kalamazoo': 'kzoo',
    'scranton': 'scranton',
    'fayetteville': 'fayetteville',
    'san francisco': 'sanfrancisco',
    'portland': 'portland',
    'buffalo': 'buffalo',
    'baltimore': 'baltimore',
    'el paso': 'elpaso',
}

def load_spacy_model():
    try:
        # Load English language model
        nlp = spacy.load("en_core_web_sm")
        return nlp
    except OSError:
        print("Downloading spaCy model...")
        import subprocess
        subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
        return spacy.load("en_core_web_sm")

def deidentify_text(text, nlp=None):
    if not isinstance(text, str):
        return ""
    # First, use pydeidentify
    deidentifier = Deidentifier()
    text = str(deidentifier.deidentify(text))  # Convert DeidentifiedText to string
    
    # Then, apply custom regex/spaCy logic for further deidentification
    if nlp is None:
        import spacy
        nlp = spacy.load("en_core_web_sm")
    
    # Process text with spaCy
    doc = nlp(text)
    deidentified = text
    
    # Custom patterns for domain-specific terms
    location_patterns = [
        (r'\b(?:St\.|Saint)\s+[A-Za-z]+\s+(?:County|Parish|City|Town)\b', '[LOCATION]'),
        (r'\b(?:Low|High)\s+Barrier\s+(?:Homeless|Housing)\s+Shelter\b', '[INSTITUTION]'),
        (r'\b(?:Homeless|Housing)\s+Shelter\b', '[INSTITUTION]'),
        (r'\b(?:Community|Resource)\s+Center\b', '[INSTITUTION]'),
        (r'\b(?:Public|Private)\s+(?:School|University|College)\b', '[INSTITUTION]'),
        (r'\b(?:Medical|Health)\s+Center\b', '[INSTITUTION]'),
        (r'\b(?:Police|Fire)\s+Department\b', '[INSTITUTION]'),
        (r'\b(?:City|County|State)\s+Hall\b', '[INSTITUTION]'),
        (r'\b(?:Public|Private)\s+(?:Library|Park|Garden)\b', '[INSTITUTION]'),
        (r'\b(?:Shopping|Retail)\s+Mall\b', '[INSTITUTION]'),
        (r'\b(?:Bus|Train|Subway)\s+Station\b', '[INSTITUTION]'),
        (r'\b(?:Airport|Harbor|Port)\b', '[INSTITUTION]'),
        (r'\b(?:Street|Avenue|Road|Boulevard|Drive|Lane|Place|Court|Circle|Way)\b', '[STREET]'),
        (r'\b(?:North|South|East|West|N|S|E|W)\s+(?:Street|Avenue|Road|Boulevard|Drive|Lane|Place|Court|Circle|Way)\b', '[STREET]'),
        (r'\b(?:First|Second|Third|Fourth|Fifth|Sixth|Seventh|Eighth|Ninth|Tenth)\s+(?:Street|Avenue|Road|Boulevard|Drive|Lane|Place|Court|Circle|Way)\b', '[STREET]'),
        (r'\b(?:Main|Broad|Market|Park|Church|School|College|University|Hospital|Library)\s+(?:Street|Avenue|Road|Boulevard|Drive|Lane|Place|Court|Circle|Way)\b', '[STREET]'),
    ]
    
    # Apply location patterns first
    for pattern, replacement in location_patterns:
        deidentified = re.sub(pattern, replacement, deidentified, flags=re.IGNORECASE)
    
    # Replace named entities
    for ent in doc.ents:
        if ent.label_ in ['PERSON', 'GPE', 'LOC', 'ORG', 'DATE', 'TIME']:
            if ent.label_ == 'PERSON':
                replacement = '[PERSON]'
            elif ent.label_ in ['GPE', 'LOC']:
                replacement = '[LOCATION]'
            elif ent.label_ == 'ORG':
                replacement = '[ORGANIZATION]'
            elif ent.label_ == 'DATE':
                replacement = '[DATE]'
            elif ent.label_ == 'TIME':
                replacement = '[TIME]'
            deidentified = deidentified.replace(ent.text, replacement)
    
    # Additional patterns for emails, phones, etc.
    patterns = {
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}': '[PHONE]',
        r'\+\d{1,2}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}': '[PHONE]',
        r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}': '[PHONE]',
        r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}': '[PHONE]',
        r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s]*)?': '[URL]',
        r'www\.(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s]*)?': '[URL]',
        r'(?:[-\w.]|(?:%[\da-fA-F]{2}))+\.(?:com|org|net|edu|gov|mil|biz|info|mobi|name|aero|asia|jobs|museum)(?:/[^\s]*)?': '[URL]',
        r'\[URL\](?:/[^\s]*)?': '[URL]',
        r'\[URL\]/search\?[^\s]*': '[URL]',
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b': '[EMAIL]',
        r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b': '[IP]',
        r'\b\d{5}(?:-\d{4})?\b': '[ZIP]',
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b': '[DATE]',
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b': '[DATE]',
    }
    
    # Apply additional patterns
    for pattern, replacement in patterns.items():
        deidentified = re.sub(pattern, replacement, deidentified)
    
    # Clean up any remaining URL-like or location patterns
    deidentified = re.sub(r'\[URL\]/[^\s]+', '[URL]', deidentified)
    deidentified = re.sub(r'\[URL\]\[URL\]', '[URL]', deidentified)
    deidentified = re.sub(r'\[LOCATION\]/[^\s]+', '[LOCATION]', deidentified)
    deidentified = re.sub(r'\[LOCATION\]\[LOCATION\]', '[LOCATION]', deidentified)
    deidentified = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', deidentified)
    
    return deidentified

class LexisNexisAPI:
    """
    Class to interact with the LexisNexis API.
    Code adapted from documentation based on work provided by Jeff Clark <jeff.clark.1@lexisnexis.com> Oct 16th, 2019 
    as well as code from Eric Lease Morgan <emorgan@nd.edu> Nov 7th, 2019
    """
    def __init__(self, client_id, secret):
        """Initialize the LexisNexis API client with credentials."""
        self.client_id = client_id
        self.secret = secret
        self.token = None
        self.headers = None
        self._refresh_token()

    def _refresh_token(self):
        """Get a new authorization token."""
        auth_url = 'https://auth-api.lexisnexis.com/oauth/v2/token'
        payload = 'grant_type=client_credentials&scope=http%3a%2f%2foauth.lexisnexis.com%2fall'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        r = requests.post(auth_url, auth=HTTPBasicAuth(self.client_id, self.secret), 
                         headers=headers, data=payload)
        self.token = r.json()['access_token']
        self.headers = {
            'Accept': 'application/json;odata.metadata=minimal',
            'Connection': 'Keep-Alive',
            'Host': 'services-api.lexisnexis.com',
            'Authorization': f'Bearer {self.token}'
        }

    def _build_url(self, content='News', query='', skip=0, expand='Document', top=50, filter=None):
        """Build the URL for the API request."""
        base_url = f'https://services-api.lexisnexis.com/v1/{content}'
        params = {
            '$expand': expand,
            '$search': query,
            '$skip': str(skip),
            '$top': str(top)
        }
        if filter:
            params['$filter'] = filter
        return f"{base_url}?{urllib.parse.urlencode(params)}"

    def get_total_count(self, query, filter=None, max_retries=25):
        """Get the total count of articles matching the query."""
        url = self._build_url(query=query, top=1, filter=filter)
        retry_count = 0
        while retry_count < max_retries:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json().get('@odata.count', 0)
            print(f"[ERROR] Status code: {response.status_code}")
            print(f"[ERROR] Response body: {response.text}")
            print(f"[ERROR] Response headers: {response.headers}")
            if response.status_code == 429:
                if retry_count == 0:
                    print("[ERROR] You have reached the API rate limit! Waiting 60 seconds before retrying...")
                    time.sleep(60)
                else:
                    print("[ERROR] You have reached the API rate limit again! Waiting 1 hour before retrying...")
                    time.sleep(3600)
                retry_count += 1
                continue
            break
        return 0

    def search_articles(self, query, filter=None, batch_size=50, max_retries=3):
        """Search for articles with pagination and retry logic."""
        total_count = self.get_total_count(query, filter)
        print(f"\nTotal articles available: {total_count}")

        all_articles = []
        seen_articles = set()
        offset = 0

        # Create progress bar
        pbar = tqdm(total=total_count, desc="Fetching articles", unit="articles")

        while offset < total_count:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    url = self._build_url(query=query, skip=offset, top=batch_size, filter=filter)
                    response = requests.get(url, headers=self.headers)
                    if response.status_code != 200:
                        print(f"[ERROR] Status code: {response.status_code}")
                        print(f"[ERROR] Response body: {response.text}")
                        print(f"[ERROR] Response headers: {response.headers}")
                        if response.status_code == 429:
                            print("[ERROR] You have reached the API rate limit! Waiting 60 seconds before retrying...")
                            time.sleep(60)
                            continue  # Retry after waiting
                        break
                    data = response.json()
                    batch_articles = data.get('value', [])

                    if not batch_articles:
                        break

                    # Add only unique articles
                    for article in batch_articles:
                        title = article.get('Title', '')
                        date = article.get('Date', '')
                        unique_id = f"{title}|{date}"
                        if unique_id and unique_id not in seen_articles:
                            seen_articles.add(unique_id)
                            all_articles.append(article)

                    # Update progress bar
                    pbar.update(len(batch_articles))
                    pbar.set_postfix({"Unique": len(all_articles)})

                    # Move to next batch
                    offset += batch_size
                    break  # Success, exit retry loop

                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    print(f"\nError fetching batch at offset {offset} (attempt {retry_count}/{max_retries}):")
                    print(f"Error: {str(e)}")
                    if retry_count == max_retries:
                        print(f"Failed to fetch batch after {max_retries} attempts, skipping to next batch")
                        offset += batch_size
                    else:
                        print("Retrying in 5 seconds...")
                        time.sleep(5)
                        self._refresh_token()  # Refresh token on retry

                except Exception as e:
                    print(f"\nAn unexpected error occurred: {str(e)}")
                    offset += batch_size
                    break

        pbar.close()
        return all_articles

    def filter_articles_by_date(self, articles, start_date=None, end_date=None):
        """Filter articles by date range."""
        if not start_date:
            start_date = datetime(2015, 1, 1)
        if not end_date:
            end_date = datetime(2025, 1, 1)

        filtered_articles = []
        for article in articles:
            date_str = article.get('Date', '')
            try:
                article_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                if start_date <= article_date <= end_date:
                    filtered_articles.append(article)
            except (ValueError, TypeError) as e:
                print(f"Error processing date {date_str}: {str(e)}")
                continue

        return filtered_articles

    def save_to_csv(self, articles, filename='search_results.csv'):
        """Save articles to a CSV file."""
        import csv
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Title', 'Date', 'Source', 'City Source', 'Summary', 'Full Text']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for article in articles:
                writer.writerow({
                    'Title': article.get('Title', ''),
                    'Date': article.get('Date', ''),
                    'Source': article.get('Source', {}).get('Name', ''),
                    'City Source': article.get('City Source', ''),
                    'Summary': article.get('Overview', ''),
                    'Full Text': article.get('Document', {}).get('Content', '')
                })
        print(f"Results have been written to {filename}") 