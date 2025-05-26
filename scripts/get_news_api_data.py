from newsapi import NewsApiClient
import csv
from utils import KEYWORDS
from config import NEWS_API_KEY

newsapi = NewsApiClient(api_key=NEWS_API_KEY)

# List of San Francisco news domains
sf_domains = [
    'sfchronicle.com',
    'sfgate.com',
    'sfexaminer.com',
    'sfpublicpress.org',
    'sfstandard.com',
    '48hills.org',
    'sfist.com'
]


# Construct the query string
query = ' OR '.join(KEYWORDS)

# Fetch articles
articles = newsapi.get_everything(
    q=query,
    domains=','.join(sf_domains),
    language='en',
    sort_by='relevancy',
    page_size=100
)

# Specify the CSV file name
csv_file = 'data/sanfrancisco/newspapers/news_api_last_month_sf_homelessness_articles.csv'

# Define the CSV headers
csv_headers = ['source', 'author', 'title', 'description', 'url', 'publishedAt', 'content']

# Write articles to the CSV file
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=csv_headers)
    writer.writeheader()
    for article in articles['articles']:
        writer.writerow({
            'source': article['source']['name'],
            'author': article.get('author', 'N/A'),
            'title': article['title'],
            'description': article.get('description', 'N/A'),
            'url': article['url'],
            'publishedAt': article['publishedAt'],
            'content': article.get('content', 'N/A')
        })

print(f"Successfully saved {len(articles['articles'])} articles to '{csv_file}'.")