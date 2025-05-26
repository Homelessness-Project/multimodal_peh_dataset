import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')

#Newas API Credentials
# You can get a free key from https://newsapi.org/
# Note: Free API key gets data only from the past month
NEWS_API_KEY = os.getenv('NEWS_API_KEY')
