# Data Collection and Analysis

This repository contains scripts for collecting and analyzing data from various sources.

## Setup

1. Create and activate a virtual environment:
```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the root directory with the following variables:
```
# Reddit API credentials
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=your_user_agent_here

# News API credentials
NEWS_API_KEY=your_news_api_key_here

# LexisNexis API credentials
LEXISNEXIS_API_ID=your_lexisnexis_api_id_here
LEXISNEXIS_API_KEY=your_lexisnexis_api_key_here
```

## Scripts

- `scripts/get_reddit_data.py`: Collects Reddit data for specified subreddits
- `scripts/deidentify_city_comments.py`: Deidentifies collected Reddit comments
- `scripts/get_news_api_data.py`: Collects news articles from News API
- `scripts/get_lexisnexis_data.py`: Collects news articles from LexisNexis API