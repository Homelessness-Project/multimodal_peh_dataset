# PEH Data Collection and Analysis Project

This repository contains a comprehensive data collection and analysis pipeline for studying homelessness and housing issues across multiple cities. The project collects data from Reddit, X (Twitter), news articles, and meeting minutes, then processes and analyzes this data for research purposes.

## Project Overview

The project focuses on collecting and analyzing data related to homelessness and housing issues from multiple sources across various cities. Data is collected from:

- **Reddit**: Comments and submissions from city-specific subreddits
- **X (Twitter)**: Posts containing relevant keywords
- **News Articles**: Articles from LexisNexis and News API
- **Meeting Minutes**: City council meeting transcripts

## Data Structure

```
data/
├── baltimore/
│   ├── reddit/
│   │   ├── all_comments.csv
│   │   ├── filtered_comments.csv
│   │   ├── filtered_comments_deidentified.csv
│   │   └── statistics.csv
│   ├── x/
│   │   ├── posts_english_2015-2025.csv
│   │   ├── posts_english_2015-2025_rt.csv
│   │   ├── posts_english_2015-2025_rt_deidentified.csv
│   │   └── statistics.csv
│   ├── newspaper/
│   │   ├── lexisnexis.csv
│   │   ├── baltimore_filtered.csv
│   │   ├── baltimore_filtered_deidentified.csv
│   │   └── statistics.csv
│   └── meeting_minutes/
│       ├── BuffaloTranscripts1of3/
│       ├── BuffaloTranscripts2of3/
│       ├── BuffaloTranscripts3of3/
│       ├── meeting_minutes_lexicon_matches.csv
│       ├── meeting_minutes_lexicon_matches_deidentified.csv
│       └── statistics.csv
├── buffalo/
├── elpaso/
├── fayetteville/
├── kzoo/
├── portland/
├── rockford/
├── sanfrancisco/
├── scranton/
├── southbend/
└── data_summary/
    └── data_summary_by_city.csv
```

## Cities Covered

- Baltimore, MD
- Buffalo, NY
- El Paso, TX
- Fayetteville, NC
- Kalamazoo, MI
- Portland, OR
- Rockford, IL
- San Francisco, CA
- Scranton, PA
- South Bend, IN

## Setup

### 1. Environment Setup

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Download spaCy Model

```bash
python -m spacy download en_core_web_sm
```

### 4. Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
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

## Scripts Overview

### Data Collection Scripts

#### Reddit Data Collection
```bash
# Edit the script to change the subreddit name
# In scripts/get_reddit_data.py, modify line 9:
# subreddit_name = "southbend"

python scripts/get_reddit_data.py
```
- Collects comments and submissions from city-specific subreddits
- Filters for relevant keywords related to homelessness and housing
- Outputs: `all_comments.csv`, `filtered_comments.csv`
- **Note**: Subreddit name must be modified in the script before running

#### X (Twitter) Data Collection
```bash
# Collect data for specific city
python scripts/get_twitter_data.py --city "san francisco"

# Collect data for all cities
python scripts/get_twitter_data.py

# Count tweets only (no data collection)
python scripts/get_twitter_data.py --count-only
```
- Collects posts containing relevant keywords
- Filters for English content and retweets
- Outputs: `posts_english_2015-2025.csv`, `posts_english_2015-2025_rt.csv`

#### News Data Collection
```bash
# LexisNexis - specific city
python scripts/get_lexisnexis_data.py southbend

# LexisNexis - all cities
python scripts/get_lexisnexis_data.py

# News API (San Francisco only)
python scripts/get_news_api_data.py
```
- Collects news articles from LexisNexis and News API
- Filters for relevant keywords
- Outputs: `lexisnexis.csv`, filtered news files

#### Meeting Minutes Processing
```bash
# Process meeting minutes for specific city
python scripts/get_meeting_minute_paragraphs.py --city southbend

# Process San Francisco meeting minutes
python scripts/get_meeting_minutes_san_francisco.py
```
- Processes meeting minutes transcripts
- Extracts paragraphs containing relevant keywords
- Outputs: `meeting_minutes_lexicon_matches.csv`

### Data Processing Scripts

#### Deidentification
```bash
# Deidentify all data types
python scripts/deidentify_text.py

# Deidentify specific data type
python scripts/deidentify_text.py --type reddit

# Deidentify specific city
python scripts/deidentify_text.py --cities southbend,portland
```
- Removes personally identifiable information from text data
- Uses spaCy for named entity recognition
- Outputs: `*_deidentified.csv` files

#### Statistics Generation
```bash
# Generate statistics for all cities
python scripts/generate_statistics.py

# Generate statistics for specific city
python scripts/generate_statistics.py --city southbend
```
- Creates `statistics.csv` files in each subfolder
- Counts keyword matches from the lexicon
- Provides file size and row count information

### Analysis Scripts

#### Data Summary
```bash
python scripts/data_summary_by_city.py
```
- Generates summary statistics across all cities
- Outputs: `data_summary_by_city.csv`

#### News Filtering
```bash
# Filter news articles by paragraph
python scripts/filter_lexisnexis_by_paragraph.py

# Filter specific cities
python scripts/filter_lexisnexis_by_paragraph.py --cities southbend portland
```
- Filters news articles by paragraph and keywords
- Outputs: `{city}_filtered.csv` files

#### Keyword Analysis
```bash
python scripts/add_keywords_to_reddit.py
python scripts/add_keywords_to_x_deidentified.py
```
- Adds keyword analysis to deidentified datasets
- Identifies which keywords appear in each entry

## Data Sources and Keywords

### Lexicon Keywords
The project uses a predefined lexicon of keywords related to homelessness and housing:

```python
KEYWORDS = [
    'homeless', 'homelessness', 'housing crisis',
    'affordable housing', 'unhoused', 'houseless',
    'housing insecurity', 'beggar', 'squatter', 
    'panhandler', 'soup kitchen'
]
```

### Data Sources
1. **Reddit**: City-specific subreddits and regional subreddits
2. **X (Twitter)**: Public posts containing relevant keywords
3. **News Articles**: LexisNexis and News API articles
4. **Meeting Minutes**: City council meeting transcripts

## File Naming Conventions

- `all_*.csv`: Raw collected data
- `filtered_*.csv`: Data filtered for relevant keywords
- `*_deidentified.csv`: Data with PII removed
- `*_lexicon_matches.csv`: Meeting minutes with keyword matches
- `statistics.csv`: Summary statistics for each subfolder

## Data Processing Pipeline

1. **Collection**: Raw data collected from various sources
2. **Filtering**: Data filtered for relevant keywords
3. **Deidentification**: PII removed for privacy protection
4. **Analysis**: Statistics and keyword analysis generated
5. **Summary**: Cross-city analysis and reporting

## Privacy and Ethics

- All data is deidentified to protect individual privacy
- Meeting minutes are public records
- Social media data is publicly available
- News articles are from public sources
- No personally identifiable information is retained in processed datasets

## Usage Examples

### Complete Pipeline

```bash
# 1. Collect Reddit data
python scripts/get_reddit_data.py

# 2. Collect X data
python scripts/get_twitter_data.py

# 3. Collect news data
python scripts/get_lexisnexis_data.py

# 4. Process meeting minutes (if available)
python scripts/get_meeting_minute_paragraphs.py

# 5. Deidentify all data
python scripts/deidentify_text.py

# 6. Generate statistics
python scripts/generate_statistics.py
```

### Analysis Workflow

```bash
# Generate comprehensive statistics
python scripts/generate_statistics.py

# Create data summary
python scripts/data_summary_by_city.py

# View statistics for specific city
cat data/southbend/reddit/statistics.csv
```

## Troubleshooting

### Common Issues

1. **spaCy model not found**: Run `python -m spacy download en_core_web_sm`
2. **API rate limits**: Check API credentials and wait between requests
3. **Memory issues**: Process cities individually for large datasets
4. **File not found errors**: Ensure data directories exist before processing

### Performance Tips

- Use `--n_process` parameter for parallel processing
- Process cities individually for large datasets
- Monitor disk space for large CSV files
- Use filtered datasets for analysis to reduce processing time

## Contributing

1. Follow the existing code structure
2. Add appropriate error handling
3. Update documentation for new features
4. Test with sample data before processing full datasets

## License

This project is for research purposes. Please ensure compliance with data source terms of service and privacy requirements.