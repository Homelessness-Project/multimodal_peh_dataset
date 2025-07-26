import os
import csv
csv.field_size_limit(2147483647)
from utils import CITY_MAP

def get_reddit_stats(stat_path):
    stats = {'Total Filtered Reddit Posts': 0, 'Total Filtered Reddit Comments': 0}
    if not os.path.exists(stat_path):
        return stats
    with open(stat_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0] == 'Total Keyword Posts':
                stats['Total Filtered Reddit Posts'] = int(row[1])
            elif row[0] == 'Total Filtered Comments':
                stats['Total Filtered Reddit Comments'] = int(row[1])
    return stats

def count_csv_records(filepath):
    if not os.path.exists(filepath):
        return 0
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        return sum(1 for _ in reader)

def count_geolocated_tweets(filepath):
    if not os.path.exists(filepath):
        return 0
    count = 0
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('tweet_geo') or row.get('tweet_country') or row.get('place_type'):
                if row.get('tweet_geo').strip() or row.get('tweet_country').strip() or row.get('place_type').strip():
                    count += 1
    return count

def main():
    base_dir = 'data'
    summary_dir = os.path.join(base_dir, 'data_summary')
    os.makedirs(summary_dir, exist_ok=True)
    summary_path = os.path.join(summary_dir, 'data_summary_by_city.csv')

    fieldnames = [
        'City',
        'Total Filtered Reddit Posts',
        'Total Filtered Reddit Comments',
        'Total News Articles',
        'Total News Paragraphs',
        'Total X Tweets',
        'Total X Geolocated Tweets',
        'Total X Non-Retweets',
        'Total Meeting Minutes Results',
        'Total Meetings'  # New column
    ]

    rows = []
    grand_total = {k: 0 for k in fieldnames if k != 'City'}
    for city_dir in CITY_MAP.values():
        city_row = {'City': city_dir}
        # Reddit
        reddit_dir = os.path.join(base_dir, city_dir, 'reddit')
        stat_path = os.path.join(reddit_dir, 'statistics.csv')
        reddit_stats = get_reddit_stats(stat_path)
        city_row.update(reddit_stats)
        # News
        news_dir = os.path.join(base_dir, city_dir, 'newspaper')
        lexisnexis = os.path.join(news_dir, 'lexisnexis.csv')
        filtered_news = os.path.join(news_dir, f'{city_dir}_processed_articles.csv')
        city_row['Total News Articles'] = count_csv_records(lexisnexis)
        city_row['Total News Paragraphs'] = count_csv_records(filtered_news)
        # X (Twitter)
        x_dir = os.path.join(base_dir, city_dir, 'x')
        x_posts = os.path.join(x_dir, 'posts_english_2015-2025.csv')
        x_posts_rt = os.path.join(x_dir, 'posts_english_2015-2025_rt.csv')
        city_row['Total X Tweets'] = count_csv_records(x_posts)
        city_row['Total X Geolocated Tweets'] = count_geolocated_tweets(x_posts)
        # Count non-retweets in posts_english_2015-2025_rt.csv
        def count_non_retweets(filepath):
            if not os.path.exists(filepath):
                return 0
            count = 0
            import csv
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'is_retweet' in row and str(row['is_retweet']).lower() in ['false', '0', 'no', '']:  # treat empty as not retweet
                        count += 1
            return count
        city_row['Total X Non-Retweets'] = count_non_retweets(x_posts_rt)
        # Meeting minutes results
        meeting_minutes_csv_deid = os.path.join(base_dir, city_dir, 'meeting_minutes', 'meeting_minutes_lexicon_matches_deidentified.csv')
        city_row['Total Meeting Minutes Results'] = count_csv_records(meeting_minutes_csv_deid)
        # Total Meetings
        meeting_minutes_dir = os.path.join(base_dir, city_dir, 'meeting_minutes')
        total_meetings = 0
        if city_dir == 'sanfrancisco':
            # Count rows in meeting_minutes.csv (excluding header)
            meeting_minutes_csv = os.path.join(meeting_minutes_dir, 'meeting_minutes.csv')
            total_meetings = count_csv_records(meeting_minutes_csv)
        else:
            # Count all .txt files in all subdirectories of meeting_minutes_dir
            for root, dirs, files in os.walk(meeting_minutes_dir):
                for file in files:
                    if file.endswith('.txt'):
                        total_meetings += 1
        city_row['Total Meetings'] = total_meetings
        # Add to grand total
        for k in grand_total:
            grand_total[k] += city_row.get(k, 0)
        # Add to grand total for Total Meetings
        grand_total['Total Meetings'] = grand_total.get('Total Meetings', 0) + total_meetings
        rows.append(city_row)

    # Add grand total row
    grand_total_row = {'City': 'Grand Total'}
    grand_total_row.update(grand_total)
    rows.append(grand_total_row)

    with open(summary_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Summary written to {summary_path}")

if __name__ == '__main__':
    main() 