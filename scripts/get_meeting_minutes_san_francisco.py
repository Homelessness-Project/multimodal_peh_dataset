import requests
from bs4 import BeautifulSoup, Comment
import csv
import os
from datetime import datetime, timedelta, timezone
import time
from urllib.parse import urljoin
from bs4 import XMLParsedAsHTMLWarning
import warnings
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd


# Suppress XML parsing warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

def get_caption_notes(url):
    """Fetch caption notes from a given URL."""
    try:
        # Set up headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Referer': 'https://sanfrancisco.granicus.com/'
        }
        
        # Get the page
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get all text content after the header table
        header_table = soup.find('table', {'class': 'head'})
        if header_table:
            # Get all text content after the header table
            transcript_content = []
            current = header_table.next_sibling
            
            # Skip any whitespace or comments
            while current and (isinstance(current, str) and current.strip() == '' or isinstance(current, Comment)):
                current = current.next_sibling
            
            # Collect all text content
            while current:
                if isinstance(current, str):
                    text = current.strip()
                    if text:
                        transcript_content.append(text)
                current = current.next_sibling
            
            # Join all text content
            transcript_text = '\n'.join(transcript_content)
            
            if transcript_text:
                return transcript_text
        
        return None
            
    except Exception as e:
        print(f"Error fetching caption notes: {str(e)}")
        return None

def get_meeting_minutes(start_date=None, end_date=None, city="sanfrancisco"):
    """
    Fetch meeting minutes from the San Francisco government website.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        city (str): City name for the data directory
    """
    # Set default date range if not provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        
    print(f"Using date range: {start_date} to {end_date}")
    
    # Convert dates to datetime objects for comparison
    if isinstance(start_date, str):
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    else:
        start_dt = start_date
        
    if isinstance(end_date, str):
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        end_dt = end_date
    
    # Create output directory
    output_dir = f"data/{city}/meeting_minutes"
    os.makedirs(output_dir, exist_ok=True)
    
    # RSS feed URL
    rss_url = "https://sanfrancisco.granicus.com/ViewPublisherRSS.php?view_id=223"
    
    try:
        print("Fetching RSS feed...")
        response = requests.get(rss_url)
        response.raise_for_status()
        
        print(f"Response status code: {response.status_code}\n")
        
        # Parse RSS feed
        soup = BeautifulSoup(response.text, 'xml')
        items = soup.find_all('item')
        
        print(f"Found {len(items)} meeting entries in RSS feed")
        print(f"Filtering meetings after: {start_date}")
        print(f"Filtering meetings before: {end_date}\n")
        
        # Store meeting data
        meetings = []
        
        for item in items:
            try:
                # Extract meeting date
                pub_date = item.find('pubDate')
                if not pub_date:
                    continue
                    
                meeting_date = datetime.strptime(pub_date.text, '%a, %d %b %Y %H:%M:%S %z')
                print(f"Processing meeting from: {meeting_date.strftime('%Y-%m-%d')}")
                
                # Skip if outside date range
                if meeting_date < start_dt:
                    print(f"Skipping meeting from {meeting_date.strftime('%Y-%m-%d')} - before start date")
                    continue
                if meeting_date > end_dt:
                    print(f"Skipping meeting from {meeting_date.strftime('%Y-%m-%d')} - after end date")
                    continue
                
                # Extract meeting details
                title = item.find('title').text if item.find('title') else "No Title"
                description = item.find('description').text if item.find('description') else "No Description"
                link = item.find('link').text if item.find('link') else None
                
                # Extract clip_id from link
                clip_id = None
                if link:
                    # Try to extract clip_id from the link
                    if 'clip_id=' in link:
                        clip_id = link.split('clip_id=')[1].split('&')[0]
                    elif 'view_id=' in link:
                        # If no clip_id, try to get it from the video URL
                        video_url = item.find('enclosure', {'type': 'video/mp4'})
                        if video_url and 'url' in video_url.attrs:
                            video_link = video_url['url']
                            if 'clip_id=' in video_link:
                                clip_id = video_link.split('clip_id=')[1].split('&')[0]
                
                if not clip_id:
                    print("Could not extract clip_id from video URL")
                    continue
                
                # Construct transcript URL
                transcript_url = f"https://sanfrancisco.granicus.com/TranscriptViewer.php?view_id=223&clip_id={clip_id}"
                
                print(f"Fetching caption notes for meeting on {meeting_date.strftime('%m/%d/%y')}...")
                caption_notes = get_caption_notes(transcript_url)
                
                if caption_notes:
                    meetings.append({
                        'Date': meeting_date.strftime('%Y-%m-%d'),
                        'Title': title,
                        'Description': description,
                        'URL': link,
                        'Transcript URL': transcript_url,
                        'Caption Notes Content': caption_notes
                    })
                    print(f"Successfully processed meeting from {meeting_date.strftime('%m/%d/%y')}\n")
                else:
                    print(f"No caption notes found for meeting from {meeting_date.strftime('%m/%d/%y')}\n")
                
            except Exception as e:
                print(f"Error processing meeting: {str(e)}")
                continue
        
        if meetings:
            # Save to CSV
            output_file = f"{output_dir}/meeting_minutes.csv"
            df = pd.DataFrame(meetings)
            df.to_csv(output_file, index=False)
            print(f"Successfully saved {len(meetings)} meeting records to '{output_file}'")
        else:
            print("No meetings found in the specified date range")
            
    except Exception as e:
        print(f"Error fetching meeting minutes: {str(e)}")
        return None

if __name__ == "__main__":
    # Set specific date range
    start_date = datetime(2015, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    
    print(f"Using date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Create output directory
    output_dir = "data/sanfrancisco/meeting_minutes"
    os.makedirs(output_dir, exist_ok=True)
    
    # RSS feed URL
    rss_url = "https://sanfrancisco.granicus.com/ViewPublisherRSS.php?view_id=223"
    
    try:
        print("Fetching RSS feed...")
        response = requests.get(rss_url)
        response.raise_for_status()
        
        # Parse RSS feed
        soup = BeautifulSoup(response.text, 'xml')
        items = soup.find_all('item')
        
        print(f"Found {len(items)} meeting entries in RSS feed")
        
        # Store meeting data
        meetings = []
        
        for item in items:
            try:
                # Extract meeting date
                pub_date = item.find('pubDate')
                if not pub_date:
                    continue
                    
                meeting_date = datetime.strptime(pub_date.text, '%a, %d %b %Y %H:%M:%S %z')
                print(f"\nProcessing meeting from: {meeting_date.strftime('%Y-%m-%d')}")
                
                # Skip if outside date range
                if meeting_date < start_date:
                    print(f"Skipping meeting from {meeting_date.strftime('%Y-%m-%d')} - before start date")
                    continue
                if meeting_date > end_date:
                    print(f"Skipping meeting from {meeting_date.strftime('%Y-%m-%d')} - after end date")
                    continue
                
                # Extract meeting details
                title = item.find('title').text if item.find('title') else "No Title"
                description = item.find('description').text if item.find('description') else "No Description"
                link = item.find('link').text if item.find('link') else None
                
                # Extract clip_id from link
                clip_id = None
                if link:
                    # Try to extract clip_id from the link
                    if 'clip_id=' in link:
                        clip_id = link.split('clip_id=')[1].split('&')[0]
                    elif 'view_id=' in link:
                        # If no clip_id, try to get it from the video URL
                        video_url = item.find('enclosure', {'type': 'video/mp4'})
                        if video_url and 'url' in video_url.attrs:
                            video_link = video_url['url']
                            if 'clip_id=' in video_link:
                                clip_id = video_link.split('clip_id=')[1].split('&')[0]
                
                if not clip_id:
                    print("Could not extract clip_id from video URL")
                    continue
                
                # Construct transcript URL
                transcript_url = f"https://sanfrancisco.granicus.com/TranscriptViewer.php?view_id=223&clip_id={clip_id}"
                
                print(f"Fetching transcript for meeting on {meeting_date.strftime('%m/%d/%y')}...")
                transcript_text = get_caption_notes(transcript_url)
                
                if transcript_text:
                    meetings.append({
                        'Date': meeting_date.strftime('%Y-%m-%d'),
                        'Title': title,
                        'Description': description,
                        'URL': link,
                        'Transcript URL': transcript_url,
                        'Transcript': transcript_text
                    })
                    print(f"Successfully processed meeting from {meeting_date.strftime('%m/%d/%y')}")
                else:
                    print(f"No transcript found for meeting from {meeting_date.strftime('%m/%d/%y')}")
                
            except Exception as e:
                print(f"Error processing meeting: {str(e)}")
                continue
        
        if meetings:
            # Save to CSV
            output_file = f"{output_dir}/meeting_minutes.csv"
            df = pd.DataFrame(meetings)
            df.to_csv(output_file, index=False)
            print(f"\nSuccessfully saved {len(meetings)} meeting records to '{output_file}'")
        else:
            print("No meetings found in the specified date range")
            
    except Exception as e:
        print(f"Error fetching meeting minutes: {str(e)}") 