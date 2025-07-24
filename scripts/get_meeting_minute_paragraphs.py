import os
import re
import csv
from pathlib import Path
from utils import KEYWORDS, load_spacy_model
from tqdm import tqdm
import pandas as pd

# Compile regex for each lexicon word/phrase (case-insensitive, word boundaries)
LEXICON_REGEX = [re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE) for word in KEYWORDS]

# Root data directory
DATA_ROOT = Path(__file__).resolve().parent.parent / 'data'

# Regex to extract date from filename (e.g., 10_03_2023 or 11_14_2017)
DATE_PATTERN = re.compile(r'(\d{2}_\d{2}_\d{4})')

def find_meeting_minutes_dirs(root):
    meeting_minutes_dirs = []
    for dirpath, dirnames, filenames in os.walk(root):
        if os.path.basename(dirpath) == 'meeting_minutes':
            meeting_minutes_dirs.append(Path(dirpath))
    return meeting_minutes_dirs


def split_into_sentence_groups(text, n=3, nlp=None):
    """Split text into groups of n sentences using spaCy NLP."""
    if nlp is None:
        from utils import load_spacy_model
        nlp = load_spacy_model()
    doc = nlp(text)
    sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
    groups = []
    for i in range(0, len(sentences), n):
        group = ' '.join(sentences[i:i+n])
        if group:
            groups.append(group)
    return groups


def extract_date_from_filename(filename):
    match = DATE_PATTERN.search(filename)
    if match:
        return match.group(1)
    return ''


def search_paragraphs(paragraphs):
    for paragraph in paragraphs:
        matches = [word for word, regex in zip(KEYWORDS, LEXICON_REGEX) if regex.search(paragraph)]
        if matches:
            yield paragraph, matches


def process_meeting_minutes_dir(meeting_minutes_dir, nlp):
    results = []
    files = list(meeting_minutes_dir.glob('**/*.txt'))
    for file in tqdm(files, desc=f"Files in {meeting_minutes_dir}"):
        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
        paragraphs = split_into_sentence_groups(text, n=3, nlp=nlp)
        date = extract_date_from_filename(file.name)
        for paragraph, matches in search_paragraphs(paragraphs):
            results.append({
                'filename': file.name,
                'date': date,
                'paragraph': paragraph.replace('\n', ' '),
                'matched_words': '; '.join(matches)
            })
    return results


def process_sanfrancisco_meeting_minutes_csv(meeting_minutes_dir, nlp):
    input_csv = meeting_minutes_dir / 'meeting_minutes.csv'
    output_csv = meeting_minutes_dir / 'meeting_minutes_lexicon_matches.csv'
    if not input_csv.exists():
        print(f"San Francisco meeting_minutes.csv not found in {meeting_minutes_dir}")
        return
    print(f"Processing San Francisco meeting_minutes.csv at {input_csv}")
    df = pd.read_csv(input_csv, dtype=str, keep_default_na=False)
    date_col = None
    transcript_col = None
    for col in df.columns:
        if col.strip().lower() == 'date':
            date_col = col
        if col.strip().lower() == 'transcript':
            transcript_col = col
    if date_col is None or transcript_col is None:
        print(f"Could not find 'Date' and 'Transcript' columns in {input_csv}")
        return
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df), desc="San Francisco rows"):
        date = row[date_col].strip()
        transcript = row[transcript_col].strip()
        # If >>> or >> present, split on those; else use spaCy sentence grouping
        if re.search(r'>>>|>>', transcript):
            paragraphs = [p.strip() for p in re.split(r'>>>|>>', transcript) if p.strip()]
        else:
            paragraphs = split_into_sentence_groups(transcript, n=3, nlp=nlp)
        for paragraph, matches in search_paragraphs(paragraphs):
            results.append({
                'date': date,
                'paragraph': paragraph.replace('\n', ' '),
                'matched_words': '; '.join(matches)
            })
    # Write output CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['date', 'paragraph', 'matched_words'])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"Wrote {len(results)} matches to {output_csv}")


def write_results_csv(meeting_minutes_dir, results):
    if not results:
        print(f"No matches found in {meeting_minutes_dir}")
        return
    out_csv = meeting_minutes_dir / 'meeting_minutes_lexicon_matches.csv'
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['filename', 'date', 'paragraph', 'matched_words'])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    print(f"Wrote {len(results)} matches to {out_csv}")


def main():
    nlp = load_spacy_model()
    meeting_minutes_dirs = find_meeting_minutes_dirs(DATA_ROOT)
    if not meeting_minutes_dirs:
        print("No meeting_minutes directories found.")
        return
    for meeting_minutes_dir in meeting_minutes_dirs:
        # Special handling for San Francisco
        if 'sanfrancisco' in str(meeting_minutes_dir).lower():
            process_sanfrancisco_meeting_minutes_csv(meeting_minutes_dir, nlp)
            continue
        out_csv = meeting_minutes_dir / 'meeting_minutes_lexicon_matches.csv'
        if out_csv.exists():
            print(f"Skipping {meeting_minutes_dir} (CSV already exists)")
            continue
        print(f"Processing {meeting_minutes_dir}")
        results = process_meeting_minutes_dir(meeting_minutes_dir, nlp)
        write_results_csv(meeting_minutes_dir, results)

if __name__ == '__main__':
    main() 