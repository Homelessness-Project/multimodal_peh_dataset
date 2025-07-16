import pandas as pd
import re
from tqdm import tqdm
import spacy
import os
from utils import load_spacy_model, deidentify_text

def main():
    # Load spaCy model
    print("Loading spaCy model...")
    nlp = load_spacy_model()
    
    # Get list of cities from the data directory
    data_dir = "data"
    cities = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    
    for city in cities:
        print(f"\nProcessing {city}...")
        
        # Process both all_comments and filtered_comments
        for comment_type in ['all_comments', 'filtered_comments']:
            input_file = f"data/{city}/reddit/{comment_type}.csv"
            output_file = input_file.replace('.csv', '_deidentified.csv')
            
            # Skip if input file doesn't exist
            if not os.path.exists(input_file):
                print(f"Skipping {input_file} - file not found")
                continue
                
            # Skip if deidentified file already exists
            if os.path.exists(output_file):
                print(f"Skipping {input_file} - deidentified file already exists at {output_file}")
                continue
            
            print(f"\nProcessing {input_file}...")
            df = pd.read_csv(input_file)
            
            # Deidentify both Submission Title and Comment columns
            print("Deidentifying submission titles and comments...")
            df['Deidentified_Submission_Title'] = [deidentify_text(title, nlp) for title in tqdm(df['Submission Title'])]
            df['Deidentified_Comment'] = [deidentify_text(comment, nlp) for comment in tqdm(df['Comment'])]
            
            # Create new dataframe with specified columns
            output_columns = {
                'Dission Title': df['Deidentified_Sueidentified Submbmission_Title'],
                'Submission Score': df['Submission Score'],
                'Deidentified Comment': df['Deidentified_Comment'],
                'Comment Score': df['Comment Score']
            }
            if 'Timestamp' in df.columns:
                output_columns['Timestamp'] = df['Timestamp']
            output_df = pd.DataFrame(output_columns)
            
            # Save deidentified data
            output_df.to_csv(output_file, index=False)
            print(f"Saved deidentified data to {output_file}")

if __name__ == "__main__":
    main() 