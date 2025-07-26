import pandas as pd
import numpy as np

# Read the sampled data
df = pd.read_csv('gold_standard/sampled_lexisnexis_news.csv')

# Calculate paragraph lengths
paragraph_lengths = df['Deidentified_paragraph_text'].str.len()

print('Paragraph Length Statistics:')
print(f'Mean length: {paragraph_lengths.mean():.0f} characters')
print(f'Median length: {paragraph_lengths.median():.0f} characters')
print(f'Min length: {paragraph_lengths.min():.0f} characters')
print(f'Max length: {paragraph_lengths.max():.0f} characters')
print(f'Standard deviation: {paragraph_lengths.std():.0f} characters')

# Show some percentiles
percentiles = [10, 25, 50, 75, 90, 95, 99]
print('\nPercentiles:')
for p in percentiles:
    print(f'{p}th percentile: {paragraph_lengths.quantile(p/100):.0f} characters')

# Word count analysis
word_counts = df['Deidentified_paragraph_text'].str.split().str.len()
print(f'\nWord count statistics:')
print(f'Mean words: {word_counts.mean():.1f} words')
print(f'Median words: {word_counts.median():.1f} words')
print(f'Min words: {word_counts.min():.0f} words')
print(f'Max words: {word_counts.max():.0f} words')

# Show a few examples of different lengths
print('\nSample paragraphs by length:')

# Find shortest and longest paragraphs
df_with_lengths = df.copy()
df_with_lengths['length'] = paragraph_lengths

short_examples = df_with_lengths.nsmallest(3, 'length')['Deidentified_paragraph_text'].tolist()
long_examples = df_with_lengths.nlargest(3, 'length')['Deidentified_paragraph_text'].tolist()

print('\nShortest paragraphs:')
for i, text in enumerate(short_examples, 1):
    preview = text[:100] + "..." if len(text) > 100 else text
    print(f'{i}. ({len(text)} chars): "{preview}"')

print('\nLongest paragraphs:')
for i, text in enumerate(long_examples, 1):
    preview = text[:100] + "..." if len(text) > 100 else text
    print(f'{i}. ({len(text)} chars): "{preview}"') 