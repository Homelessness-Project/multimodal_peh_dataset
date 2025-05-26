import spacy
from pydeidentify import Deidentifier


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
