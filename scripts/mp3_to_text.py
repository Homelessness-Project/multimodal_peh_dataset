import whisper
import os
from pathlib import Path
from multiprocessing import Pool, cpu_count
import time
from tqdm import tqdm

# === CONFIGURATION ===
"""brew install ffmpeg"""

def init_worker():
    # Initialize model for each worker process
    global model
    model = whisper.load_model("base")  # Use "small","medium","large-v2" for better accuracy

def transcribe_file(args):
    mp3_file, output_file = args
    if output_file.exists():
        return f"Skipped {mp3_file.name} - transcription already exists"
    
    tqdm.write(f"  Transcribing: {mp3_file.name}")
    start_time = time.time()
    
    # Get file size in MB for progress estimation
    file_size_mb = os.path.getsize(mp3_file) / (1024 * 1024)
    # Rough estimate: 1MB of audio takes about 30 seconds to process
    estimated_duration = file_size_mb * 30
    
    # Create progress bar for transcription
    with tqdm(total=100, desc=f"Transcribing {mp3_file.name}", unit="%", leave=False) as pbar:
        # Start a timer to update progress
        def update_progress():
            elapsed = time.time() - start_time
            progress = min(100, (elapsed / estimated_duration) * 100)
            pbar.update(progress - pbar.n)
            if pbar.n < 100:
                pbar.refresh()
        
        # Update progress every second
        while pbar.n < 100:
            update_progress()
            time.sleep(1)
        
        # Do the actual transcription
        result = model.transcribe(str(mp3_file))
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(result["text"])
    
    duration = time.time() - start_time
    return f"Completed {mp3_file.name} in {duration:.1f} seconds"

def main():
    cities = ["rockford", "portland", "southbend"]

    # Use 75% of available CPU cores to leave some resources for other tasks
    num_workers = max(1, int(cpu_count() * 0.75))

    for city in cities:
        input_dir = Path(f"data/{city}/meeting_minutes")
        output_dir = Path(f"output/{city}/meeting_minutes")
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nProcessing city: {city}")
        
        # Prepare arguments for parallel processing
        mp3_files = list(input_dir.glob("*.mp3"))
        args = [(mp3_file, output_dir / mp3_file.with_suffix('.txt').name) 
                for mp3_file in mp3_files]
        
        # Process files in parallel with initialized workers
        with Pool(num_workers, initializer=init_worker) as pool:
            # Create progress bar for overall file progress
            results = list(tqdm(
                pool.imap(transcribe_file, args),
                total=len(args),
                desc="Overall Progress",
                unit="file"
            ))
        
        # Print results
        for result in results:
            print(f"  {result}")

if __name__ == '__main__':
    main()
