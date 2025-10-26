import os
import io
import gzip
import zipfile
import json
import logging
import requests
import polars as pl
import pandas as pd
import hashlib


# ------------------- Configuration -------------------
# URL = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/All_Beauty.jsonl.gz"
# DATA_DIR = "DataSet"
# os.makedirs(DATA_DIR, exist_ok=True)
# LOG_FILE = os.path.join(DATA_DIR, "data_ingestion.log")

# ------------------- Logging Setup -------------------
# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# ------------------- Helper Functions -------------------

def download_file(url: str, dest_folder: str) -> str:
    """Download file from a URL and return the local file path."""
    local_filename = os.path.join(dest_folder, url.split("/")[-1])
    logging.info(f"Downloading dataset from {url}")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info(f"File downloaded: {local_filename}")
    return local_filename


def decompress_file(file_path: str, output_folder: str) -> list:
    """
    Decompress .gz or .zip files and save extracted files to `output_folder`.
    Returns a list of paths to the extracted files.
    """
    os.makedirs(output_folder, exist_ok=True)
    extracted_paths = []

    if file_path.endswith(".gz"):
        filename = os.path.basename(file_path).replace(".gz", "")
        output_path = os.path.join(output_folder, filename)
        with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())
        logging.info(f"Extracted GZ file: {output_path}")
        extracted_paths.append(output_path)

    elif file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                # Ensure directories are ignored and only files are extracted
                if not member.endswith('/'):
                    output_path = os.path.join(output_folder, os.path.basename(member))
                    with zip_ref.open(member) as source, open(output_path, 'wb') as target:
                        target.write(source.read())
                    extracted_paths.append(output_path)
        logging.info(f"Extracted ZIP file(s): {extracted_paths}")

    else:
        logging.info("File is not compressed. Copying to output folder.")
        output_path = os.path.join(output_folder, os.path.basename(file_path))
        with open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())
        extracted_paths.append(output_path)

    return extracted_paths


def load_json_or_jsonl(file_path: str) -> pl.DataFrame:
    """Load .json or .jsonl file into a Polars DataFrame."""
    logging.info(f"Loading file into Polars DataFrame: {file_path}")
    if file_path.endswith(".jsonl"):
        with open(file_path, 'r', encoding='utf-8') as f:
            data = [json.loads(line) for line in f]
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

    df = pl.DataFrame(data)
    logging.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    return df

def load_json_or_jsonl_pandas(file_path: str) -> pd.DataFrame:
    """Load .json or .jsonl file into a Pandas DataFrame."""
    logging.info(f"Loading file into Pandas DataFrame: {file_path}")

    if file_path.endswith(".jsonl"):
        df = pd.read_json(file_path, lines=True)
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)

    logging.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")
    return df


# ----------------------------
# Utility Functions
# ----------------------------
def load_processed_files(PROCESSED_LOG):
    """Load processed file hashes from JSON log. Returns empty list if file missing or empty."""
    if not os.path.exists(PROCESSED_LOG):
        print("does not exits")
        return []


    try:
        with open(PROCESSED_LOG, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                return []
    except (json.JSONDecodeError, ValueError):
        # File is empty or corrupted
        return []

def save_processed_files(new_files, PROCESSED_LOG):
    """Save processed file hashes back to JSON log, appending to existing entries."""
    # Load existing processed files
    processed = load_processed_files(PROCESSED_LOG)
    
    # Merge without duplicates
    updated_files = list(set(processed + new_files))
    
    # Write back to JSON
    with open(PROCESSED_LOG, "w") as f:
        json.dump(updated_files, f, indent=2)

def compute_file_hash(file_path):
    """Compute a file hash (MD5) to detect duplicates."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def get_new_files(data_folder, processed_hashes):
    """Return list of new files not yet processed."""
    all_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith(".jsonl")]
    new_files = []
    for file in all_files:
        file_hash = compute_file_hash(file)
        if file_hash not in processed_hashes:
            new_files.append((file, file_hash))
    return new_files

def get_newzipped__files(folder, processed_hashes):
    all_files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".gz")]
    new_files = []
    for file in all_files:
        file_hash = compute_file_hash(file)
        if file_hash not in processed_hashes:
            new_files.append((file, file_hash))
    return new_files

# ----------------------------
# Main Ingestion Logic
# ----------------------------
def process_files(DATA_FOLDER,Processed_FOLDER):
    processed_hashes = load_processed_files(Processed_FOLDER)
    new_files = get_new_files(DATA_FOLDER, processed_hashes)

    if not new_files:
        print("No new files found.")
        return

    for file_path, file_hash in new_files:
        try:
            print(f"Processing: {file_path}")

            df = load_json_or_jsonl(file_path=file_path)
            processed_hashes.append(file_hash)
            print(df)
            save_processed_files(processed_hashes,Processed_FOLDER)
            # print(f"✅ Successfully ingested {os.path.basename(file_path)}")
        except Exception as e:
            print(f"❌ Failed to ingest {file_path}: {e}")



# ------------------- Main Script -------------------

# if __name__ == "__main__":
#     try:
#         logging.info("=== Data Ingestion Process Started ===")
#         downloaded_file = download_file(URL, DATA_DIR)
#         extracted_file = decompress_file(downloaded_file)
#         df = load_json_or_jsonl(extracted_file)

#         # Save DataFrame to parquet for next ingestion step (optional)
#         parquet_path = os.path.join(DATA_DIR, "amazon_reviews.parquet")
#         df.write_parquet(parquet_path)
#         logging.info(f"Saved cleaned data to {parquet_path}")

#         logging.info("=== Data Ingestion Process Completed Successfully ===")

#     except Exception as e:
#         logging.error(f"Error during data ingestion: {e}", exc_info=True)
#         raise
