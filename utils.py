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
import time
import numpy as np
from clickhouse_driver import Client




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

            df = load_json_or_jsonl_pandas(file_path=file_path)

            processed_hashes.append(file_hash)
            # print(df)
            save_processed_files(processed_hashes,Processed_FOLDER)
            # print(f"✅ Successfully ingested {os.path.basename(file_path)}")
        except Exception as e:
            print(f"❌ Failed to ingest {file_path}: {e}")

    return df
    


def convert_to_json(x):
    if x is None:
        return None
    return json.dumps(x)
# Map DataFrame types to ClickHouse types
def map_dtype(col_name, sample_value):
    if isinstance(sample_value, (int, np.integer)):
        return "Int32"
    elif isinstance(sample_value, (float, np.floating)):
        return "Float32"
    elif isinstance(sample_value, (bool, np.bool_)):
        return "UInt8"
    elif isinstance(sample_value, (dict, list)):
        return "String"  # store as JSON string
    else:
        return "String"

def clean_value(x):
    """Clean and convert values before insertion."""
    # Handle NaN / None
    if isinstance(x, (int, float, str, bool, type(None), pd.Timestamp)):
        return None if pd.isnull(x) else x
    
    # Handle dicts/lists → serialize as JSON string
    elif isinstance(x, (dict, list, np.ndarray)):
        return json.dumps(x, ensure_ascii=False)
    
    # Other exotic types
    else:
        return str(x)

def gen_rows_for_insert(Data, TimeStampColumn=None):
    """
    Convert a DataFrame into a list of tuples suitable for ClickHouse insert.
    Handles timestamps, NaNs, dicts/lists.
    """
    ColumnNames = [str(x) for x in Data.columns]
    OldRows = Data.values.tolist()
    NewRows = []

    TSColIndx = Data.columns.get_loc(TimeStampColumn) if TimeStampColumn else None

    for row in OldRows:
        row = [clean_value(x) for x in row]

        # Timestamp handling
        # Timestamp handling — convert all to ISO string
        if TSColIndx is not None and row[TSColIndx] is not None:
            val = row[TSColIndx]
            if isinstance(val, pd.Timestamp):
                row[TSColIndx] = val.strftime("%Y-%m-%d %H:%M:%S.%f")
            elif isinstance(val, datetime):
                row[TSColIndx] = val.strftime("%Y-%m-%d %H:%M:%S.%f")
            else:
                row[TSColIndx] = str(val)

        # Boolean handling
        row = [int(x) if isinstance(x, bool) else x for x in row]
        NewRows.append(tuple(row))

    return ColumnNames, NewRows

    
def push_data_in_chunks(client: Client, Schema: str, TableName: str, Data: pd.DataFrame,
                        TimeStampColumn: str = None, chunk_size: int = 5000):
    """
    Push DataFrame data to ClickHouse in chunks.
    
    Args:
        client: clickhouse_driver.Client instance
        Schema: ClickHouse database/schema
        TableName: ClickHouse table name
        Data: pandas DataFrame
        TimeStampColumn: optional timestamp column name
        chunk_size: number of rows per insert
    """
    assert isinstance(Data, pd.DataFrame), f"Expected DataFrame, got {type(Data)}"
    Data = Data.dropna(axis=1, how='all')
    if Data.empty:
        print("No data to insert.")
        return

    ColumnNames, _ = gen_rows_for_insert(Data.head(1), TimeStampColumn)
    col_str = "(" + ", ".join(ColumnNames) + ")"

    total_rows = len(Data)
    print(f"Total rows to insert: {total_rows}")

    start_total = time.time()

    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        chunk = Data.iloc[start_idx:end_idx]
        _, Rows = gen_rows_for_insert(chunk, TimeStampColumn)

        start = time.time()
        client.execute(
            f"INSERT INTO {Schema}.{TableName} {col_str} VALUES",
            Rows,
            types_check=True
        )
        print(f"Inserted rows {start_idx} to {end_idx-1} in {round(time.time() - start, 2)}s")

    print(f"All data inserted in {round(time.time() - start_total, 2)}s")