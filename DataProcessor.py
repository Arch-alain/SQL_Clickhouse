import os
import io
import gzip
import zipfile
import json
import logging
import requests
import polars as pl
import pandas as pd
from dotenv import load_dotenv
from utils import *
import argparse

# ------------------- Logging Setup -------------------
load_dotenv()

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")
DATA_DIR=os.getenv("DATA_DIR")
DATA_DIR_ZIPPED=os.getenv("DATA_DIR_ZIPPED")
LOGS_DIR=os.getenv("LOGS_DIR")
PROCESSED_LOG = os.getenv("PROCESSED_LOG")
PROCESSED_LOG_ZIPPED = os.getenv("PROCESSED_ZIPPED_LOG")




# ------------------- Ensure directories exist -------------------
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ------------------- Logging Setup -------------------
LOG_FILE = os.path.join(LOGS_DIR, "data_ingestion.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        # logging.StreamHandler()  # prints logs to console as well
    ]
)

# ------------------- Main Function -------------------
def main():
    logging.info("=== Data Ingestion Process Started ===")

    # ------------------- Parse arguments -------------------
    parser = argparse.ArgumentParser(description="A script that receives arguments.")
    parser.add_argument("--url", type=str, required=False, help="URL to download the data from")
    parser.add_argument("--unzipping", action="store_true", help="Process already downloaded zip files")

    args = parser.parse_args()

    # ---------------- Download Mode ----------------
    if args.url:
        try:
            logging.info(f"Downloading file from URL: {args.url}")
            downloaded_file = download_file(args.url, DATA_DIR_ZIPPED)
            logging.info(f"Downloaded: {downloaded_file}")
        except Exception as e:
            logging.error(f"Error downloading file: {e}", exc_info=True)
            raise

    # ---------------- Unzip Mode ----------------
    if args.unzipping:
        try:
            print("God I want to finish this task today")
            logging.info("=== Unzipping new files ===")
            
            # Load processed zipped file hashes
            processed_hashes = load_processed_files(PROCESSED_LOG_ZIPPED)
            print(f"{processed_hashes}")
            
            # Find new files to unzip
            new_files = get_newzipped__files(DATA_DIR_ZIPPED, processed_hashes)
            # print( new_files)
            if not new_files:
                logging.info("No new files to unzip.")
            else:
                for file_path, file_hash in new_files:
                    extracted_files = decompress_file(file_path, DATA_DIR)
                    logging.info(f"Unzipped {file_path} → {extracted_files}")
                    print(f"Processing: {file_path}")

                    processed_hashes.append(file_hash)
                    save_processed_files(processed_hashes,PROCESSED_LOG_ZIPPED)
                    
                    # Update processed log

        except Exception as e:
            logging.error(f"Error during unzipping: {e}", exc_info=True)
            raise

    logging.info("=== Data Ingestion Process Finished ===")

# def main():

#     logging.info("=== Data Ingestion Process Started ===")
#     # print(LOG_FILE)
#     parser = argparse.ArgumentParser(description="A script that receives arguments.")
#     parser.add_argument("--url", type=str, required=False)
#     # parser.add_argument("--age", type=int, required=False)

#     args = parser.parse_args()
#     if args.url:
#         logging.info("=== Data Ingestion Process Started ===")

#         print(args.url)
#         all_files=os.listdir(DATA_DIR)
#         print(all_files)
        # try:
        #     logging.info("=== Data Ingestion Process Started ===")
        #     downloaded_file = download_file(URL, DATA_DIR)
        #     extracted_file = decompress_file(downloaded_file)
        #     df = load_json_or_jsonl_pandas(extracted_file)
        #     logging.info("=== Data Ingestion Process Completed Successfully ===")

        # except Exception as e:
        #     logging.error(f"Error during data ingestion: {e}", exc_info=True)
        #     raise


if __name__ == "__main__":
    # logging.info("=== Data Ingestion Process Started ===")

    main()