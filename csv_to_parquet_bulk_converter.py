import os
import pandas as pd
import logging
import json
import argparse
from datetime import datetime

def setup_logger(log_dir):
    """
    Sets up a logger that writes to a file named CSV_to_Parquet_BULK.log.

    Args:
        log_dir (str): Directory where the log file should be stored.
    """
    os.makedirs(log_dir, exist_ok=True)  # Ensure the directory exists
    log_file = os.path.join(log_dir, "CSV_to_Parquet_BULK.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )

def next_nonexistent(filepath):
    """
    Returns a unique file path by appending a numeric suffix if the file already exists.

    Args:
        filepath (str): Desired file path.

    Returns:
        str: A unique file path that doesn't conflict with existing files.
    """
    root, ext = os.path.splitext(filepath)
    i = 0
    new_filepath = filepath
    while os.path.exists(new_filepath):
        i += 1
        new_filepath = f"{root}_{i}{ext}"
    return new_filepath

def convert_csv_to_parquet(csv_path, parquet_path):
    """
    Converts a single CSV file to Parquet format.

    Args:
        csv_path (str): Full path to the input CSV file.
        parquet_path (str): Desired path for the output Parquet file.

    Returns:
        dict: Metadata including filename, rows, and file size; or None if failed.
    """
    try:
        df = pd.read_csv(csv_path)
        final_path = next_nonexistent(parquet_path)
        df.to_parquet(final_path, engine="pyarrow", index=False)
        file_size = os.path.getsize(final_path)
        row_count = len(df)
        logging.info(f"Converted: {os.path.basename(csv_path)} → {os.path.basename(final_path)}")
        return {
            "filename": os.path.basename(final_path),
            "rows": row_count,
            "size_bytes": file_size
        }
    except Exception as e:
        logging.error(f"Failed to convert {csv_path}: {e}")
        return None

def write_summary_json(summary_data, output_dir):
    """
    Writes a summary of the conversion job to a JSON file.

    Args:
        summary_data (dict): Dictionary containing summary information.
        output_dir (str): Directory where the summary file will be saved.

    Returns:
        str: Path to the saved summary JSON file.
    """
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"Bulk_Run_Summary_{timestamp}.json"
    summary_path = os.path.join(output_dir, filename)
    try:
        with open(summary_path, "w") as f:
            json.dump(summary_data, f, indent=4)
        logging.info(f"Summary JSON written to: {summary_path}")
        return summary_path
    except Exception as e:
        logging.error(f"Failed to write summary JSON: {e}")
        return None

def batch_convert_csvs(source_dir, target_dir=None, csv_ext=".csv", parquet_ext=".parquet"):
    """
    Converts all CSV files in the source directory to Parquet format.

    Args:
        source_dir (str): Path to the folder containing CSV files.
        target_dir (str): Path to the folder where Parquet files will be saved. Defaults to source_dir.
        csv_ext (str): Extension used to identify CSV files.
        parquet_ext (str): Extension to use for output Parquet files.
    """
    if target_dir is None:
        target_dir = source_dir

    start_time = datetime.now()
    logging.info("CSV-to-Parquet conversion job started.")
    logging.info(f"Source directory: {source_dir}")
    logging.info(f"Target directory: {target_dir}")

    converted_files = []

    for filename in os.listdir(source_dir):
        if filename.endswith(csv_ext):
            root, _ = os.path.splitext(filename)
            csv_path = os.path.join(source_dir, filename)
            parquet_path = os.path.join(target_dir, root + parquet_ext)
            result = convert_csv_to_parquet(csv_path, parquet_path)
            if result:
                converted_files.append(result)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logging.info("Job completed.")
    logging.info(f"Duration: {duration:.2f} seconds")
    logging.info(f"Total files converted: {len(converted_files)}")
    if converted_files:
        logging.info("Files created:")
        for f in converted_files:
            logging.info(f"  - {f['filename']} ({f['rows']} rows, {f['size_bytes']} bytes)")
    else:
        logging.info("No files were converted.")

    # Write summary JSON
    summary_data = {
        "job_start": start_time.isoformat(),
        "job_end": end_time.isoformat(),
        "duration_seconds": duration,
        "source_directory": source_dir,
        "target_directory": target_dir,
        "total_files_converted": len(converted_files),
        "converted_files": converted_files
    }
    write_summary_json(summary_data, target_dir)

def parse_args():
    """
    Parses command-line arguments for source and target directories.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Bulk convert CSV files to Parquet format.")
    parser.add_argument("--source", required=True, help="Source directory containing CSV files.")
    parser.add_argument("--target", help="Target directory for Parquet files. Defaults to source.")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    source_dir = args.source
    target_dir = args.target if args.target else args.source

    setup_logger(target_dir)
    batch_convert_csvs(source_dir, target_dir)
