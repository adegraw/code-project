
```markdown
# CSV to Parquet Bulk Converter

A Python utility for bulk converting CSV files to Parquet format using Pandas and PyArrow. Designed for data engineers and analysts who need fast, reliable transformation across large datasets.

---

## Features

- Batch conversion of CSV files in a specified directory
- Duplicate filename handling to prevent overwrites
- Logging to both file and console for traceability
- JSON summary report with row counts and file sizes
- Command-line interface using `argparse`

---

## Installation

Make sure you have Python 3.7+ installed. Then install the required packages:

```bash
pip install pandas pyarrow
```

---

## Usage

Run the script from the command line:

```bash
python csv_to_parquet_bulk_converter.py --source "C:/path/to/csvs" --target "C:/path/to/output"
```

- If `--target` is omitted, output files will be saved in the source directory.
- The script will automatically create the target directory if it doesn't exist.

---

## Output

After running the script, you'll get:

- `.parquet` files for each CSV in the target directory
- A log file named `CSV_to_Parquet_BULK.log` with detailed conversion info
- A JSON summary file named `Bulk_Run_Summary_YYYYMMDD_HHMMSS.json` containing:
  - Source and target paths
  - File-level metadata (row counts, sizes)
  - Total files processed and total rows converted

---

## Requirements

- Python 3.7 or higher
- Libraries:
  - `pandas`
  - `pyarrow`

---

## License

This project is licensed under the **MIT License**.  
You’re free to use, modify, and distribute it with attribution.

---

## Author

**Allen DeGraw**  
Database Engineer, Senior ETL Developer, & Data Integration Specialist
LinkedIn: www.linkedin.com/in/allen-degraw-68577265
GitHub: https://github.com/adegraw
```
