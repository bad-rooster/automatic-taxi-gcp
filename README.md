# NYC Taxi Data Processing Pipeline

This Python script utilizes [Apache Beam](https://beam.apache.org/) to process NYC taxi data. The script reads input data in Parquet format, performs column renaming and formatting, and then writes the processed data back to Parquet files.

## Prerequisites

Before running the script, ensure that you have the following installed:

- Python
- Apache Beam
- pyarrow

## Usage

```bash
python nyc_taxi_df_job.py --input <input_path> --output <output_path>
```

- `input_path`: Path to the input Parquet file.
- `output_path`: Path to the directory where the processed Parquet files will be saved.

## Script Overview

1. **Importing Libraries:**

   - The script imports necessary libraries, including `apache_beam` and `pyarrow`.

2. **Taxi Schema:**

   - Defines the schema for the NYC taxi data using the `pyarrow` library.

3. **Column Transformation Functions:**

   - `rename_datetime_cols`: Renames datetime columns for consistency.
   - `formatting_col_name`: Formats specific column names to match the defined schema.

4. **Pipeline Configuration:**

   - Parses command-line arguments using `argparse`.
   - Configures the Apache Beam pipeline options.

5. **Pipeline Execution:**
   - Reads Parquet data from the specified input path.
   - Applies the column renaming and formatting functions.
   - Writes the processed data to Parquet files in the specified output directory.

## Example Usage

```bash
python generate_nyc_taxi_data.py --input /path/to/input_data.parquet --output /path/to/output_directory
```

The script reads the NYC taxi data from the specified input Parquet file, processes it, and saves the results in Parquet files in the specified output directory.

Note: The output files will have a timestamp appended to their names for uniqueness.

Feel free to customize the script according to your specific needs or integrate it into a larger data processing workflow.
