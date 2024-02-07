# NYC Taxi Data Processing Pipeline

This Python script utilizes [Apache Beam](https://beam.apache.org/) to process NYC taxi data. The script reads input data in Parquet format, performs column renaming and formatting, and then writes the processed data back to Parquet files.

This project uses a serverless cloud stack solution in Google Cloud Platform (GCP):

- Google Cloud Storage
- Dataflow - Apache Beam Runner
- BigQuery

Please have a look at the generated report produced in Looker Studio titled `AN_Generated_Dashboard.pdf`. Feel free to reach out if you want to find out more about my methodologies and hypothesis. BQ statements are truncated to hide the project.

## Point of Improvements

- CRON scheduler (Cloud Scheduler)
- Archiving/failure path (Cloud Function)
- Infrastructure as Code (Terraform)

Otherwise the three features can be implemented in Airflow via Cloud Composer (higher cost)

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
