import os
import io
import logging

import requests
import pandas as pd

from google.cloud import bigquery

def get_rate_data_and_upload_to_bq(request):
    '''
    This function fetches the text files from the public API, merges them together, filters them, and saves them into BigQuery.
    '''
    
    # Get the date range from the config
    start_date = os.environ.get("START_DATE")
    end_date = os.environ.get("END_DATE")
    base_url = os.environ.get("BASE_CNB_URL")
    currency = os.environ.get("CURRENCY")
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    table_id = os.environ.get('BIGQUERY_TABLE_ID')
    topic_name = os.environ.get('PUBSUB_TOPIC_NAME')
    project_id = os.environ.get('PROJECT_ID')

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Fetch text files from the public API based on the date range
    files = fetch_files(start_date, end_date, base_url)

    # Merge and filter the text files
    merged_files = merge_files(files)
    filtered_files = filter_files(merged_files, start_date, end_date, currency)

    # Save the filtered files into BigQuery
    save_to_bigquery(filtered_files, dataset_id, table_id)

    return "Files fetched, merged, filtered, and saved to BigQuery successfully"

def fetch_files(start_date, end_date, base_url):
    '''
    This function fetches the text files from the public API based on the date range.
    CNB API provides file for each year, therefore we need to extract the year from the date range. Then we iteratively fetch data from the API for each year.
    Input: Start date, end date
    Output: List of fetched files
    '''
    # Initialize the list of files
    files = []

    # Extract the year from the start and end date
    start_year = start_date.split(".")[2]
    end_year = end_date.split(".")[2]

    # Iterate over the range of years
    for year in range(int(start_year), int(end_year) + 1 ): #TODO: check if the daterange is inclusive or exclusive
        # Construct the URL
        url = base_url + str(year)

        # Fetch the file
        response = requests.get(url)

        # Append the file to the list
        files.append(response.text)
        logging.info("Fetched file for year %s", year)
       
    return files

def merge_files(files):
    '''
    This function loads each of the files as a pandas dataframe, and merge them together into a single dataframe.
    Input: List of files
    Output: Merged file
    '''
    # Initialize the list of dataframes
    dfs = []

    # Iterate over the list of files
    for file in files:
        # Load the file as a dataframe
        df = pd.read_csv(io.StringIO(file), sep="|")

        # Append the dataframe to the list
        dfs.append(df)
    
    # Merge the dataframes together
    merged_file = pd.concat(dfs)

    return merged_file

def filter_files(merged_file, start_date, end_date, currency):
    '''
    This function filters the merged file to get only dates from specified date range and for specified currency.
    Input: Merged file, start date, end date, currency
    Output: Filtered file
    '''
    # Clear rows that have value that cannot be formatted to %d.%m.%Y in Datum column
    merged_file = merged_file[merged_file["Datum"].str.contains(r"^\d{2}\.\d{2}\.\d{4}$")]

    # Convert the date columns to datetime
    merged_file["Datum"] = pd.to_datetime(merged_file["Datum"], format="%d.%m.%Y")

    # Filter the merged file based on the date range
    filtered_file = merged_file[(merged_file["Datum"] >= pd.Timestamp(start_date)) & (merged_file["Datum"] <= pd.Timestamp(end_date))] #TODO: check if the daterange is inclusive or exclusive

    # Get only the column for the specified currency in BQ float formatt
    filtered_file["Rate"] = filtered_file[ "1 " + currency].str.replace(',', '.').astype(float)

    # Drop the columns that are not needed
    filtered_file = filtered_file[["Datum", "Rate"]]

    # Formatt datum column to BQ date format YYYY-MM-DD
    filtered_file["Datum"] = filtered_file["Datum"].dt.date


    logging.info("Head of the filtered dataframe: %s", filtered_file.head())

    return filtered_file

def save_to_bigquery(filtered_file, dataset_id, table_id):
    '''
    This function saves the filtered file into BigQuery.
    Input: Filtered file, dataset ID, table ID
    Output: None
    '''
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Create the BigQuery dataset reference
    dataset_ref = client.dataset(dataset_id)

    # Create the BigQuery table reference
    table_ref = dataset_ref.table(table_id)

    # Create the BigQuery job config
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=False,
        schema=[
            bigquery.SchemaField("Datum", "DATE"),
            bigquery.SchemaField("Rate", "FLOAT"),
        ],
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load the data into BigQuery
    load_job = client.load_table_from_dataframe(
        filtered_file,
        table_ref,
        job_config=job_config
    )

    load_job.result()

    # Check if the load job completed successfully
    if load_job.state == 'DONE':
        if load_job.errors:
            logging.error("Load job completed with errors: %s", load_job.errors)
            return "Load job completed with errors"
        else:
            logging.info("Data successfully loaded into BigQuery table %s.%s", dataset_id, table_id)
            return f"Data loaded into BigQuery table {dataset_id}.{table_id}"
    else:
        logging.error("Load job did not complete successfully. Final state: %s", load_job.state)
        return f"Load job did not complete successfully. Final state: {load_job.state}"

