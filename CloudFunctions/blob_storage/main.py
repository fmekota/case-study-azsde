import io
import os
import logging

import requests
import pandas as pd

from google.cloud import bigquery
from google.cloud import pubsub_v1

def trigger_followup_processing(topic_name, project_id):
    '''
    This function triggers the follow-up processing by publishing a message to a Pub/Sub topic.
    Input: topic_name, project_id
    Output: None
    '''

    # Initialize the Pub/Sub client
    publisher = pubsub_v1.PublisherClient()

    # Construct the fully qualified topic path
    topic_path = publisher.topic_path(project_id, topic_name)

    # Publish the message
    future = publisher.publish(topic_path, b'Follow-up processing triggered')
    try:
        # When result() is called, the publish call has either succeeded or failed.
        future.result()
        logging.info("Message published.")
    except Exception as e:
        logging.error(f"An error occurred when publishing the message: {e}")
        return str(e)



def cleanup_download(csv_file):
    '''
    This function cleans up the downloaded CSV file.
    Input: CSV file
    Output: Cleaned up CSV file as pandas DataFrame
    '''
    # Read the data into a Pandas DataFrame
    df = pd.read_csv(csv_file, on_bad_lines="skip")

    # Clean the DataFrame
    df.iloc[:, 0] = pd.to_numeric(df.iloc[:, 0], errors='coerce').fillna(0).astype(int)
    
    df.iloc[:, 1] = df.iloc[:, 1].astype(str)
    
    df.iloc[:, 2] = df.iloc[:, 2].astype(str).str.replace(',', '.').astype(float)

    df.convert_dtypes()

    # Log the data types and number of missing values
    logging.info("Data type of column 0: %s, number of nans: %s", df.iloc[:, 0].dtype, df['bike_id'].isnull().sum())
    logging.info("Data type of column 1: %s, number of nans: %s", df.iloc[:, 1].dtype, df['manufacturer'].isnull().sum())
    logging.info("Data type of column 2: %s, number of nans: %s", df.iloc[:, 2].dtype, df['manufacturer'].isnull().sum())

    return df

def calculate_max_error(df, max_error_percent):
    '''
    This function calculates the maximum number of errors allowed to keep a specified percent of the data.
    Input: Pandas DataFrame, maximum error percent
    Output: Maximum number of errors allowed
    '''
    # Calculate number of records that can be errored out to keep specified percent of the data
    num_records = len(df)
    num_errors = round(num_records * (max_error_percent / 100))
    logging.info("Number of records: %s", num_records)
    logging.info("Number of allowed errors: %s", num_errors)

    return num_errors


def download_and_upload_to_bq(request):
    '''
    This function downloads a file from Azure Blob Storage and uploads it to BigQuery.
    '''
    # Initialize the BigQuery client
    bq_client = bigquery.Client()

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Azure Blob Storage SAS URL
    sas_url = os.environ.get('AZURE_BLOB_SAS_URL')

    # BigQuery dataset and table information
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    table_id = os.environ.get('BIGQUERY_TABLE_ID')

    # Maximum error percent allowed
    max_error_percent = float(os.environ.get('MAX_ERROR_PERCENT'))

    # Pub/Sub topic name and project ID
    topic_name = os.environ.get('TOPIC_NAME')
    project_id = os.environ.get('PROJECT_ID')

    try:
        # Send an HTTP GET request to the Azure Blob Storage URL
        response = requests.get(sas_url)
        logging.info("Response status code: %s",  response.status_code)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Convert bytes response to a file-like object
            data_file = io.BytesIO(response.content)

            # Clean up the CSV file
            cleaned_data = cleanup_download(data_file)

            # Create a BigQuery dataset reference
            dataset_ref = bq_client.dataset(dataset_id)

            # Create a BigQuery table reference
            table_ref = dataset_ref.table(table_id)

            # Calculate the maximum number of errors allowed
            num_errors = calculate_max_error(cleaned_data, max_error_percent)

            # Configure the job to append data to the table
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip the header row
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                max_bad_records=num_errors,  # Number of errors allowed to keep specified percent of the data

            )

            # Load the data into the BigQuery table
            load_job = bq_client.load_table_from_dataframe(cleaned_data, table_ref, job_config=job_config)
            load_job.result()

            # Check if the load job completed successfully
            if load_job.state == 'DONE':
                if load_job.errors:
                    logging.error("Load job completed with errors: %s", load_job.errors)
                    trigger_followup_processing(topic_name, project_id)
                    return "Load job completed with errors"
                else:
                    logging.info("Data successfully loaded into BigQuery table %s.%s", dataset_id, table_id)
                    trigger_followup_processing(topic_name, project_id)
                    return f"Data loaded into BigQuery table {dataset_id}.{table_id}"
            else:
                logging.error("Load job did not complete successfully. Final state: %s", load_job.state)
                return f"Load job did not complete successfully. Final state: {load_job.state}"

        else:
            logging.error("Failed to download file. Status code: %s", response.status_code)
            return f"Failed to download file. Status code: {response.status_code}"

    except Exception as e:
        logging.error(e)
        return str(e)
