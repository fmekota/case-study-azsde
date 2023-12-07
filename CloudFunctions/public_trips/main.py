import os
import logging

from google.cloud import bigquery

import public_trips

def download_data_from_public_dataset(source_project_id, dataset_id, table_id, bucket_name):
    '''
    This function downloads the data from the public dataset from US destination to cloud storage.
    '''

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Initialize the BigQuery client
    client = bigquery.Client(location='US')

    # Get the dataset reference
    dataset_ref = client.dataset(dataset_id, project=source_project_id)

    # Get the table reference
    table_ref = dataset_ref.table(table_id)

    # Get the destination URI
    destination_uri = "gs://{bucket_name}/{dataset_id}/{table_id}.csv".format(bucket_name=bucket_name, dataset_id=dataset_id, table_id=table_id)

    job_config = bigquery.ExtractJobConfig(
        compression=bigquery.Compression.GZIP,
        destination_format=bigquery.DestinationFormat.CSV,
        field_delimiter="|"
    )

    # Make an API request
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="US",
    )  

    # Wait for the job to complete
    extract_job = extract_job.result()     

def load_data_from_cloud_storage_to_bigquery(project_id, source_dataset_id, table_id, bucket_name, dataset_id):
    '''
    This function loads the data from cloud storage to BigQuery.
    Input parameters:
        project_id: The project ID of the project containing the destination table.
        dataset_id: The ID of the dataset containing the destination table.
        table_id: The ID of the destination table.
        bucket_name: The name of the Cloud Storage bucket where the source data is stored.
    '''

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Initialize the BigQuery client
    client = bigquery.Client(location='europe-west3')

    # Get the dataset reference
    dataset_ref = client.dataset(dataset_id, project=project_id)

    # Get the table reference
    table_ref = dataset_ref.table(table_id)

    # Get the source URI
    source_uri = "gs://{bucket_name}/{source_dataset_id}/{table_id}.csv".format(bucket_name=bucket_name, source_dataset_id=source_dataset_id, table_id=table_id)

    # Create the job config
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        field_delimiter="|",
        max_bad_records=1000
    )

    # Make an API request
    load_job = client.load_table_from_uri(
        source_uri,
        table_ref,
        job_config=job_config,
        location="europe-west3"
    )

    # Wait for the job to complete
    load_job = load_job.result()



def get_trips_from_public_dataset(request):
    '''
    This function fetches the trips from the public dataset using prepared SQL template.
    '''

    # Get environment variables
    source_project_id = os.environ.get('SOURCE_PROJECT_ID')
    source_dataset_id = os.environ.get('SOURCE_DATASET_ID')
    source_trip_table_id = os.environ.get('SOURCE_TRIP_TABLE_ID')
    source_station_table_id = os.environ.get('SOURCE_STATION_TABLE_ID')
    project_id = os.environ.get('PROJECT_ID')
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    table_id = os.environ.get('BIGQUERY_TABLE_ID')
    start_date = os.environ.get('START_DATE')
    end_date = os.environ.get('END_DATE')
    bucket_name = os.environ.get('BUCKET_NAME')

    # Initialize the BigQuery client
    client = bigquery.Client(location='europe-west3')

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Execute function to download the data from the public dataset from US destination to cloud storage
    download_data_from_public_dataset(source_project_id, source_dataset_id, source_trip_table_id, bucket_name)
    download_data_from_public_dataset(source_project_id, source_dataset_id, source_station_table_id, bucket_name)

    # Loaded extracted csv files from cloud storage to BigQuery
    load_data_from_cloud_storage_to_bigquery(project_id, source_dataset_id, source_trip_table_id, bucket_name, dataset_id)
    load_data_from_cloud_storage_to_bigquery(project_id, source_dataset_id, source_station_table_id, bucket_name, dataset_id)

    # Format start_date and end_date to YYYY-MM-DD
    start_date = start_date.split(".")[2] + "-" + start_date.split(".")[1] + "-" + start_date.split(".")[0]
    end_date = end_date.split(".")[2] + "-" + end_date.split(".")[1] + "-" + end_date.split(".")[0]

    # Get the query from the public_trips.SQL
    query = public_trips.SQL.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id, start_date=start_date, end_date=end_date)
    
    try:
        # Make an API request
        query_job = client.query(query)  

        # Wait for the job to complete
        query_job = query_job.result()
        logging.info("Query results loaded to the table %s.%s", dataset_id, table_id)
        return f"Query results loaded to the table {dataset_id}.{table_id}"

    except Exception as e:
        logging.error("Error while executing the query: %s", e)
        return f"Error while executing the query: {e}"  
