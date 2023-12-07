import os
import io

import logging
import requests
import pandas as pd

from google.cloud import bigquery


def download_and_store_weather_report(event, context):
    '''
    This function fetches the weather data from the public API and stores it to BigQuery table
    '''
    start_date = os.environ.get("START_DATE")
    end_date = os.environ.get("END_DATE")
    base_url = os.environ.get("BASE_WEATHER_URL")
    api_key = os.environ.get("API_KEY")
    dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
    table_id = os.environ.get('BIGQUERY_TABLE_ID')

    # Set the logging level
    logging.basicConfig(level=logging.INFO)

    # Fetch weather data from the public API
    weather_data = fetch_weather_data(start_date, end_date, base_url, api_key)

    # Store the weather data to BigQuery table
    store_weather_data(weather_data, dataset_id, table_id)

    return 'Weather data fetched and stored successfully.'

def fetch_weather_data(start_date, end_date, base_url, api_key):
    '''
    This function fetches the weather data from the public API based on the start and end date
    Input: Start date, end date, base_url, api_key
    Output: DataFrame with weather data
    '''
    # Format start and end date from DD.mm.YYYY to match the API requirements YYYY-MM-DD
    start_date = start_date.split(".")[2] + "-" + start_date.split(".")[1] + "-" + start_date.split(".")[0]
    end_date = end_date.split(".")[2] + "-" + end_date.split(".")[1] + "-" + end_date.split(".")[0]

    # Construct the URL
    url = base_url.format(
        start_date=start_date,
        end_date=end_date,
        api_key=api_key)
    
    # Fetch the data
    response = requests.get(url)
    logging.info("Response status code: %s", response.status_code)

    # Define the column names and types:
    data_types = {
    'name': 'object',
    'tempmax': 'float',
    'tempmin': 'float',
    'temp': 'float',
    'feelslikemax': 'float',
    'feelslikemin': 'float',
    'feelslike': 'float',
    'dew': 'float',
    'humidity': 'float',
    'precip': 'float',
    'precipprob': 'float',
    'precipcover': 'float',
    'preciptype': 'object',
    'snow': 'float',
    'snowdepth': 'float',
    'windgust': 'float',
    'windspeed': 'float',
    'winddir': 'float',
    'sealevelpressure': 'float',
    'cloudcover': 'float',
    'visibility': 'float',
    'solarradiation': 'float',
    'solarenergy': 'float',
    'uvindex': 'float',
    'severerisk': 'float',
    'moonphase': 'float',
    'conditions': 'object',
    'description': 'object',
    'icon': 'object',
    'stations': 'object'
    }

    # Convert the response to a DataFrame from csv response
    df = pd.read_csv(io.StringIO(response.text), dtype=data_types, parse_dates=['datetime', 'sunrise', 'sunset'])

    return df

def store_weather_data(df, dataset_id, table_id):
    '''
    This function stores the weather data to BigQuery table
    Input: DataFrame with weather data, dataset_id, table_id
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
        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("datetime", "TIMESTAMP"),
            bigquery.SchemaField("tempmax", "FLOAT"),
            bigquery.SchemaField("tempmin", "FLOAT"),
            bigquery.SchemaField("temp", "FLOAT"),
            bigquery.SchemaField("feelslikemax", "FLOAT"),
            bigquery.SchemaField("feelslikemin", "FLOAT"),
            bigquery.SchemaField("feelslike", "FLOAT"),
            bigquery.SchemaField("dew", "FLOAT"),
            bigquery.SchemaField("humidity", "FLOAT"),
            bigquery.SchemaField("precip", "FLOAT"),
            bigquery.SchemaField("precipprob", "FLOAT"),
            bigquery.SchemaField("precipcover", "FLOAT"),
            bigquery.SchemaField("preciptype", "STRING"),
            bigquery.SchemaField("snow", "FLOAT"),
            bigquery.SchemaField("snowdepth", "FLOAT"),
            bigquery.SchemaField("windgust", "FLOAT"),
            bigquery.SchemaField("windspeed", "FLOAT"),
            bigquery.SchemaField("winddir", "FLOAT"),
            bigquery.SchemaField("sealevelpressure", "FLOAT"),
            bigquery.SchemaField("cloudcover", "FLOAT"),
            bigquery.SchemaField("visibility", "FLOAT"),
            bigquery.SchemaField("solarradiation", "FLOAT"),
            bigquery.SchemaField("solarenergy", "FLOAT"),
            bigquery.SchemaField("uvindex", "FLOAT"),
            bigquery.SchemaField("severerisk", "FLOAT"),
            bigquery.SchemaField("sunrise", "TIMESTAMP"),
            bigquery.SchemaField("sunset", "TIMESTAMP"),
            bigquery.SchemaField("moonphase", "FLOAT"),
            bigquery.SchemaField("conditions", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("icon", "STRING"),
            bigquery.SchemaField("stations", "STRING"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Load the data into BigQuery
    load_job = client.load_table_from_dataframe(
        df,
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
