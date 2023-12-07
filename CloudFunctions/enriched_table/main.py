import os
import logging

from google.cloud import bigquery

import enriched_table

def create_enriched_table(request):
    '''
    Creates a new table in BigQuery, combining all data sources
    '''
    # Set up logging
    logging.basicConfig(level=logging.INFO)

    # Get the environment variables
    project_id = os.environ.get('PROJECT_ID')
    oz_dataset_id = os.environ.get('BIGQUERY_OZ_DATASET_ID')
    lz_dataset_id = os.environ.get('BIGQUERY_LZ_DATASET_ID')
    table_id = os.environ.get('BIGQUERY_TABLE_ID')


    # Create a BigQuery client
    client = bigquery.Client()

    # Define the SQL query to create the table
    query = enriched_table.SQL.format(project_id=project_id,
                                      oz_dataset_id=oz_dataset_id,
                                      lz_dataset_id=lz_dataset_id,
                                      table_id=table_id)

    # Execute the query
    job = client.query(query)
    job.result()  # Wait for the query to complete

    return 'Table created successfully'
