from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import requests
import io
import os
from mage_ai.io.s3 import S3
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



def download_file(filename):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
    response = requests.get(url)
    
    # Ensure the request was successful
    response.raise_for_status()
    
    # Define the local path to save the file
    local_path = os.path.join(get_repo_path(), 'data', filename)
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Save the file locally
    with open(local_path, 'wb') as f:
        f.write(response.content)


@data_loader
def load_api_to_minio(*args, **kwargs):
    # download file
    type = kwargs['configuration'].get('type')
    year = kwargs['configuration'].get('year')

    for i in range(12):
        month = '0'+str(i+1)
        month = month[-2:]
        filename = f'{type}_tripdata_{year}-{month}.parquet'
        download_file(filename)

        # Configure MinIO connection
        config_path = path.join(get_repo_path(), 'io_config.yaml')
        config_profile = 'dev'

        # MinIO details
        bucket_name = 'zoomcamp'
        object_key = f'bronze/{filename}'

        # Store the file in MinIO
        S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
            data=path.join(get_repo_path(), 'data', filename),
            bucket_name=bucket_name,
            object_key=object_key,
        )

    return f"File stored in MinIO at {bucket_name}/{object_key}"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
