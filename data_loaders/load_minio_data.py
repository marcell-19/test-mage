from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_s3_bucket(*args, **kwargs):
    type = kwargs.get('type')
    year = kwargs.get('year')
    month = kwargs.get('month')
    filename = f'{type}_tripdata_{year}-{month}.parquet'
    
    # Configure MinIO connection
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    # MinIO details
    bucket_name = 'zoomcamp'
    object_key = f'bronze/{filename}'

    return S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
