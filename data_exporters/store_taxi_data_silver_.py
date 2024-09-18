from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_s3(df: DataFrame, **kwargs) -> None:
    type = kwargs.get('type')
    year = kwargs.get('year')
    month = kwargs.get('month')
    filename = f'{type}_tripdata_{year}-{month}.parquet'
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    # MinIO details
    bucket_name = 'zoomcamp'
    object_key = f'silver/{filename}'

    S3.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
