if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from pandas import DataFrame
from os import path


@transformer
def transform(data, *args, **kwargs):
    type = kwargs['configuration'].get('type')
    year = kwargs['configuration'].get('year')
    month = kwargs['configuration'].get('month')

    filename = f'{type}_tripdata_{year}-{month}.parquet'
    bucket_name = 'zoomcamp'
    object_key = f'bronze/{filename}'

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'dev'

    data = S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name=bucket_name,
        object_key=object_key,
    )

    print("Rows with zero passengers: ", data['passenger_count'].isin([0]).sum())

    return data[data['passenger_count'] > 0]
    # return data

@test
def test_output(output, *args):
    assert output['passenger_count'].isin([0]).sum() == 0 # There are rides with zero passengers