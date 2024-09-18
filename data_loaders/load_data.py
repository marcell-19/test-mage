import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from minio import Minio
from minio.error import S3Error
import urllib.parse

minio_client = Minio(
    "minio:9000",
    access_key="superadmin",
    secret_key="superadmin",
    secure=False
)


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    urls = [
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q1%20(Jan-Mar)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q2%20spring%20(Apr-Jun)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q3%20(Jul-Sep)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2014%20Q4%20autumn%20(Oct-Dec)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q1%20(Jan-Mar)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q2%20spring%20(Apr-Jun)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q3%20(Jul-Sep)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2015%20Q4%20autumn%20(Oct-Dec)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q1%20(Jan-Mar)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q2%20spring%20(Apr-Jun)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q3%20(Jul-Sep)-Central.csv',
        'https://cycling.data.tfl.gov.uk/ActiveTravelCountsProgramme/2016%20Q4%20autumn%20(Oct-Dec)-Central.csv',
    ]

    for url in urls:
        response = requests.get(url, stream=True)
        bucket_name = 'cyclingetl'
        foldername = 'raw'

        try:
            # get file name 
            object_name = url.split('/')[-1]
            # object_name = urllib.parse.unquote(object_name)

            # upload to minio
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name='raw/' + object_name,
                data=response.raw,
                length=int(response.headers.get('Content-Length', 0)),
                content_type='text/csv'
            )

            print(f"'{object_name}' has been successfully uploaded to MinIO.")
        except S3Error as e:
            print(f"Error occurred: {e}")

    return 'Success loading data to Minio'



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
