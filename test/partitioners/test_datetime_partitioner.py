# pylint: skip-file
import time
from datetime import datetime, timedelta

from athena_glue_service_logs.partitioners.datetime_partitioner import DateTimePartitioner

from utils import S3Stubber


def now():
    return datetime.utcfromtimestamp(time.time())


def last_hour():
    return datetime.utcfromtimestamp(time.time()) - timedelta(hours=1)


def basic_s3_key():
    return {
        'Key': '/2020/06/11/18/some_data.json.gz',  # '/2020/06/11/18/some_data.json.gz'
        'LastModified': datetime(2020, 6, 23, hour=5),
        'ETag': 'string',
        'Size': 123,
        'StorageClass': 'STANDARD',
        'Owner': {
            'DisplayName': 'string',
            'ID': 'string'
        }
    }


def request_params():
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': '/'}


def today_objects():
    return [
        {
            'Key': '/%s/some_data.json.gz' % now().strftime("%Y/%m/%d/%H"),
            'LastModified': datetime(2020, 6, 23, hour=17),
            'ETag': 'string',
            'Size': 123,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        },
        {
            'Key': '/%s/more_data.json.gz' % now().strftime("%Y/%m/%d/%H"),
            'LastModified': datetime(2020, 6, 23, hour=18),
            'ETag': 'string',
            'Size': 123,
            'StorageClass': 'STANDARD',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        }
    ]


def yesterday_objects():
        return [
            {
                'Key': '/%s/some_data.json.gz' % last_hour().strftime("%Y/%m/%d/%H"),
                'LastModified': datetime(2020, 6, 22, hour=17),
                'ETag': 'string',
                'Size': 123,
                'StorageClass': 'STANDARD',
                'Owner': {
                    'DisplayName': 'string',
                    'ID': 'string'
                }
            }
        ]


def list_request_for_ts(timestamp):
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': timestamp.strftime("%Y/%m/%d/%H")}


def today_objects_request():
    return {'Bucket': 'nowhere', 'MaxKeys': 10, 'Prefix': now().strftime("%Y/%m/%d/%H")}


def test_partition_scanner(mocker):
    datetime_part = DateTimePartitioner(s3_location="s3://nowhere")
    now = datetime.utcfromtimestamp(time.time())

    s3_stub = S3Stubber.for_single_request('list_objects_v2', request_params(), [basic_s3_key()])
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_tuples = datetime_part.build_partitions_from_s3()

    assert new_tuples[0] == ['2020', '06', '11', '18']
    assert new_tuples[-1] == now.strftime('%Y-%m-%d-%H').split('-')


def test_partition_builder():
    datetime_part = DateTimePartitioner(s3_location="s3://nowhere")
    response = datetime_part.build_partitioned_path(['2017', '08', '11', '19'])

    assert response == 's3://nowhere/2017/08/11/19'


def test_partition_key_order():
    """Partition keys should be returned in order"""
    datetime_part = DateTimePartitioner(s3_location="s3://nowhere")
    key_names = [x['Name'] for x in datetime_part.partition_keys()]
    assert key_names == ['year', 'month', 'day', 'hour']


def test_find_new_partitions(mocker):
    datetime_part = DateTimePartitioner(s3_location="s3://nowhere")
    existing_part = last_hour().strftime('%Y-%m-%d-%H').split('-')

    s3_stub = S3Stubber.for_single_request('list_objects_v2', today_objects_request(), today_objects())
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_partitions = datetime_part.find_recent_partitions([existing_part])

    assert len(new_partitions) == 1
    assert new_partitions == [now().strftime('%Y-%m-%d-%H').split('-')]


def test_find_all_new_partitions(mocker):
    datetime_part = DateTimePartitioner(s3_location="s3://nowhere")

    requests = []
    # Create request parameters for every day since (and including) today
    for i in range(DateTimePartitioner.MAX_RECENT_HOURS):
        requests.append(list_request_for_ts(now() - timedelta(hours=i)))

    s3_stub = S3Stubber.for_multiple_requests(
        'list_objects_v2',
        requests,
        [today_objects(), yesterday_objects()] + [[]]*(DateTimePartitioner.MAX_RECENT_HOURS-2)
    )
    with mocker.patch('boto3.client', return_value=s3_stub.client):
        with s3_stub.stubber:
            new_partitions = datetime_part.find_recent_partitions([])

    # Only 2 hours have data
    assert len(new_partitions) == 2
    assert new_partitions == [now().strftime('%Y-%m-%d-%H').split('-'), last_hour().strftime('%Y-%m-%d-%H').split('-')]
