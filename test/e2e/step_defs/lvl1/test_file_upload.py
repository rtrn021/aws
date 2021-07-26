from pytest_bdd import scenarios, given, when, then, parsers
from test.e2e.utilities.aws import *

scenarios('../../features/lvl1/file_upload.feature')


@given('Lets Start')
def start():
    print()
    print('Lets Start!')


@when(parsers.parse('I upload "{file_name}" to "{bucket}"'))
def upload_file_to_bucket(file_name, bucket):
    upload_csv_file_to_stag(file_name, bucket, target_path=f'csv/{file_name}.csv')
