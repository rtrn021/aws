from pytest_bdd import scenarios, given, when, then, parsers
from test.e2e.utilities.aws import *

scenarios('../../features/lvl1/run_etl.feature')

job_name = 'pokemon-etl'


@given('Lets Start')
def start():
    print()
    print('Lets Start!')


@when('I run etl')
def upload_file_to_bucket():
    # run_etl(job_name)
    pass