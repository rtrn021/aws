from pytest_bdd import scenarios, given, when, then, parsers
from test.e2e.utilities.aws import *

resource = boto3.resource('s3')