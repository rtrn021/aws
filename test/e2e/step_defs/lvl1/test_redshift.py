import json

from pytest_bdd import scenarios, given, when, then, parsers
from test.e2e.utilities.aws import *
import botocore.session as bc

scenarios('../../features/lvl1/redshift.feature')

redshift_cluster_id = 'redshift-cluster-1'
redshift_database = 'dev'
redshift_user = 'awsuser'
aws_region_name = 'eu-west-2'


def formatRecords(meta, records):
    columns = [k['name'] for k in meta]
    rows = []
    for r in records:
        tmp = []
        for c in r:
            tmp.append(c[list(c.keys())[0]])
        rows.append(tmp)
    return pd.DataFrame(rows, columns=columns)


def get_client(service, region=aws_region_name):
    session = bc.get_session()
    s = boto3.Session(botocore_session=session, region_name=region)
    if region:
        return s.client(service)
    return s.client(service)


@given('Lets Start')
def start():
    print()
    print('Lets Start!')


@when('query in redshift')
def upload_file_to_bucket():
    query = """
            select * from dev.public.date limit 10"""

    rsd = get_client('redshift-data')
    resp = rsd.execute_statement(
    ClusterIdentifier=redshift_cluster_id,
    Database=redshift_database,
    DbUser=redshift_user,
    Sql=query)

    queryId = resp['Id']
    print('============resp===========')
    print(resp)
    print('============queryId===========')
    print(queryId)
    stmt = rsd.describe_statement(Id=queryId)
    print('============stmt===========')
    print(stmt)
    desc = rsd.describe_statement(Id=queryId)
    while True:
        desc = rsd.describe_statement(Id=queryId)
        print(f"desc >> {desc}")
        if desc['Status'] == "FINISHED":
            break
            print("========desc ResultRows")
            print(desc["ResultRows"])

    if desc and desc["ResultRows"] > 0:
        result = rsd.get_statement_result(Id=queryId)
        print("result Json"+"\n")
        print(json.dumps(result,indent=3))
        rows, meta = result["Records"], result["ColumnMetadata"]
        df=formatRecords(meta,rows)
        print("\n" + "Results" + "\n")
        print(df)