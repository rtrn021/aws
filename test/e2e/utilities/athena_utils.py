import boto3

def query_athena(query,db,workgroup):

    athena = boto3.client('athena')
    QueryExecutionContext = {'Database' : db}
    bucket = "athena-results-3"
    path = "Unsaved/"
    output_location = f's3://{bucket}/{path}'
    ResultConfiguration = {'OutputLocation': output_location}

    response = athena.start_query_execution(QueryString = query,QueryExecutionContext = QueryExecutionContext,ResultConfiguration=ResultConfiguration )
    print(response)
    print('Start')

    print('End')


query = """SELECT * FROM "sampledb"."elb_logs" limit 10"""
workgroup = 'primary'
db = 'sampledb'

query_athena(query,db,workgroup)