import boto3
from botocore.exceptions import ClientError
import logging
from io import StringIO
import pandas as pd
import test.e2e.utilities.path_utils as path_utils

data_path = path_utils.add_path_to_project_path('data')


def get_s3_client():
    try:
        client = boto3.client('s3')
    except ClientError as ce:
        logging.error(f'Failed to create an instance of boto3 client Error: {ce}')
        return False
    return client


def get_s3_resource():
    try:
        resource = boto3.resource('s3')
    except ClientError as ce:
        logging.error(f'Failed to create an instance of boto3 client Error: {ce}')
        return False
    return resource

def get_glue_client():
    try:
        glue_client = boto3.client('glue')
    except ClientError as ce:
        logging.error(f'Failed to create an instance of boto3 client Error: {ce}')
        return False
    return glue_client

def get_lambda_client():
    try:
        lambda_client = boto3.client('lambda')
    except ClientError as ce:
        logging.error(f'Failed to create an instance of boto3 client Error: {ce}')
        return False
    return lambda_client


def upload_file(file_path, bucket, target_path):
    try:
        get_s3_client().upload_file(file_path, bucket, target_path)
    except ClientError as ce:
        logging.error(f'Failed to upload file to S3 bucket, Error: {ce}')
        return False
    return True


def upload_csv_file_to_stag(file_name, bucket='rt-stag', target_path='csv'):
    """ex:upload_csv_file_to_stag('NBA', target_path='csv/NBA.csv')"""
    try:
        get_s3_client().upload_file(f'{data_path}/csv/{file_name}.csv', bucket, target_path)
    except ClientError as ce:
        logging.error(f'Failed to upload file to S3 bucket, Error: {ce}')
        return False
    return True


def csv_to_parquet(file_name, csv_filepath, parquet_filepath):
    df = pd.read_csv(f'{csv_filepath}.csv')
    df.to_parquet(f'{parquet_filepath}.parquet')
    print("=== Test ===")
    print(pd.read_parquet(f'{data_path}/parquet/{file_name}.parquet').head())


def csv_to_parquet(file_name):
    df = pd.read_csv(f'{data_path}/csv/{file_name}.csv')
    df.to_parquet(f'{data_path}/parquet/{file_name}.parquet')
    print("=== Test ===")
    print(pd.read_parquet(f'{data_path}/parquet/{file_name}.parquet').head())


def upload_df_to_s3_as_csv(df, bucket, target_path):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    get_s3_resource().Object(bucket, target_path).put(Body=csv_buffer.getvalue())
