import logging
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

bucket = Variable.get('BUCKET')

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fonseca',
    'start_date': datetime(2021, 11, 20),
    'depends_on_past': False,
    'provide_context': True,
    'schedule_interval': '00***',
}

"""
Need to check if we can check folder per folder for a better performance, and reduce number of consults in Paginator object
"""

def delete_empty_files():
    client = boto3.client('s3',
                          aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'))

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=bucket, PaginationConfig={'MaxItems': 1000})

    s3 = boto3.resource('s3')


    filtered_iterator = page_iterator.search("Contents[?Size < `38600`][]")

    for key_data in filtered_iterator:
        all_data = [key_data]
        print('Removing empty file: {}'.format(all_data[0]['Key']))
        s3.Object(bucket, all_data[0]['Key']).delete()

    return ({
        'statusCode': 200,
        'message': 'Delete empty files'
    })

with DAG(
    'delete_empty_files_s3',
    default_args=default_args,
    description='Job to delete empty files from S3',
    catchup=False
    )as dag:

    remove_empty_files = PythonOperator(
        task_id='remove_empty_files',
        python_callable=delete_empty_files,
    )

    remove_empty_files