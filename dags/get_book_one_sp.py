from datetime import datetime
import logging
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
from botocore.exceptions import ClientError
import requests
from datetime import datetime, timedelta
from io import BytesIO
from bizdays import Calendar
import os

bucket = Variable.get('BUCKET')

URL = Variable.get('URL_SP')

cal = Calendar(weekdays=['Sunday', 'Saturday'])

logger = logging.getLogger(__name__)

state = 'SP'


week = 0

default_args = {
    'owner': 'fonseca',
    'start_date': datetime(2020, 5, 20),
    'depends_on_past': False,
    'provide_context': True
}

dag = DAG('get_data_from_tjsp',
          description='Dag TJSP - Book One',
          schedule_interval=None,
          catchup=False,
          default_args=default_args)

"""
TO DO LIST:
- [] Save 5 books in S3
- [] Save the books with correct name
- [] Send the event

"""

# Upload files to s3
def upload_file(file_name, bucket, ti, object_name):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # the name of file need to be 2021-11-02.pdf
    week = ti.xcom_pull(key='actual_week', task_ids='get_folder_s3')
    new_object_name = 'week-{0}-{1}/{2}'.format(week, state, object_name)

    # Upload the file
    s3_client = boto3.client('s3',
                             aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'))

    try:
        s3_client.upload_fileobj(file_name, bucket, new_object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

    week_list = week_list[::-1]

    return week_list


def get_book(ti):
    logger.info('Start request: {}'.format(str(datetime.now())))

    receive = requests.get('{}?dtDiario=27/02/2020&cdCaderno=11'.format(URL))

    print("Request completed in {0:.2f} s".format(receive.elapsed.total_seconds()))
    fileobject = BytesIO(receive.content)
    name_object = 'pdf.pdf'
    print('File created: {}'.format(str(name_object)))
    upload_file(fileobject, bucket, ti, object_name=name_object)

    print('Request book:  | AT: {}'.format(str(datetime.now())))


# Get folder name
def get_folder_s3(week, ti):
    # week-1-book_1/5 books -> change pdf file in csv file
    # week-2-book_1/5 books
    print('Get actual week number')
    # Create a client
    # client = boto3.client('s3',
    #                       aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
    #                       aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'))
    #
    # # Create a reusable Paginator
    # paginator = client.get_paginator('list_objects')
    #
    # # Create a PageIterator from the Paginator
    # page_iterator = paginator.paginate(Bucket=bucket, PaginationConfig={'MaxItems': 10})

    # get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    all_data = []

    filtered_iterator = page_iterator.search("Contents[?Size > `36600`][]")
    for key_data in filtered_iterator:
        all_data = [key_data]

    week_name = all_data[0]['Key']
    actual_week_number = int(week_name.split("/")[0].split("-")[1])
    print('Actual week is: {}'.format(actual_week_number))

    actual_week = actual_week_number
    ti.xcom_push(key='actual_week', value=actual_week)
    return actual_week_number


t1 = PythonOperator(
    task_id='get_book',
    dag=dag,
    python_callable=get_book,
    op_kwargs={'week': week}
)

t2 = PythonOperator(
    task_id='get_folder_s3',
    dag=dag,
    python_callable=get_folder_s3,
    op_kwargs={'week': week}

)

sensor = ExternalTaskSensor(
    task_id='sensor',
    dag=dag,
    external_dag_id='get_work_period_week',
    external_task_id=' No images to push',
    poke_interval=30
)

t2 >> t1