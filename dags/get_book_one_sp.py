from datetime import datetime
import logging
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
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
def upload_file(file_name, bucket, week, object_name):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # the name of file need to be 2021-11-02.pdf
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

    return {
        "message": "Book saved"
    }

def get_last_week_record():
    postgres_hook = PostgresHook(postgres_conn_id='airflow')
    pg_conn = postgres_hook.get_conn()
    cursor = pg_conn.cursor()

    cursor.execute("SELECT * FROM week_period ORDER BY 1 DESC LIMIT 1;")
    record_data = cursor.fetchall()

    for i in record_data:
        week_number = i[2]
        week_period = i[3]

    pg_conn.commit()

    return week_number, week_period



def get_book():

    week_number, week_period = get_last_week_record()

    print(type(week_period))

    # logger.info('Start request: {}'.format(str(datetime.now())))
    #
    # receive = requests.get('{}?dtDiario=27/02/2020&cdCaderno=11'.format(URL))
    #
    # print("Request completed in {0:.2f} s".format(receive.elapsed.total_seconds()))
    # fileobject = BytesIO(receive.content)
    # name_object = 'pdf.pdf'
    # print('File created: {}'.format(str(name_object)))
    #
    # upload_file(fileobject, bucket, ti, object_name=name_object)
    #
    # print('Request book:  | AT: {}'.format(str(datetime.now())))


t1 = PythonOperator(
    task_id='get_book',
    dag=dag,
    python_callable=get_book
)

t1
