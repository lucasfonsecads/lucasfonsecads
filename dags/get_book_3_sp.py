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
import time
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

dag = DAG('get_data_book_3',
          description='Dag TJSP - Book three',
          schedule_interval=None,
          catchup=False,
          default_args=default_args)


# Upload files to s3
def upload_file(file_name, bucket, week, object_name):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # the name of file need to be 2021-11-02.pdf
    new_object_name = 'week-{0}-{1}-book-3/{2}'.format(week, state, object_name)

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

    return ({
        'statusCode': 200,
        'message': 'Upload book in to S3'
    })

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


    logger.info('Start request: {}'.format(str(datetime.now())))

    try:
        control = 0
        for control in range(len(week_period)):
            print('Start process to get book number: {0} | At: {1}'.format(control, str(datetime.now())))
            response = requests.get('{0}?dtDiario={1}&cdCaderno=12'.format(URL, week_period[control]))
            # Need to fix time request completed
            _start = time.time()
            print("Request completed in {0:.2f} s".format(response.elapsed.total_seconds()))
            fileobject = BytesIO(response.content)
            name_object = '{}.pdf'.format(str(week_period[control]))
            print('File created: {}'.format(str(name_object)))
            upload_file(fileobject, bucket, week_number, object_name=name_object)
            control = + 1
        print('Request book: {} END | AT: {}'.format(week_number, str(datetime.now())))
    except ValueError as err:
        return {
            'statusCode': 400,
            'body': json.dumps(err)}



t1 = PythonOperator(
    task_id='get_book',
    dag=dag,
    python_callable=get_book
)

t1
