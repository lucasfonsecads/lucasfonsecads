from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from bizdays import Calendar
import logging
import json
import boto3

bucket = Variable.get('BUCKET')

cal = Calendar(weekdays=['Sunday', 'Saturday'])

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fonseca',
    'start_date': datetime(2020, 5, 20),
    'depends_on_past': False,
    'provide_context': True,
    'schedule_interval': '00**0',
}

def record_data_on_postgress(new_week_number, new_week):
    postgres_hook = PostgresHook(postgres_conn_id='airflow')
    pg_conn = postgres_hook.get_conn()
    cursor = pg_conn.cursor()

    week_name = "week"
    week_number = new_week_number
    week_period = new_week

    cursor.execute("INSERT INTO week_period ( week_name, week_number, week_period) VALUES ( %s, %s, %s)", [week_name, week_number, json.dumps(week_period)])
    pg_conn.commit()

    return {
        "message": "Save week data in database"
    }


# Get folder name
def get_folder_s3():
    # week-1-book_1/5 books -> change pdf file in csv file
    # week-2-book_1/5 books
    print('Get actual week number')
    # Create a client
    client = boto3.client('s3',
                          aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
                          aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'))
    #
    # # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')
    #
    # # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=bucket, PaginationConfig={'MaxItems': 10})

    # get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    all_data = []

    filtered_iterator = page_iterator.search("Contents[?Size > `36600`][]")
    for key_data in filtered_iterator:
        all_data = [key_data]

    week_name = all_data[0]['Key']
    actual_week_number = int(week_name.split("/")[0].split("-")[1])
    print('Actual week is: {}'.format(actual_week_number))

    actual_week = actual_week_number

    return actual_week_number

def get_week_period(**context):
    today = datetime.now().date()
    week_list = []
    start_day_timedelta = 3

    first_day_week = today - timedelta(days=7)
    last_day_week = today - timedelta(days=3)
    logger.info("This week start at: {0} | End at: {1}".format(str(first_day_week), str(last_day_week)))
    interval_days = last_day_week - first_day_week
    days_total_week = int(interval_days.days) + 1

    # Create the list with the valid days in the week
    for control_days in range(days_total_week):
        dates_in_the_week = today - timedelta(days=start_day_timedelta)
        validate_week_day = cal.isbizday(dates_in_the_week)
        if validate_week_day:
            week_list.append(str(dates_in_the_week))
            start_day_timedelta = start_day_timedelta + 1
        else:
            start_day_timedelta = start_day_timedelta + 1

    # Convert week list order
    logger.info('Week list: {}'.format(week_list[::-1]))
    ti = context['task_instance']

    new_week = week_list[::-1]
    actual_week_number = get_folder_s3()

    new_week_number = int(actual_week_number) + 1

    record_data = record_data_on_postgress(new_week_number, new_week)

    ti.xcom_push(key='week_period', value=week_list[::-1])

    return week_list[::-1]



with DAG(
        'get_week_period',
        default_args=default_args,
        description='Get period of work week',
        catchup=False,
        ) as dag:
    get_work_period_week = PythonOperator(
        task_id='get_week_period',
        python_callable=get_week_period,
        provide_context=True
    )

    get_work_period_week