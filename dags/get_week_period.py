from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from bizdays import Calendar
import logging
import json

cal = Calendar(weekdays=['Sunday', 'Saturday'])

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fonseca',
    'start_date': datetime(2020, 5, 20),
    'depends_on_past': False,
    'provide_context': True
}


def get_week_period(**kwargs):
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
    ti = kwargs['ti']

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