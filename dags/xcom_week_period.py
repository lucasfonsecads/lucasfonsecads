from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fonseca',
    'start_date': datetime(2020, 5, 20),
    'depends_on_past': False,
    'provide_context': True
}

with DAG(
        'show_xcom_data',
        default_args=default_args,
        description='Get period of work week',
        catchup=False,
) as dag:
    create_table_week = PostgresOperator(
        task_id="create_table_week",
        postgres_conn_id="airflow",
        sql=""" 
        CREATE TABLE IF NOT EXISTS week_period ( 
        week_id SERIAL PRIMARY KEY,
        week_name VARCHAR NOT NULL,
        week_number INT NOT NULL,
        week_period VARCHAR NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
        """,
    )
    get_databas_records = PostgresOperator(
        task_id='get_records',
        postgres_conn_id="airflow",
        sql="SELECT week_id FROM week_period",

    )

    create_table_week >> get_databas_records