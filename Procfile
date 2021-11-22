release: airflow db init
web: airflow variables import variables.json  && airflow webserver -p $PORT --daemon && airflow scheduler