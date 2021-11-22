release: airflow db init
web: airflow webserver stop && airflow variables import variables.json  && airflow webserver -p $PORT --daemon && airflow scheduler