release: airflow db init && airflow users create \
    --username admin \
    --firstname Lucas \
    --password lucas \
    --lastname Fonseca \
    --role Admin \
    --email lucas@lucasfonmiranda.com
web: airflow webserver -p $PORT --daemon && airflow scheduler