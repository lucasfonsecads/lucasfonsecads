release: airflow db init
web: airflow users create \
    --username admin \
    --firstname Lucas \
    --password lucas \
    --lastname Fonseca \
    --role Admin \
    --email lucas@lucasfonmiranda.com && airflow webserver -p $PORT --daemon && airflow scheduler