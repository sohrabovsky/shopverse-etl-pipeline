FROM apache/airflow:3.0.6

RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    pymongo \
    psycopg2-binary

