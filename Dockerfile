FROM apache/airflow:2.10.0
USER root
RUN mkdir -p /opt/airflow/db && chown -R airflow:0 /opt/airflow/db
USER airflow
COPY requirements-airflow.txt /
RUN pip install --no-cache-dir -r /requirements-airflow.txt
