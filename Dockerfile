FROM apache/airflow:3.2.0

USER airflow

COPY requirements.txt /requirements.txt
COPY requirements-dev.txt /requirements-dev.txt

RUN pip install --no-cache-dir -r /requirements-dev.txt