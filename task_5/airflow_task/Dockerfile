FROM apache/airflow:2.10.2
USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential
USER airflow
RUN pip install --no-cache-dir pandas pymongo apache-airflow-providers-mongo