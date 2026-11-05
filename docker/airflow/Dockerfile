ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update \
    && ACCEPT_EULA=Y apt-get install --yes --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root docker/airflow/requirements.txt /opt/airflow/requirements.txt
COPY --chown=airflow:root dbt/requirements.txt /opt/airflow/dbt-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    --requirement /opt/airflow/requirements.txt \
    --requirement /opt/airflow/dbt-requirements.txt
