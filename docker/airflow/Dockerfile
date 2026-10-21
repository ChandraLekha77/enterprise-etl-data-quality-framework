ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.1-python3.12
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && curl --fail --show-error --silent --location \
        --output /tmp/packages-microsoft-prod.deb \
        https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
    && dpkg --install /tmp/packages-microsoft-prod.deb \
    && rm /tmp/packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --yes --no-install-recommends msodbcsql18 \
    && apt-get purge --yes curl \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.1" \
    --requirement /opt/airflow/requirements.txt
