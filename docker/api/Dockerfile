ARG PYTHON_BASE_IMAGE=python:3.12.12-slim-bookworm
FROM ${PYTHON_BASE_IMAGE}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN groupadd --system api \
    && useradd --system --gid api --home-dir /app api

WORKDIR /app

COPY api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --requirement /app/requirements.txt

COPY --chown=api:api src /app/src
COPY --chown=api:api api /app/api
COPY --chown=api:api data/raw/json /app/data/raw/json

USER api

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
