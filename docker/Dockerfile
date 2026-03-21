FROM apache/airflow:2.10.5-python3.9

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow/project

COPY requirements.txt requirements-dev.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . .

ENV PYTHONPATH=/opt/airflow/project/src

CMD ["python", "-m", "bees_breweries"]
