FROM apache/airflow:slim-2.5.0rc2-python3.10

USER airflow

COPY plugins plugins

# Install additional packages
RUN pip install -r plugins/requirements.txt

