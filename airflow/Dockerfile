FROM apache/airflow:2.10.2

USER airflow

# Install ML, Telegram & MLOps dependencies
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
