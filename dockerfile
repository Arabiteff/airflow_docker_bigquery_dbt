FROM apache/airflow:3.0.0

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-bigquery==1.5.3 setuptools  && deactivate

# Install astronomer-cosmos in the main Airflow environment
RUN pip install astronomer-cosmos==1.10.1
