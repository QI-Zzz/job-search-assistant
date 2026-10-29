FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && apt-get install -y git

USER airflow

# pipeline packages, pinned against Airflow's constraints so Airflow itself is not upgraded
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.8.1" -r /requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt

# dbt in its own virtualenv so its dependencies cannot clash with Airflow's
# (the base image sets PIP_USER=true, which pip rejects inside a virtualenv)
RUN python -m venv /home/airflow/dbt_venv \
    && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
       "dbt-core>=1.10,<1.13" dbt-postgres

# LLM steps in their own virtualenv for the same reason (Airflow pins typing_extensions too low)
COPY requirements-llm.txt /requirements-llm.txt
RUN python -m venv /home/airflow/llm_venv \
    && PIP_USER=false /home/airflow/llm_venv/bin/pip install --no-cache-dir -r /requirements-llm.txt
