import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from job_agent.config import RUN_PASS_2
from job_agent.fetch import main as fetch_jobs

DBT = "cd /opt/airflow/dbt_project && /home/airflow/dbt_venv/bin/dbt"

with DAG(
    dag_id="daily_job_agent",
    # Tuesday to Saturday: skips the Sunday and Monday mornings, which only cover weekend postings
    schedule="30 7 * * 2-6",
    # Airflow runs at the end of each interval: the first run is Friday 2026-10-30 07:30.
    # Starting at Thursday 07:30 (not earlier) stops an extra run firing as soon as the DAG is unpaused.
    start_date=pendulum.datetime(2026, 10, 29, 7, 30, tz="Europe/Amsterdam"),
    catchup=False,
    tags=["job_agent"],
) as dag:
    fetch = PythonOperator(task_id="fetch_jobs", python_callable=fetch_jobs)

    dbt_prefilter = BashOperator(
        task_id="dbt_run_prefilter",
        bash_command=f"{DBT} run --select stg_jobs jobs_prefiltered",
    )

    # pass 1 runs in its own virtualenv (the Anthropic SDK clashes with Airflow's pins)
    extract = BashOperator(
        task_id="extract_job_facts",
        bash_command="cd /opt/airflow/dags && /home/airflow/llm_venv/bin/python -m job_agent.extract",
    )

    dbt_eligibility = BashOperator(
        task_id="dbt_run_eligibility",
        bash_command=f"{DBT} run --select jobs_eligibility keyword_stats",
    )

    fetch >> dbt_prefilter >> extract >> dbt_eligibility

    if RUN_PASS_2:
        score = BashOperator(
            task_id="score_jobs",
            bash_command="cd /opt/airflow/dags && /home/airflow/llm_venv/bin/python -m job_agent.score",
        )

        dbt_ranking = BashOperator(
            task_id="dbt_run_ranking",
            bash_command=f"{DBT} run --select job_ranking keyword_stats",
        )

        dbt_eligibility >> score >> dbt_ranking
