from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'chembl_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'chembl_full_pipeline',
    default_args=default_args,
    description='End-to-end ChEMBL Data Pipeline for Regression',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['chembl', 'regression', 'etl']
) as dag:

    # Fetch Activity Data
    t1_fetch = BashOperator(
        task_id='fetch_data',
        bash_command='python /opt/airflow/dags/fetch_chembl_data.py'
    )

    # Enrich with Metadata
    t2_enrich = BashOperator(
        task_id='enrich_data',
        bash_command='python /opt/airflow/dags/enrich_chembl_data.py'
    )

    # Merge and Clean
    t3_process = BashOperator(
        task_id='process_data',
        bash_command='python /opt/airflow/dags/process_data.py'
    )

    # Pipeline Flow
    t1_fetch >> t2_enrich >> t3_process