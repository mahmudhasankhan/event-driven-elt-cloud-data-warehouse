from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor

# from include.tasks import _extract_and_load
from pathlib import Path

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping

# FILE_PATH = '/usr/local/airflow/include/dataset/'
# SERVICE_ACCOUNT = '/usr/local/airflow/include/gcp/service_account.json'
DBT_PROJECT_PATH = Path('/usr/local/airflow/dags/dbt/sales_dw_pipeline') 

@dag(
    start_date=datetime(2025,11,11),
    schedule=None,
    catchup=False,
    default_args={
         'retries': 2, # will retry this dag upto 2 times before failing it. 
         'retry_delay': timedelta(seconds=2) # how much to wait between retries
        #  'retry_exponential_backoff': True, # after each time a task fails, it will wait exponentially, each time it will double
        #  'max_try_delay': timedelta(hours=1) # the maximum time the task will wait until failure
         
     },
    tags=['grocer_sales_elt']
)
def transform():

    start = EmptyOperator(task_id="start")
    
    # Wait for new Excel file (checks for any .xlsx or .xls file)
    # wait_for_file = FileSensor(
    #     task_id="wait_for_excel_file",
    #     filepath="/usr/local/airflow/include/dataset/*.xlsx",  # Pattern matching
    #     fs_conn_id="fs_default",
    #     poke_interval=18000,  # Check every 5 hours
    #     timeout=86400,  # Wait up to 24 hours
    #     mode="poke",  # or "reschedule" to free up worker slots
    # )

    # extract_and_load = PythonOperator(
    #     task_id='extract_and_load',
    #     python_callable=_extract_and_load,
    #     op_kwargs={'excel_dir': FILE_PATH, 'service_account': SERVICE_ACCOUNT}
    # )

    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH
    )

    profile_config = ProfileConfig(
        profile_name="sales_dw_pipeline",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
            conn_id="gcp_bigquery",
            profile_args={
                "project": "sales-datawarehouse",
                "dataset": "staging",
                "location": "asia-south2",
                "threads": 4,
                "job_execution_timeout_seconds": 7200,
                "job_retries": 1,
            },
        )
    )

    transform = DbtTaskGroup(
        group_id="transform",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        operator_args={
            "install_deps": True,  # Automatically runs dbt deps
        },

    )
    end = EmptyOperator(task_id="end")
    
    start >> transform >> end

transform()