import os
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.models import Variable

# for dbt
DBT_PROJECT_DIR = "/opt/airflow/transform"
DBT_PROFILES_DIR = "/opt/airflow/"

with DAG(
    dag_id='ufc_analytics',
    schedule=None,
    catchup=False
):
    
    start = EmptyOperator(
        task_id='start'
    )

    run_extraction = BashOperator(
        task_id='extract_and_load_to_snowflake',
        bash_command= "python /opt/airflow/dags/extract/main.py",
        # THIS IS THE MAGIC BRIDGE:
        env={
            'SNOWFLAKE_ACCOUNT': '{{ var.value.SNOWFLAKE_ACCOUNT }}'
        }
    )

    dbt_deps = BashOperator(
        task_id = 'dbt_package_installation',
        bash_command = f"cd {DBT_PROJECT_DIR} && if [ ! -d 'dbt_packages' ] || [ packages.yml -nt dbt_packages ]; then dbt deps --profiles-dir {DBT_PROFILES_DIR}; else echo 'Packages up to date. Skipping download.'; fi"
    )

    dbt_stage_layer = BashOperator(
        task_id = 'stage_layer_models',
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select stage --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_intermediate_layer = BashOperator(
            task_id = 'intermediate_layer_models',
            bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select intermediate --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_mart_layer = BashOperator(
        task_id = 'marts_layer_models',
        bash_command = f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}"
    )

    end = EmptyOperator(
        task_id = 'end'
    )

    

 
    start >> run_extraction >> dbt_deps >> dbt_stage_layer >> dbt_intermediate_layer >> dbt_mart_layer >> end