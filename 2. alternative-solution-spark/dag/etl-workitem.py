from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'workitem_dag',
    default_args=default_args,
    description='ETL pipeline for WorkItem data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['workitem'],
)

raw_etl = BashOperator(
    task_id='raw_etl_workitem',
    bash_command='python /path/to/mmd/raw-etl-workitem.py',
    dag=dag,
)

stan_etl = BashOperator(
    task_id='stan_etl_workitem',
    bash_command='python /path/to/mmd/stan-etl-workitem.py',
    dag=dag,
)

cons_etl = BashOperator(
    task_id='cons_etl_workitem',
    bash_command='python /path/to/mmd/cons-etl-workitem.py',
    dag=dag,
)

raw_etl >> stan_etl >> cons_etl