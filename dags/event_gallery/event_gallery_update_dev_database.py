from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='update_dev_database',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['trigger'],
) as dag:

    trigger_backup_dag = TriggerDagRunOperator(
        task_id='trigger_backup_dag',
        trigger_dag_id='daily_postgres_backup',
    )

    trigger_restore_dag = TriggerDagRunOperator(
        task_id='trigger_restore_dag',
        trigger_dag_id='restore_postgres_backup',
    )

    trigger_backup_dag >> trigger_restore_dag