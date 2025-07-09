from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_postgres_backup(**kwargs):
    dag_run = kwargs.get("dag_run")
    backup_dir = (dag_run.conf.get("backup_dir") if dag_run and dag_run.conf else None) or \
                 Variable.get("backup_dir", default_var="/db_backup")

    hook = PostgresHook(postgres_conn_id='postgres_custom')
    conn = hook.get_connection('postgres_custom')

    host = conn.host
    port = conn.port or 5432
    user = conn.login
    password = conn.password
    db = conn.schema

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    os.makedirs(backup_dir, exist_ok=True)
    filepath = os.path.join(backup_dir, f"{db}_backup_{timestamp}.sql")

    cmd = [
        "pg_dump",
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", db,
        "-f", filepath
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = password

    try:
        subprocess.run(cmd, check=True, env=env)
    finally:
        env.pop("PGPASSWORD", None)

with DAG(
    dag_id='daily_postgres_backup',
    default_args=default_args,
    description='Daily PostgreSQL database backup',
    schedule='@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['backup', 'postgres'],
) as dag:

    perform_postgres_backup = PythonOperator(
        task_id='perform_postgres_backup',
        python_callable=run_postgres_backup,
    )