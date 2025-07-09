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

def restore_latest_backup(**kwargs):
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    backup_dir = conf.get("backup_dir") or Variable.get("backup_dir", default_var="/db_backup")

    custom_sql_file = conf.get("sql_file")

    if custom_sql_file:
        latest_sql = custom_sql_file
        if not os.path.isfile(latest_sql):
            raise FileNotFoundError(f"Given file {latest_sql} does not exist")
    else:
        if not os.path.isdir(backup_dir):
            raise FileNotFoundError(f"Folder backup_dir: {backup_dir} does not exist")

        sql_files = [f for f in os.listdir(backup_dir) if f.endswith(".sql")]
        if not sql_files:
            raise FileNotFoundError(f"No .sql files in {backup_dir}")

        sql_files.sort(key=lambda f: os.path.getmtime(os.path.join(backup_dir, f)), reverse=True)
        latest_sql = os.path.join(backup_dir, sql_files[0])

    hook = PostgresHook(postgres_conn_id='postgres_custom_dev')
    conn = hook.get_connection('postgres_custom_dev')

    host = conn.host
    port = conn.port or 5432
    user = conn.login
    password = conn.password
    db = conn.schema

    cmd = [
        "psql",
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", db,
        "-f", latest_sql,
    ]

    env = os.environ.copy()
    env["PGPASSWORD"] = password

    print(f"Restoring backup from file: {latest_sql}")
    print("Running command:", " ".join(cmd))

    try:
        subprocess.run(cmd, check=True, env=env)
    finally:
        env.pop("PGPASSWORD", None)

with DAG(
    dag_id='restore_postgres_backup',
    default_args=default_args,
    description='Restore PostgreSQL from the latest backup',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['restore', 'postgres'],
) as dag:

    restore_backup = PythonOperator(
        task_id='restore_latest_sql_backup',
        python_callable=restore_latest_backup
    )