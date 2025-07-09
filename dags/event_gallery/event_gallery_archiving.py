from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import os
import shutil
from pendulum import timezone
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

local_tz = timezone("Europe/Warsaw")

JSON_PATH = os.path.join(os.path.dirname(__file__), 'transfers.json')

def extract_event_gallery_paths():
    # Extract path_src and path_dst from transfers with name 'event_gallery'
    with open(JSON_PATH, 'r') as f:
        data = json.load(f)
    
    transfers = data['transfers']
    for transfer in transfers:
        if transfer['name'] == 'event_gallery':
            paths = transfer['paths']
            path_src = paths['path_src']
            path_dst = paths['path_dst']
            if not path_src or not path_dst:
                raise ValueError("Both 'path_src' and 'path_dst' must be defined")
            return path_src, path_dst
    raise ValueError("No transfer with name 'event_gallery' found")


def get_max_folder_name_from_dbo():
    hook = PostgresHook(postgres_conn_id='postgres_custom')
    sql = "SELECT MAX(file_name) FROM dbo.event_gallery_files;"
    result = hook.get_first(sql)
    return result[0] if result and result[0] else ''

def scan_and_insert(src, dst, max_folder):

    hook = PostgresHook(postgres_conn_id='postgres_custom')

    max_folder = max_folder or ''

    if not os.path.exists(src):
        raise FileNotFoundError(f"Source path does not exist: {src}")

    if not os.path.exists(dst):
        os.makedirs(dst, exist_ok=True)

    first_level_dirs = [d for d in os.listdir(src) if os.path.isdir(os.path.join(src, d))]

    for folder_name in first_level_dirs:
        if folder_name > max_folder:
            folder_path = os.path.join(src, folder_name)

            for root, dirs, files in os.walk(folder_path):
                rel_path = os.path.relpath(root, src)
                for file in files:
                    insert_sql = """
                    INSERT INTO staging.event_gallery_files (file_name, path)
                    VALUES (%s, %s);
                    """
                    hook.run(insert_sql, parameters=(file, rel_path))

            dst_folder_path = os.path.join(dst, folder_name)
            for root, dirs, files in os.walk(folder_path):
                rel_path = os.path.relpath(root, folder_path)
                target_dir = os.path.join(dst_folder_path, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                for file in files:
                    shutil.copy2(os.path.join(root, file), os.path.join(target_dir, file))

            print(f"Folder {folder_name} copied to {dst_folder_path}")

POSTGRES_CONN_ID = "postgres_custom"

with DAG(
    dag_id='event_gallery_archiving',
    start_date=datetime(2025, 1, 1, 0, 0, 0, tzinfo=local_tz),
    schedule='0 0 * * *',
    catchup=False,
    tags=['example'],
) as dag:


    extract_paths_task = PythonOperator(
        task_id="extract_event_gallery_paths",
        python_callable=extract_event_gallery_paths,
    )

    get_max_folder_name_task = PythonOperator(
        task_id="get_max_folder_name_from_dbo",
        python_callable=get_max_folder_name_from_dbo
    )

    truncate_staging_table_task = SQLExecuteQueryOperator(
        task_id='truncate_staging_table',
        conn_id=POSTGRES_CONN_ID,
        sql='TRUNCATE TABLE staging.event_gallery_files;',
    )

    scan_and_insert_task = PythonOperator(
        task_id="scan_and_insert_files",
        python_callable=scan_and_insert,
        op_kwargs={
            "src": "{{ ti.xcom_pull(task_ids='extract_event_gallery_paths')[0] }}",
            "dst": "{{ ti.xcom_pull(task_ids='extract_event_gallery_paths')[1] }}",
            "max_folder": "{{ ti.xcom_pull(task_ids='get_max_folder_name_from_dbo') }}"
        }
    )

    call_migrate_proc_task = SQLExecuteQueryOperator(
        task_id='call_migrate_procedure',
        conn_id=POSTGRES_CONN_ID,
        sql='CALL staging.migrate_event_gallery_files();',
    )

    # Define dependencies
    extract_paths_task >> truncate_staging_table_task >> get_max_folder_name_task >> scan_and_insert_task >> call_migrate_proc_task