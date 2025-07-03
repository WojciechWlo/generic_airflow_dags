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

def read_json():
    with open(JSON_PATH, 'r') as f:
        data = json.load(f)
    return data

def extract_event_gallery_paths(ti):
    # Extract path_src and path_dst from transfers with name 'event_gallery'
    data = ti.xcom_pull(task_ids='read_json')
    transfers = data.get('transfers', [])
    for transfer in transfers:
        if transfer.get('name') == 'event_gallery':
            paths = transfer.get('paths', {})
            path_src = paths.get('path_src')
            path_dst = paths.get('path_dst')
            if not path_src or not path_dst:
                raise ValueError("Both 'path_src' and 'path_dst' must be defined in paths for event_gallery")
            return (path_src, path_dst)
    raise ValueError("No transfer with name 'event_gallery' found")


def get_max_folder_name_from_dbo(ti):
    # Connect to Postgres and get max folder name from dbo.event_gallery_files
    hook = PostgresHook(postgres_conn_id='postgres_custom')
    sql = "SELECT MAX(file_name) FROM dbo.event_gallery_files;"
    result = hook.get_first(sql)
    max_folder = result[0] if result and result[0] else ''
    ti.xcom_push(key='max_folder_name', value=max_folder)

def scan_and_insert(ti):
    import shutil
    import os

    # Get paths from XCom
    src, dst = ti.xcom_pull(task_ids='extract_event_gallery_paths')
    max_folder = ti.xcom_pull(task_ids='get_max_folder_name_from_dbo', key='max_folder_name')
    hook = PostgresHook(postgres_conn_id='postgres_custom')

    # Convert max_folder to string, empty means no folders yet
    max_folder = max_folder or ''

    if not os.path.exists(src):
        raise FileNotFoundError(f"Source path does not exist: {src}")

    if not os.path.exists(dst):
        os.makedirs(dst, exist_ok=True)
        os.chmod(dst, 0o777)

    # Walk through first-level directories in src
    first_level_dirs = [d for d in os.listdir(src) if os.path.isdir(os.path.join(src, d))]

    for folder_name in first_level_dirs:
        # Compare folder names lexicographically
        if folder_name > max_folder:
            folder_path = os.path.join(src, folder_name)

            # Insert all files from this folder and subfolders into staging.event_gallery_files
            for root, dirs, files in os.walk(folder_path):
                rel_path = os.path.relpath(root, src)
                for file in files:
                    file_name = file
                    insert_sql = """
                    INSERT INTO staging.event_gallery_files (file_name, path)
                    VALUES (%s, %s);
                    """
                    hook.run(insert_sql, parameters=(file_name, rel_path))

            # Copy entire folder to dst preserving structure (safe method)
            dst_folder_path = os.path.join(dst, folder_name)

            for root, dirs, files in os.walk(folder_path):
                rel_path = os.path.relpath(root, folder_path)
                target_dir = os.path.join(dst_folder_path, rel_path)
                os.makedirs(target_dir, exist_ok=True)
                os.chmod(target_dir, 0o777)
                for file in files:
                    src_file = os.path.join(root, file)
                    dst_file = os.path.join(target_dir, file)
                    shutil.copy2(src_file, dst_file)

            print(f"Folder {folder_name} copied to {dst_folder_path}")

with DAG(
    dag_id='event_gallery_archiving',
    start_date=datetime(2025, 1, 1, 0, 0, 0, tzinfo=local_tz),
    schedule='0 */6 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    read_json_task = PythonOperator(
        task_id='read_json',
        python_callable=read_json,
    )

    extract_paths_task = PythonOperator(
        task_id='extract_event_gallery_paths',
        python_callable=extract_event_gallery_paths,
    )

    truncate_staging_table = SQLExecuteQueryOperator(
        task_id='truncate_staging_table',
        conn_id='postgres_custom',
        sql='TRUNCATE TABLE staging.event_gallery_files;',
    )

    get_max_folder_task = PythonOperator(
        task_id='get_max_folder_name_from_dbo',
        python_callable=get_max_folder_name_from_dbo,
    )

    scan_and_insert_task = PythonOperator(
        task_id='scan_and_insert_files',
        python_callable=scan_and_insert,
    )

    call_migrate_proc_task = SQLExecuteQueryOperator(
        task_id='call_migrate_procedure',
        conn_id='postgres_custom',
        sql='CALL staging.migrate_event_gallery_files();',
    )

    # Define dependencies
    read_json_task >> extract_paths_task >> truncate_staging_table
    truncate_staging_table >> get_max_folder_task >> scan_and_insert_task >> call_migrate_proc_task