"""
Пример DAG для работы с MinIO
Демонстрирует создание bucket и загрузку файлов в MinIO
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")


def create_bucket(**context):
    """Создание bucket в MinIO"""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    bucket_name = 'airflow-test-bucket'

    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)
        print(f"Bucket '{bucket_name}' успешно создан")
    else:
        print(f"Bucket '{bucket_name}' уже существует")

    return bucket_name


def upload_data(**context):
    """Загрузка данных в MinIO"""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    bucket_name = context['ti'].xcom_pull(task_ids='create_bucket')

    # Пример данных
    data = {
        'timestamp': datetime.now().isoformat(),
        'message': 'Hello from Airflow!',
        'execution_date': context['ds'],
        'dag_id': context['dag'].dag_id
    }

    # Загрузка JSON данных
    s3_hook.load_string(
        string_data=json.dumps(data, indent=2),
        key=f'data/{context["ds"]}/data.json',
        bucket_name=bucket_name,
        replace=True
    )

    print(f"Данные успешно загружены в {bucket_name}/data/{context['ds']}/data.json")


def list_files(**context):
    """Просмотр файлов в MinIO"""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    bucket_name = context['ti'].xcom_pull(task_ids='create_bucket')

    keys = s3_hook.list_keys(bucket_name=bucket_name)

    print(f"Файлы в bucket '{bucket_name}':")
    if keys:
        for key in keys:
            print(f"  - {key}")
    else:
        print("  Bucket пуст")

    return keys


def read_data(**context):
    """Чтение данных из MinIO"""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    bucket_name = context['ti'].xcom_pull(task_ids='create_bucket')

    # Чтение загруженного файла
    obj = s3_hook.get_key(
        key=f'data/{context["ds"]}/data.json',
        bucket_name=bucket_name
    )

    content = obj.get()['Body'].read().decode('utf-8')
    data = json.loads(content)

    print("Прочитанные данные:")
    print(json.dumps(data, indent=2))

    return data


with DAG(
    'example_minio_workflow',
    default_args=default_args,
    description='Пример работы с MinIO в Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'minio', 's3'],
) as dag:

    create_bucket_task = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket,
    )

    upload_data_task = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data,
    )

    list_files_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
    )

    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_data,
    )

    # Определяем порядок выполнения задач
    create_bucket_task >> upload_data_task >> list_files_task >> read_data_task
