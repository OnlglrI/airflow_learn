"""
DAG с инициализацией через `with` (Context Manager)
Логика: 2 таски отправляют данные в XCom, 3-я таска их читает.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def push_task_1(**kwargs):
    """Отправляет простое строковое значение в XCom."""
    kwargs['ti'].xcom_push(key='value_from_task_1', value='Hello from Task 1')

def push_task_2(**kwargs):
    """Отправляет словарь в XCom."""
    kwargs['ti'].xcom_push(key='value_from_task_2', value={'user': 'airflow', 'id': 123})

def pull_task(**kwargs):
    """Читает данные из XCom от двух предыдущих тасок."""
    ti = kwargs['ti']
    value1 = ti.xcom_pull(key='value_from_task_1', task_ids='task_1')
    value2 = ti.xcom_pull(key='value_from_task_2', task_ids='task_2')
    
    print(f"Получено из task_1: {value1}")
    print(f"Получено из task_2: {value2}")
    
    if not value1 or not value2:
        raise ValueError("Не удалось получить данные из XCom")

with DAG(
    dag_id="2_1_xcom_context_init",
    description="Пример DAG с 3 тасками и XCom (context manager)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tutorial", "xcom", "context"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
) as dag:
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=push_task_1,
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=push_task_2,
    )

    task_3 = PythonOperator(
        task_id='task_3_pull',
        python_callable=pull_task,
    )

    [task_1, task_2] >> task_3

