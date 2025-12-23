"""
Пример DAG для демонстрации параллельного выполнения задач с Celery Executor
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import time
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def process_data(task_id, **context):
    """Симуляция обработки данных"""
    duration = random.randint(5, 15)
    print(f"[{task_id}] Начало обработки...")
    print(f"[{task_id}] Буду работать {duration} секунд")

    for i in range(duration):
        time.sleep(1)
        if i % 3 == 0:
            print(f"[{task_id}] Прогресс: {i}/{duration} секунд")

    print(f"[{task_id}] Обработка завершена!")
    return f"Task {task_id} completed in {duration} seconds"


def aggregate_results(**context):
    """Агрегация результатов из всех задач"""
    ti = context['ti']

    results = []
    for i in range(1, 6):
        result = ti.xcom_pull(task_ids=f'process_task_{i}')
        results.append(result)

    print("Все результаты:")
    for result in results:
        print(f"  - {result}")

    return results


with DAG(
    'example_celery_parallel',
    default_args=default_args,
    description='Демонстрация параллельного выполнения задач с Celery',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'celery', 'parallel'],
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Начало параллельной обработки"',
    )

    # Создаем несколько параллельных задач
    parallel_tasks = []
    for i in range(1, 6):
        task = PythonOperator(
            task_id=f'process_task_{i}',
            python_callable=process_data,
            op_kwargs={'task_id': f'task_{i}'},
        )
        parallel_tasks.append(task)

    aggregate = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Все задачи выполнены!"',
    )

    # Структура DAG:
    # start -> [task_1, task_2, task_3, task_4, task_5] -> aggregate -> end
    start >> parallel_tasks >> aggregate >> end

