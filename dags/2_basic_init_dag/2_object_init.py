"""
DAG с legacy инициализацией (Legacy Approach)
Использует старый стиль с явным созданием объекта DAG без контекстного менеджера
Этот подход использовался до Airflow 2.0
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


def task1_function():
    """Первая задача"""
    print("Executing Task 1 in Legacy DAG")
    return "Task 1 completed"


def task2_function():
    """Вторая задача"""
    print("Executing Task 2 in Legacy DAG")
    return "Task 2 completed"


def task3_function(**kwargs):
    """Третья задача с контекстом"""
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']

    print(f"Execution date: {execution_date}")
    print(f"Task instance: {ti}")

    # Получение результатов предыдущих задач через XCom
    task1_result = ti.xcom_pull(task_ids='task_1')
    task2_result = ti.xcom_pull(task_ids='task_2')

    print(f"Task 1 result: {task1_result}")
    print(f"Task 2 result: {task2_result}")

    return "Task 3 completed"


# Создание DAG объекта (legacy стиль)
dag = DAG(
    dag_id='02_legacy_init',
    description='Пример legacy инициализации DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'legacy', 'init'],
)

# Создание задач с явной привязкой к DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task1_function,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task2_function,
    dag=dag,
)

join_task = DummyOperator(
    task_id='join',
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task3_function,
    provide_context=True,
    dag=dag,
)

bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Legacy DAG is running" && date',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Определение зависимостей (legacy стиль с битовыми операторами)
start_task >> [task_1, task_2]
task_1 >> join_task
task_2 >> join_task
join_task >> task_3
task_3 >> bash_task
bash_task >> end_task

