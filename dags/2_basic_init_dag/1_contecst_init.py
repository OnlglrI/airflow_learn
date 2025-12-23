"""
DAG с классической инициализацией (Classic Approach)

Сделан таким же по структуре, как 2_legacy_init.py (те же таски/bits),
но с отдельным default_args и явной привязкой задач к DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def task1_function():
    """Первая задача"""
    print("Executing Task 1 in Classic DAG")
    return "Task 1 completed"


def task2_function():
    """Вторая задача"""
    print("Executing Task 2 in Classic DAG")
    return "Task 2 completed"


def task3_function(**kwargs):
    """Третья задача с контекстом"""
    ti = kwargs["ti"]
    execution_date = kwargs["execution_date"]

    print(f"Execution date: {execution_date}")
    print(f"Task instance: {ti}")

    # Получение результатов предыдущих задач через XCom
    task1_result = ti.xcom_pull(task_ids="task_1")
    task2_result = ti.xcom_pull(task_ids="task_2")

    print(f"Task 1 result: {task1_result}")
    print(f"Task 2 result: {task2_result}")

    return "Task 3 completed"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    dag_id="01_classic_init",
    default_args=default_args,
    description="Пример классической инициализации DAG (как legacy, но через default_args)",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["tutorial", "classic", "init"],
)


start_task = DummyOperator(
    task_id="start",
    dag=dag,
)

task_1 = PythonOperator(
    task_id="task_1",
    python_callable=task1_function,
    dag=dag,
)

task_2 = PythonOperator(
    task_id="task_2",
    python_callable=task2_function,
    dag=dag,
)

join_task = DummyOperator(
    task_id="join",
    dag=dag,
)

task_3 = PythonOperator(
    task_id="task_3",
    python_callable=task3_function,
    provide_context=True,
    dag=dag,
)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command='echo "Classic DAG is running" && date',
    dag=dag,
)

end_task = DummyOperator(
    task_id="end",
    dag=dag,
)


start_task >> [task_1, task_2]
task_1 >> join_task
task_2 >> join_task
join_task >> task_3
task_3 >> bash_task
bash_task >> end_task
