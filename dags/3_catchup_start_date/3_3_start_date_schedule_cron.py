"""
DAG с TaskFlow API (Modern Approach)

Сделан таким же по логике и структуре, как 2_legacy_init.py (start -> task_1/task_2 -> join -> task_3 -> bash_task -> end),
но использует TaskFlow API (@dag/@task).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id="3_3_start_date_schedule_cron",
    description="Пример start_date + catchup с cron schedule (каждый час)",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 12, 10),
    catchup=False,
    tags=["tutorial", "taskflow", "start_date", "catchup", "cron"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def taskflow_dag():
    start = EmptyOperator(task_id="start")

    @task(task_id="task_1")
    def task_1():
        print("Executing Task 1 in TaskFlow DAG")
        return "Task 1 completed"

    @task(task_id="task_2")
    def task_2():
        print("Executing Task 2 in TaskFlow DAG")
        return "Task 2 completed"

    join = EmptyOperator(task_id="join")

    @task(task_id="task_3")
    def task_3(task1_result: str, task2_result: str, **context):
        execution_date = context.get("execution_date")
        ti = context.get("ti")

        print(f"Execution date: {execution_date}")
        print(f"Task instance: {ti}")
        print(f"Task 1 result: {task1_result}")
        print(f"Task 2 result: {task2_result}")

        return "Task 3 completed"

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command='echo "TaskFlow DAG is running" && date',
    )

    end = EmptyOperator(task_id="end")

    t1 = task_1()
    t2 = task_2()
    t3 = task_3(t1, t2)

    start >> [t1, t2]
    t1 >> join
    t2 >> join
    join >> t3
    t3 >> bash_task
    bash_task >> end


dag_instance = taskflow_dag()
