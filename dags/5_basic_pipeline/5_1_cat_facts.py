"""
DAG для получения фактов о кошках из API и сохранения в PostgreSQL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '5_1_cat_facts',
    default_args=default_args,
    description='Получение фактов о кошках и сохранение в PostgreSQL',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['api', 'postgres', 'cat_facts', 'basic_pipeline'],
)


# Задача 1: Проверка доступности API endpoint
check_api_availability = HttpSensor(
    task_id='check_cat_facts_api',
    http_conn_id='http_default',
    endpoint='/fact',
    request_params={},
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=120,
    mode='poke',
    dag=dag,
)


# Задача 2: Получение данных из API
@task(task_id='fetch_cat_fact', dag=dag)
def fetch_cat_fact():
    """
    Получает факт о кошках из API
    """
    hook = HttpHook(method='GET', http_conn_id='http_default')
    response = hook.run('/fact')
    response.raise_for_status()
    data = response.json()

    return {
        'fact': data.get('fact'),
        'length': data.get('length'),
        'fetched_at': datetime.now().isoformat()
    }


# Задача 3: Создание таблицы и загрузка данных в PostgreSQL
@task(task_id='load_to_postgres', dag=dag)
def load_to_postgres(cat_fact_data: dict):
    """
    Создает таблицу (если не существует) и загружает данные в PostgreSQL
    """
    POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")

    # Подключение к PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Создание таблицы, если не существует
    create_table_query = """
    CREATE TABLE IF NOT EXISTS cat_facts (
        id SERIAL PRIMARY KEY,
        fact TEXT NOT NULL,
        length INTEGER,
        fetched_at TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Вставка данных
    insert_query = """
    INSERT INTO cat_facts (fact, length, fetched_at)
    VALUES (%s, %s, %s)
    RETURNING id;
    """
    cursor.execute(
        insert_query,
        (
            cat_fact_data['fact'],
            cat_fact_data['length'],
            cat_fact_data['fetched_at']
        )
    )
    inserted_id = cursor.fetchone()[0]
    conn.commit()

    cursor.close()
    conn.close()

    print(f"Успешно загружен факт о кошках с ID: {inserted_id}")
    print(f"Факт: {cat_fact_data['fact']}")

    return inserted_id


# Определение зависимостей между задачами
cat_fact = fetch_cat_fact()
check_api_availability >> cat_fact >> load_to_postgres(cat_fact)
