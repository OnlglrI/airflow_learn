"""
Комплексный пример: ETL pipeline с PostgreSQL и MinIO
Демонстрирует полный цикл: извлечение данных из PostgreSQL,
обработку и сохранение в MinIO
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import pandas as pd
from io import StringIO

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Shared config
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")
MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
SALES_BUCKET = Variable.get("SALES_BUCKET", default_var="sales-reports")
SALES_RAW_PREFIX = Variable.get("SALES_RAW_PREFIX", default_var="raw_data")
SALES_REPORTS_PREFIX = Variable.get("SALES_REPORTS_PREFIX", default_var="reports")


def create_sample_table(**context):
    """Создание примерной таблицы в PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Создаем таблицу с примерными данными
    sql = """
    DROP TABLE IF EXISTS sales_data;
    
    CREATE TABLE sales_data (
        id SERIAL PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        price DECIMAL(10, 2),
        quantity INTEGER,
        sale_date DATE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    INSERT INTO sales_data (product_name, category, price, quantity, sale_date)
    VALUES 
        ('Laptop', 'Electronics', 999.99, 5, CURRENT_DATE - INTERVAL '1 day'),
        ('Mouse', 'Electronics', 29.99, 15, CURRENT_DATE - INTERVAL '1 day'),
        ('Keyboard', 'Electronics', 79.99, 10, CURRENT_DATE - INTERVAL '1 day'),
        ('Chair', 'Furniture', 199.99, 3, CURRENT_DATE - INTERVAL '1 day'),
        ('Desk', 'Furniture', 299.99, 2, CURRENT_DATE - INTERVAL '1 day'),
        ('Monitor', 'Electronics', 249.99, 7, CURRENT_DATE),
        ('Headphones', 'Electronics', 149.99, 12, CURRENT_DATE),
        ('Lamp', 'Furniture', 49.99, 8, CURRENT_DATE);
    """

    postgres_hook.run(sql)
    print("✅ Таблица sales_data создана и заполнена")


def extract_from_postgres(**context):
    """Извлечение данных из PostgreSQL"""
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = """
    SELECT 
        product_name,
        category,
        price,
        quantity,
        price * quantity as total_amount,
        sale_date
    FROM sales_data
    WHERE sale_date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY sale_date DESC, total_amount DESC;
    """

    # Получаем данные как pandas DataFrame
    df = postgres_hook.get_pandas_df(sql)

    print(f"📊 Извлечено {len(df)} записей")
    print("\nПервые записи:")
    print(df.head())

    # Сохраняем в XCom как JSON
    return df.to_json(orient='records', date_format='iso')


def transform_data(**context):
    """Обработка данных"""
    ti = context['ti']
    data_json = ti.xcom_pull(task_ids='extract_from_postgres')

    # Преобразуем обратно в DataFrame
    df = pd.read_json(StringIO(data_json))

    # Добавляем аналитику
    summary = {
        'total_records': len(df),
        'total_revenue': float(df['total_amount'].sum()),
        'average_price': float(df['price'].mean()),
        'categories': df['category'].value_counts().to_dict(),
        'top_products': df.nlargest(3, 'total_amount')[['product_name', 'total_amount']].to_dict('records'),
        'report_date': datetime.now().isoformat()
    }

    print("📈 Статистика:")
    print(f"  Всего записей: {summary['total_records']}")
    print(f"  Общая выручка: ${summary['total_revenue']:.2f}")
    print(f"  Средняя цена: ${summary['average_price']:.2f}")

    return {
        'data': data_json,
        'summary': summary
    }


def load_to_minio(**context):
    """Загрузка данных в MinIO"""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='transform_data')

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    bucket_name = SALES_BUCKET

    # Создаем bucket если не существует
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name=bucket_name)
        print(f"✅ Bucket '{bucket_name}' создан")

    # Сохраняем сырые данные
    execution_date = context['ds']
    data_key = f'{SALES_RAW_PREFIX}/{execution_date}/sales_data.json'
    s3_hook.load_string(
        string_data=result['data'],
        key=data_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"✅ Данные сохранены: {data_key}")

    # Сохраняем отчет
    report_key = f'{SALES_REPORTS_PREFIX}/{execution_date}/summary.json'
    s3_hook.load_string(
        string_data=json.dumps(result['summary'], indent=2),
        key=report_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"✅ Отчет сохранен: {report_key}")

    return {
        'bucket': bucket_name,
        'data_key': data_key,
        'report_key': report_key
    }


def send_notification(**context):
    """Отправка уведомления о завершении"""
    ti = context['ti']
    result = ti.xcom_pull(task_ids='load_to_minio')
    summary = ti.xcom_pull(task_ids='transform_data')['summary']

    message = f"""
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    📊 ETL Pipeline выполнен успешно!
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    📅 Дата: {context['ds']}
    
    📈 Статистика:
      • Обработано записей: {summary['total_records']}
      • Общая выручка: ${summary['total_revenue']:.2f}
      • Средняя цена: ${summary['average_price']:.2f}
    
    📦 Данные сохранены в MinIO:
      • Bucket: {result['bucket']}
      • Данные: {result['data_key']}
      • Отчет: {result['report_key']}
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """

    print(message)
    return "Success"


with DAG(
    'etl_postgres_to_minio',
    default_args=default_args,
    description='ETL pipeline: PostgreSQL → Обработка → MinIO',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'postgres', 'minio', 'production'],
) as dag:

    create_table_task = PythonOperator(
        task_id='create_sample_table',
        python_callable=create_sample_table,
    )

    extract_task = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_to_minio',
        python_callable=load_to_minio,
    )

    notify_task = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )

    # Определяем порядок выполнения
    create_table_task >> extract_task >> transform_task >> load_task >> notify_task

