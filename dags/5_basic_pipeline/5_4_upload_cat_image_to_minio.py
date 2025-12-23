"""DAG 3_4_upload_cat_image_to_minio

Ждёт, пока в /opt/airflow/data/img появится хотя бы один файл изображения,
загружает его в MinIO и удаляет локально.

Важно:
- Для FileSensor нужен fs connection. Проще всего использовать локальный путь.
  В Airflow UI создайте connection:
    Conn Id: fs_default
    Conn Type: File (FileSystem)
    Extra: {"path": "/"}

- Для MinIO используется connection "minio" (он уже задан через AIRFLOW_CONN_MINIO в docker-compose).

Папка для обмена:
/opt/airflow/data/img
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


DATA_DIR = Path(Variable.get("DATA_IMG_DIR", default_var="/opt/airflow/data/img"))
BUCKET_NAME = Variable.get("MINIO_BUCKET_CATS", default_var="cats")
S3_KEY_PREFIX = Variable.get("MINIO_IMAGES_PREFIX", default_var="images")
MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="5_4_upload_cat_image_to_minio",
    default_args=default_args,
    description="Wait file in /opt/airflow/data/img -> upload to MinIO -> delete local",
    schedule_interval=None,
    catchup=False,
    tags=["minio", "s3", "files", "sensor", "basic_pipeline"],
)


# Ждем, что папка смонтирована и существует
wait_for_image_dir = FileSensor(
    task_id="wait_for_image_dir",
    fs_conn_id="fs_default",
    filepath=str(DATA_DIR),
    poke_interval=15,
    timeout=60 * 30,
    mode="poke",
    dag=dag,
)
# reschedule

@task(task_id="wait_for_any_image", dag=dag)
def wait_for_any_image() -> str:
    """Ждёт появления хотя бы одного файла изображения и возвращает путь к нему."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    timeout_s = 20 * 60
    step_s = 5
    started = time.time()

    while time.time() - started < timeout_s:
        candidates = []
        for pattern in ("*.jpg", "*.jpeg", "*.png", "*.gif"):
            candidates.extend(DATA_DIR.glob(pattern))

        if candidates:
            file_path = str(sorted(candidates)[0])
            return file_path

        time.sleep(step_s)

    raise TimeoutError(f"No images appeared in {DATA_DIR} in {timeout_s} seconds")


@task(task_id="upload_and_delete", dag=dag)
def upload_and_delete(file_path: str) -> str:
    """Грузит файл в MinIO и удаляет локально."""

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File does not exist: {file_path}")

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

    if not s3.check_for_bucket(BUCKET_NAME):
        s3.create_bucket(bucket_name=BUCKET_NAME)

    key = f"{S3_KEY_PREFIX}/{p.name}"

    s3.load_file(
        filename=str(p),
        key=key,
        bucket_name=BUCKET_NAME,
        replace=True,
    )

    # 1) удаляем сам файл
    os.remove(p)

    # 2) удаляем папку целиком после успешной загрузки
    # Важно: это удалит ВСЕ файлы внутри DATA_DIR.
    shutil.rmtree(DATA_DIR, ignore_errors=True)

    return f"s3://{BUCKET_NAME}/{key}"


# Wiring (dependencies)
file_path = wait_for_any_image()
wait_for_image_dir >> file_path
upload_and_delete(file_path)
