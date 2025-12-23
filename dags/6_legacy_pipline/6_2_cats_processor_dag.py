"""DAG B: cats_processor_dag

Роль: consumer

Что делает:
- Получает path_to_raw_data из dag_run.conf (ожидаем {bucket, key})
- Читает raw JSON из MinIO
- Трансформирует: id, url, width, height + load_dt
- Сохраняет результат в Postgres (создаёт таблицу сам)

Пререквизиты:
- MinIO connection id берётся из Variable MINIO_CONN_ID (по умолчанию minio)
- Postgres connection id берётся из Variable POSTGRES_CONN_ID (по умолчанию postgres_data)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")

TARGET_TABLE = "cat_images"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="6_2_cats_processor_dag",
    default_args=default_args,
    description="Consumer: read raw cats json from MinIO -> transform -> load to Postgres",
    schedule_interval=None,
    catchup=False,
    tags=["cats", "minio", "postgres", "consumer", "basic_pipeline"],
)


@task(task_id="read_raw_from_minio", dag=dag)
def read_raw_from_minio(**context) -> Dict[str, Any]:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    ref = conf.get("path_to_raw_data")

    # producer может передать dict или строку (Jinja рендерит dict как строку)
    if isinstance(ref, str):
        try:
            ref = json.loads(ref.replace("'", '"'))
        except Exception:
            # если не получилось привести к JSON — отдадим понятную ошибку
            raise ValueError(f"path_to_raw_data must be dict or json-string, got: {ref}")

    if not isinstance(ref, dict) or "bucket" not in ref or "key" not in ref:
        raise ValueError(
            "dag_run.conf.path_to_raw_data is required and must be {bucket, key}. "
            f"Got: {ref}"
        )

    bucket = ref["bucket"]
    key = ref["key"]

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    body = s3.read_key(key=key, bucket_name=bucket)
    if body is None:
        raise FileNotFoundError(f"Cannot read s3://{bucket}/{key}")

    return {"bucket": bucket, "key": key, "raw": json.loads(body)}


@task(task_id="transform", dag=dag)
def transform(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        raise ValueError(f"Unexpected raw structure: {type(raw)}")

    items = raw.get("items")
    if not isinstance(items, list):
        raise ValueError(f"raw.items must be list, got: {type(items)}")

    load_dt = datetime.now(timezone.utc)

    out: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        out.append(
            {
                "id": item.get("id"),
                "url": item.get("url"),
                "width": item.get("width"),
                "height": item.get("height"),
                "load_dt": load_dt,
            }
        )

    # базовая валидация
    out = [r for r in out if r.get("id") and r.get("url")]
    return out


@task(task_id="load_to_postgres", dag=dag)
def load_to_postgres(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            width INTEGER,
            height INTEGER,
            load_dt TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()

    # upsert по id
    cur.executemany(
        f"""
        INSERT INTO {TARGET_TABLE} (id, url, width, height, load_dt)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            url = EXCLUDED.url,
            width = EXCLUDED.width,
            height = EXCLUDED.height,
            load_dt = EXCLUDED.load_dt;
        """,
        [
            (
                r.get("id"),
                r.get("url"),
                r.get("width"),
                r.get("height"),
                r.get("load_dt"),
            )
            for r in rows
        ],
    )
    conn.commit()

    inserted = len(rows)
    cur.close()
    conn.close()
    return inserted


raw_payload = read_raw_from_minio()
transformed = transform(raw_payload)
load_to_postgres(transformed)
