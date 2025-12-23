"""DAG B (Dataset): cats_processor_dataset_dag

Consumer на Airflow Datasets.

Запускается при появлении события Dataset (из producer DAG).

Что делает:
- Берёт bucket/key из Dataset-trigger (самый свежий DatasetEvent)
- Читает raw JSON из MinIO
- Трансформирует: id, url, width, height + load_dt
- Сохраняет результат в Postgres
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID", default_var="postgres_data")
RAW_BUCKET = Variable.get("MINIO_BUCKET_CATS", default_var="cats")
RAW_PREFIX = Variable.get("MINIO_RAW_PREFIX", default_var="raw")

CATS_RAW_DATASET = Dataset(f"minio://{RAW_BUCKET}/{RAW_PREFIX}")
TARGET_TABLE = "cat_images"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="7_2_cats_processor_dataset_dag",
    default_args=default_args,
    description="Consumer (Dataset): read raw from MinIO on dataset event -> transform -> load to Postgres",
    schedule=[CATS_RAW_DATASET],
    catchup=False,
    tags=["cats", "minio", "postgres", "dataset", "consumer", "basic_pipeline"],
)


def _extract_bucket_key_from_context(context) -> Tuple[str, str]:
    """Пытаемся достать bucket/key из dataset-trigger.

    Примечание:
    В некоторых конфигурациях Airflow dataset_event.extra не заполняется, поэтому
    triggering_dataset_events не содержит bucket/key. Тогда делаем fallback:
    берём последний объект в MinIO по RAW_PREFIX.

    Поддерживаем 3 варианта:
    1) dag_run.conf.path_to_raw_data (ручной запуск)
    2) triggering_dataset_events[-1].extra (если Airflow передаёт extra)
    3) fallback: поиск последнего ключа в MinIO bucket/prefix
    """

    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    ref = conf.get("path_to_raw_data")
    if isinstance(ref, dict) and "bucket" in ref and "key" in ref:
        return ref["bucket"], ref["key"]

    triggering_events = context.get("triggering_dataset_events")
    if isinstance(triggering_events, list) and triggering_events:
        event = triggering_events[-1]
        extra = getattr(event, "extra", None)
        if isinstance(extra, dict) and "bucket" in extra and "key" in extra:
            return extra["bucket"], extra["key"]
        if isinstance(extra, str):
            try:
                d = json.loads(extra.replace("'", '"'))
                if "bucket" in d and "key" in d:
                    return d["bucket"], d["key"]
            except Exception:
                pass

    # Fallback: берём последний ключ из MinIO по RAW_PREFIX
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    keys = s3.list_keys(bucket_name=RAW_BUCKET, prefix=RAW_PREFIX) or []
    if not keys:
        raise ValueError(
            "Не удалось определить bucket/key из dataset-trigger и в MinIO нет объектов по prefix. "
            "Запусти producer DAG заново или запусти этот DAG вручную с conf.path_to_raw_data={bucket,key}."
        )

    # берём лексикографически последний (у нас timestamp в имени, поэтому подходит)
    key = sorted(keys)[-1]
    return RAW_BUCKET, key


@task(task_id="read_raw_from_minio", dag=dag)
def read_raw_from_minio(**context) -> Dict[str, Any]:
    bucket, key = _extract_bucket_key_from_context(context)

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
        cat_id = item.get("id")
        url = item.get("url")
        if not cat_id or not url:
            continue

        out.append(
            {
                "id": cat_id,
                "url": url,
                "width": item.get("width"),
                "height": item.get("height"),
                "load_dt": load_dt,
            }
        )

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

    cur.close()
    conn.close()
    return len(rows)


raw_payload = read_raw_from_minio()
transformed = transform(raw_payload)
load_to_postgres(transformed)
