"""DAG A: cats_api_producer_dag

Роль: producer

Что делает:
- Забирает N случайных фото котов из TheCatAPI
- Сохраняет raw JSON в MinIO
- Триггерит DAG B, передавая путь (bucket/key) в dag_run.conf

Источник:
GET https://api.thecatapi.com/v1/images/search

Пререквизиты:
- MinIO connection: conn id "minio" (уже задан через AIRFLOW_CONN_MINIO в docker-compose)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# TheCatAPI: connection должен иметь host=https://api.thecatapi.com
THECATAPI_CONN_ID = Variable.get("THECATAPI_CONN_ID", default_var="thecatapi_default")
CAT_API_ENDPOINT = "/v1/images/search"

MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
RAW_BUCKET = Variable.get("MINIO_BUCKET_CATS", default_var="cats")
RAW_PREFIX = Variable.get("MINIO_RAW_PREFIX", default_var="raw")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="6_1_cats_api_producer_dag",
    default_args=default_args,
    description="Producer: fetch N cat images metadata -> save raw JSON to MinIO -> trigger consumer",
    schedule_interval=None,
    catchup=False,
    tags=["cats", "api", "minio", "producer", "basic_pipeline"],
)


check_cat_api = HttpSensor(
    task_id="check_cat_api",
    http_conn_id=THECATAPI_CONN_ID,
    endpoint=CAT_API_ENDPOINT,
    request_params={"limit": 1},
    response_check=lambda r: r.status_code == 200,
    poke_interval=20,
    timeout=120,
    mode="poke",
    dag=dag,
)


@task(task_id="fetch_cats", dag=dag)
def fetch_cats(n: int = 10) -> Dict[str, Any]:
    """Забирает N записей из API (по одной за запрос)."""

    hook = HttpHook(method="GET", http_conn_id=THECATAPI_CONN_ID)

    items: List[Dict[str, Any]] = []
    for _ in range(int(n)):
        r = hook.run(CAT_API_ENDPOINT)
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list) or not payload:
            raise ValueError(f"Unexpected payload: {payload}")
        items.append(payload[0])

    return {
        "n": int(n),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }


@task(task_id="save_raw_to_minio", dag=dag)
def save_raw_to_minio(raw: Dict[str, Any]) -> Dict[str, str]:
    """Сохраняет raw JSON в MinIO и возвращает идентификатор файла (bucket/key)."""

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not s3.check_for_bucket(RAW_BUCKET):
        s3.create_bucket(bucket_name=RAW_BUCKET)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{RAW_PREFIX}/cats_raw_{ts}.json"

    body = json.dumps(raw, ensure_ascii=False).encode("utf-8")
    s3.load_bytes(
        bytes_data=body,
        key=key,
        bucket_name=RAW_BUCKET,
        replace=True,
    )

    return {"bucket": RAW_BUCKET, "key": key}


raw = fetch_cats()
raw_ref = save_raw_to_minio(raw)

trigger_consumer = TriggerDagRunOperator(
    task_id="trigger_cats_processor_dag",
    trigger_dag_id="6_2_cats_processor_dag",
    conf={"path_to_raw_data": "{{ ti.xcom_pull(task_ids='save_raw_to_minio') }}"},
    wait_for_completion=False,
    dag=dag,
)

check_cat_api >> raw >> raw_ref >> trigger_consumer
