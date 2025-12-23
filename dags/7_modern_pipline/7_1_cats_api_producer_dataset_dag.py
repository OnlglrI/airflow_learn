"""DAG A (Dataset): cats_api_producer_dataset_dag

То же самое, что cats_api_producer_dag, но вместо TriggerDagRun использует Airflow Datasets.

Producer:
- Забирает N случайных фото котов (metadata) из TheCatAPI
- Сохраняет raw JSON в MinIO
- Публикует Dataset событие с extra (bucket/key)

Consumer DAG подписывается на этот Dataset.

Источник:
GET https://api.thecatapi.com/v1/images/search
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook


CAT_API_ENDPOINT = "/v1/images/search"
THECATAPI_CONN_ID = Variable.get("THECATAPI_CONN_ID", default_var="thecatapi_default")
MINIO_CONN_ID = Variable.get("MINIO_CONN_ID", default_var="minio")
RAW_BUCKET = Variable.get("MINIO_BUCKET_CATS", default_var="cats")
RAW_PREFIX = Variable.get("MINIO_RAW_PREFIX", default_var="raw")

CATS_RAW_DATASET = Dataset(f"minio://{RAW_BUCKET}/{RAW_PREFIX}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="7_1_cats_api_producer_dataset_dag",
    default_args=default_args,
    description="Producer (Dataset): fetch N cats -> save raw to MinIO -> publish Dataset event",
    schedule_interval=None,
    catchup=False,
    tags=["cats", "api", "minio", "dataset", "producer", "basic_pipeline"],
)


check_cat_api = HttpSensor(
    task_id="check_cat_api",
    http_conn_id=THECATAPI_CONN_ID,
    endpoint=CAT_API_ENDPOINT,
    request_params={"limit": 1},
    response_check=lambda r: r.status_code == 200,
    poke_interval=20,
    timeout=120,
    mode="reschedule",
    dag=dag,
)


@task(task_id="fetch_cats", dag=dag)
def fetch_cats(n: int = 10) -> Dict[str, Any]:
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


@task(task_id="save_raw_to_minio", dag=dag, outlets=[CATS_RAW_DATASET])
def save_raw_to_minio(raw: Dict[str, Any]) -> Dict[str, str]:
    """Пишет raw JSON в MinIO.

    Важно: outlets публикует Dataset event. В Airflow UI событие будет видно,
    а consumer DAG сможет стартовать по dataset-schedule.

    Возвращаем bucket/key — это попадёт в XCom, и также будет доступно по DatasetEvent.extra.
    """

    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not s3.check_for_bucket(RAW_BUCKET):
        s3.create_bucket(bucket_name=RAW_BUCKET)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{RAW_PREFIX}/cats_raw_{ts}.json"

    body = json.dumps(raw, ensure_ascii=False).encode("utf-8")
    s3.load_bytes(bytes_data=body, key=key, bucket_name=RAW_BUCKET, replace=True)

    # Airflow 2.6+ умеет сохранять dataset event extra; но даже если extra не поддержан,
    # consumer может вытащить key/bucket из XCom через DatasetEvent.
    return {"bucket": RAW_BUCKET, "key": key}


raw = fetch_cats()
ref = save_raw_to_minio(raw)

check_cat_api >> raw >> ref
