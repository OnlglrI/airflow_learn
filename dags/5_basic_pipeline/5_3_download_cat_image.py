"""DAG 3_3_download_cat_image

1) HttpSensor проверяет доступность TheCatAPI endpoint
2) Скачивает картинку кота в /opt/airflow/data/img
3) Возвращает путь к файлу (XCom)

Источник:
GET https://api.thecatapi.com/v1/images/search
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook


DATA_DIR = Path(Variable.get("DATA_IMG_DIR", default_var="/opt/airflow/data/img"))
THECATAPI_CONN_ID = Variable.get("THECATAPI_CONN_ID", default_var="thecatapi_default")
CAT_API_ENDPOINT = "/v1/images/search"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


dag = DAG(
    dag_id="5_3_download_cat_image",
    default_args=default_args,
    description="Check TheCatAPI and download image to /opt/airflow/data/img",
    schedule_interval=None,
    catchup=False,
    tags=["api", "sensor", "files", "cats", "basic_pipeline"],
)


check_cat_api = HttpSensor(
    task_id="check_cat_api",
    http_conn_id=THECATAPI_CONN_ID,
    endpoint=CAT_API_ENDPOINT,
    response_check=lambda r: r.status_code == 200,
    poke_interval=20,
    timeout=120,
    mode="reschedule",
    dag=dag,
)


@task(task_id="download_cat_image", dag=dag)
def download_cat_image() -> Dict[str, str]:
    """Запрашивает TheCatAPI, скачивает одну картинку и сохраняет в DATA_DIR."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    hook = HttpHook(method="GET", http_conn_id=THECATAPI_CONN_ID)
    r = hook.run(CAT_API_ENDPOINT)
    r.raise_for_status()
    payload = r.json()

    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Unexpected payload from TheCatAPI: {payload}")

    item = payload[0]
    url = item.get("url")
    if not url:
        raise ValueError(f"No url in TheCatAPI response item: {item}")

    # определяем расширение
    ext = os.path.splitext(url.split("?")[0])[1].lower()
    if not ext or len(ext) > 5:
        ext = ".jpg"

    filename = f"cat_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}{ext}"
    file_path = DATA_DIR / filename

    img = requests.get(url, timeout=60)
    img.raise_for_status()

    file_path.write_bytes(img.content)

    return {
        "file_path": str(file_path),
        "file_name": filename,
        "source_url": url,
    }


meta = download_cat_image()
check_cat_api >> meta
