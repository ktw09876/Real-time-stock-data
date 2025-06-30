from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="streaming_producer",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    doc_md="[1] 데이터 프로듀서: KIS API로부터 데이터를 수집하여 Kafka로 전송합니다.",
    tags=["streaming-control"],
) as dag:
    run_data_producer = BashOperator(
        task_id="run_data_producer_task",
        bash_command="docker exec producer python3 -u /app/websockets/script/1.data_extract.py",
    )