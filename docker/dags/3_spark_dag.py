from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session

# 가장 최근에 'running' 상태인 loader DAG 실행을 찾는 함수
@provide_session
def get_latest_producer_dag_run(execution_date, session=None, **kwargs):
    dag_run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == "streaming_loader",
            DagRun.state == "running",
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    return [dag_run.execution_date] if dag_run else []

with DAG(
    dag_id="streaming_spark_reporter",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    doc_md="[3] 실시간 리포터: 프로듀서가 실행 중일 때만 실행됩니다.",
    tags=["streaming-control"],
) as dag:
    # 프로듀서 DAG의 태스크가 'running' 상태인지 감지하는 센서
    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer_running",
        external_dag_id="streaming_producer",  # 감시할 DAG의 ID
        external_task_id="run_data_producer_task",  # 감시할 태스크의 ID
        allowed_states=["running"],  # 'running' 상태를 성공으로 간주
        failed_states=["failed", "skipped"],
        execution_date_fn=get_latest_producer_dag_run,
        poke_interval=30,  # 30초마다 상태를 확인
        timeout=600, # 10분 동안 감지되지 않으면 실패 처리
        mode="poke",
    )

    run_spark_reporter = BashOperator(
        task_id="run_spark_reporter_task",
        bash_command="""
            docker exec spark /bin/bash -c 'spark-submit \
            --master "local[*]" \
            --jars /app/jars/elasticsearch-spark-30_2.12-8.17.8.jar,/app/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,/app/jars/kafka-clients-3.4.1.jar,/app/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar,/app/jars/commons-pool2-2.11.1.jar \
            /app/websockets/script/3.report_daily.py'
        """,
    )
    
    # 센서가 성공해야 스파크 리포터가 실행되도록 종속성 설정
    wait_for_producer >> run_spark_reporter