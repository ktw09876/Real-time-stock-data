from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import timedelta

# DAG 정의
with DAG(
    dag_id="stock_pipeline_controller",
    schedule="50 8 * * 1-5",  # 매주 월요일부터 금요일까지, 오전 8시 50분(KST)에 실행
    start_date=pendulum.datetime(2025, 6, 20, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ## 실시간 주식 데이터 파이프라인 컨트롤러 DAG
    
    - **Producer**: 한국투자증권 WebSocket API -> Kafka
    - **Loader**: Kafka -> MongoDB
    - **Spark Processor**: Kafka -> Spark -> S3
    
    이 DAG는 위 세 가지 스트리밍 서비스의 생명 주기를 관리합니다.
    """,
    tags=["stock-pipeline", "docker"],
) as dag:
    
    # 작업 1: 모든 스트리밍 서비스 시작
    # docker-compose.yaml에 정의된 producer, loader, spark 서비스를 백그라운드에서 실행합니다.
    start_streaming_services = BashOperator(
        task_id="start_streaming_services",
        bash_command="docker-compose up -d producer loader spark",
        doc_md="`producer`, `loader`, `spark` 3개의 서비스를 백그라운드에서 시작합니다."
    )
    
    # 작업 2: 장 마감 시간까지 대기
    # 한국 시간(UTC+9) 기준으로, 오후 4시까지 기다립니다.
    # DAG가 8:50에 시작되므로, 약 7시간 10분 후입니다.
    wait_until_market_close = TimeSensor(
        task_id="wait_until_market_close",
        target_time=pendulum.time(16, 0, 0), # 오후 4시 (16:00)
    )

    # 작업 3: 모든 스트리밍 서비스 중지
    stop_streaming_services = BashOperator(
        task_id="stop_streaming_services",
        bash_command="docker-compose stop producer loader spark",
        doc_md="실행 중이던 3개의 서비스를 중지합니다."
    )
    
    # 작업 흐름(의존성) 정의
    start_streaming_services >> wait_until_market_close >> stop_streaming_services

