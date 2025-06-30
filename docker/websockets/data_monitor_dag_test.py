# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.models.dag import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

def _check_mongodb_latency():
    """
    MongoDB에 연결하여 **오늘 날짜의** H0STCNT0 컬렉션 데이터 중
    가장 최근 데이터의 insert_time을 확인하고, 현재 시간과의 차이를 검사하는 함수.
    
    이 함수는 PythonSensor에 의해 주기적으로 호출되며,
    결과는 콘솔과 별도의 로그 파일에 모두 기록됩니다.
    """
    from pymongo import MongoClient

    # --- 로거 설정 ---
    logger = logging.getLogger("data_ingestion_monitor_sensor")
    logger.setLevel(logging.INFO)

     # 오늘 날짜의 범위를 UTC로 계산합니다.
    KST = timezone(timedelta(hours=9))
    today_start_kst = datetime.now(KST).strftime('%Y-%m-%d')
    today_start_utc = today_start_kst.astimezone(timezone.utc)
    
    if not logger.hasHandlers():
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        
        log_dir = os.getenv('LOG_PATH')
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, f"{today_start_kst}_data_monitor.log")
        
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"별도 모니터링 로그가 '{log_file_path}' 경로에 저장됩니다.")

    # --- 환경 변수 로드 ---
    MONGO_HOST = os.getenv('MONGO_HOST')
    MONGO_PORT = int(os.getenv('MONGO_PORT'))
    MONGO_DB_NAME = os.getenv('MONGO_DATABASE')
    COLLECTION_NAME = os.getenv('SPARK_TRADE_TOPIC')
    
    if not all([MONGO_HOST, MONGO_DB_NAME, COLLECTION_NAME]):
        logger.error("MongoDB 연결에 필요한 환경 변수가 설정되지 않았습니다.")
        return False

    client = None
    try:
        print(f"MongoDB에 연결 시도: {MONGO_HOST}:{MONGO_PORT}")
        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("MongoDB 연결에 성공했습니다.")
        
        db = client[MONGO_DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # 오늘 날짜의 데이터 중에서 가장 최근 문서를 찾습니다.
        latest_doc = collection.find_one(
            {'insert_time': {'$gte': today_start_utc}},
            sort=[('insert_time', -1)]
        )
        
        if not latest_doc:
            logger.warning(f"오늘({today_start_kst.strftime('%Y-%m-%d')})의 데이터가 아직 수집되지 않았습니다. (장 시작 전이거나 문제가 있을 수 있습니다)")
            return False

        last_insert_time = latest_doc.get('insert_time')
        if not last_insert_time:
            logger.error("최신 문서에 'insert_time' 필드가 없습니다.")
            return False

        current_time_utc = datetime.now(timezone.utc)
        time_diff_seconds = (current_time_utc - last_insert_time).total_seconds()
        
        logger.info(f"마지막 데이터 저장 시간: {last_insert_time}")
        logger.info(f"현재 시간: {current_time_utc}")
        logger.info(f"시간 차이: {time_diff_seconds:.2f} 초")

        latency_threshold = 300 # 5분

        if time_diff_seconds < latency_threshold:
            logger.info(f"데이터 수집이 정상입니다 (지연 시간 < {latency_threshold}초).")
            return True
        else:
            logger.error(f"경고: 데이터 수집이 {latency_threshold}초 이상 지연되었습니다.")
            return False

    except Exception as e:
        logger.error(f"MongoDB 연결 또는 쿼리 중 오류 발생: {e}")
        return False
    finally:
        if client:
            client.close()

# DAG 정의
with DAG(
    dag_id="data_monitor",
    schedule="*/5 * * * 1-5", 
    start_date=pendulum.datetime(2025, 6, 27, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ## 실시간 데이터 수집 상태 모니터링 DAG
    
    매 5분마다 MongoDB에 **오늘 날짜의 데이터**가 정상적으로 적재되고 있는지 확인합니다.
    데이터 수집이 5분 이상 중단될 경우, DAG는 실패 상태가 되고 알림을 보냅니다.
    """,
    tags=["monitoring", "stock_pipeline"],
) as dag:
    
    # 작업 1: 데이터 수집 상태를 감지하는 센서
    check_data_ingestion_status = PythonSensor(
        task_id="check_mongodb_latency",
        python_callable=_check_mongodb_latency,
        poke_interval=60,
        timeout=240,       # 4분 동안 계속 확인 (60초 * 4회)
        mode='poke',
        soft_fail=True,
    )

    # 작업 2: 센서 성공 시 실행되는 더미 태스크
    all_clear_task = DummyOperator(
        task_id="all_clear"
    )

    # 작업 3: 센서 실패 시 실행되는 이메일 알림 태스크
    send_alert_email = EmailOperator(
        task_id="send_ingestion_failure_alert",
        to="your_email@example.com", # 알림을 받을 이메일 주소
        subject="[긴급] 데이터 파이프라인 수집 지연 경고 ({{ ds }})",
        # [수정] 이메일 본문에 날짜를 명확히 표시
        html_content="""
        <h3>데이터 수집 지연 경고</h3>
        <p>
            <strong>{{ ds }}</strong> 날짜의 실시간 데이터 수집이 <strong>5분 이상 중단</strong>된 것으로 감지되었습니다.
        </p>
        <p>
            <strong>DAG:</strong> {{ dag.dag_id }} <br>
            <strong>Task:</strong> {{ task.task_id }} <br>
            <strong>실행 시간:</strong> {{ ts }}
        </p>
        <p>
            즉시 Airflow UI의 로그를 확인하고, producer 및 loader 컨테이너의 상태를 점검해주시기 바랍니다.
        </p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    # 작업 흐름 정의
    check_data_ingestion_status >> [all_clear_task, send_alert_email]
