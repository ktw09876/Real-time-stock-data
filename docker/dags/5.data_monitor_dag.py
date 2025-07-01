from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="data_monitor",
    schedule="*/5 * * * 1-5", 
    start_date=pendulum.datetime(2025, 6, 27, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ## 실시간 데이터 수집 상태 모니터링 DAG
    
    매 5분마다 MongoDB에 데이터가 정상적으로 적재되고 있는지 확인합니다.
    `loader` 컨테이너에서 검사 스크립트를 실행하며,
    데이터 수집이 5분 이상 중단될 경우 경고 이메일을 보냅니다.
    """,
    tags=["monitoring", "stock_pipeline"],
) as dag:
    
    # 작업 1: loader 컨테이너에서 데이터 지연 시간을 확인하는 스크립트 실행
    check_mongodb_latency = BashOperator(
        task_id="check_mongodb_latency_task",
        # loader 컨테이너의 /app 폴더에 마운트된 스크립트를 실행
        bash_command="docker exec loader python3 /app/websockets/script/5.data_monitor.py",
    )

    # 작업 2: 센서 성공 시 실행되는 더미 태스크
    all_clear_task = DummyOperator(
        task_id="all_clear"
    )

    # 작업 3: 센서 실패 시 실행되는 이메일 알림 태스크
    send_alert_email = EmailOperator(
        task_id="send_ingestion_failure_alert",
        to="your_email@example.com", # 실제 이메일 주소로 변경
        subject="[긴급] 데이터 파이프라인 수집 지연 경고 ({{ ds }})",
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
        # 이전 태스크가 실패했을 때만 이메일을 보냄
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    
    # 작업 흐름 정의:
    # check_mongodb_latency 태스크가 성공하면 all_clear_task가 실행됨
    # check_mongodb_latency 태스크가 실패하면 send_alert_email 태스크가 실행됨
    check_mongodb_latency >> all_clear_task
    check_mongodb_latency >> send_alert_email