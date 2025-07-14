from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

def _generate_alert_html_content(**context):
    """
    실패 시 보낼 이메일의 HTML 본문을 생성하여 XCom에 저장(return)합니다.
    """
    dag_run = context["dag_run"]
    execution_time_kst = pendulum.parse(dag_run.execution_date.isoformat()).in_timezone('Asia/Seoul')
    
    html_content = f"""
    <h3>데이터 수집 지연 경고</h3>
    <p>데이터 파이프라인에 문제가 감지되었습니다. Airflow UI를 확인해주세요.</p>
    <p>
        <strong>DAG:</strong> {dag_run.dag_id} <br>
        <strong>실행 시간 (KST):</strong> {execution_time_kst.strftime('%Y-%m-%d %H:%M:%S')}
    </p>
    """
    return html_content

with DAG(
    dag_id="data_monitor",
    schedule="*/5 * * * 1-5", 
    start_date=pendulum.datetime(2025, 6, 27, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ## 실시간 데이터 수집 상태 모니터링 DAG
    
    매 5분마다 loader 컨테이너 상태와 MongoDB 데이터 적재 상태를 확인합니다.
    하나라도 문제가 발생하면 경고 이메일을 보냅니다.
    """,
    tags=["monitoring", "stock_pipeline"],
) as dag:
    
    # 작업 1-1: loader 컨테이너가 실행 중인지 확인
    check_loader_container_running = BashOperator(
        task_id="check_loader_container_running",
        bash_command="docker inspect -f '{% raw %}{{.State.Running}}{% endraw %}' loader | grep true",
    )
    
    # 작업 1-2: 데이터 지연 시간을 확인하는 스크립트 실행
    check_mongodb_latency = BashOperator(
        task_id="check_mongodb_latency_task",
        bash_command="docker exec loader python3 /app/pipeline/script/5.data_monitoring.py",
    )

    # 작업 2: 모든 검사가 성공했을 때만 실행되는 '성공' 더미 태스크
    all_clear = DummyOperator(
        task_id="all_clear",
        # 기본 규칙(all_success)을 사용하므로, 두 확인 작업이 모두 성공해야 실행됨
    )

    # 실패 경로의 구조를 변경합니다.
    join_on_failure = DummyOperator(
        task_id='join_on_failure',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # 2. 이메일 본문을 생성하는 새로운 PythonOperator 태스크
    generate_email_content = PythonOperator(
        task_id="generate_email_content",
        python_callable=_generate_alert_html_content,
    )

    # 3. EmailOperator가 XCom을 통해 이메일 본문을 가져오도록 변경
    send_alert_email = EmailOperator(
        task_id="send_ingestion_failure_alert",
        # to="{{ var.value.alert_recipient_email }}",
        to="dhqhfjem@gmail.com",
        subject="[긴급] 데이터 파이프라인 수집 지연 경고 ({{ ds }})",
        html_content="{{ task_instance.xcom_pull(task_ids='generate_email_content') }}", # XCom에서 'generate_email_content' 태스크의 반환 값을 가져옵니다.
    )
    
    # [수정된 작업 흐름]
    check_loader_container_running >> check_mongodb_latency >> all_clear
    [check_loader_container_running, check_mongodb_latency] >> join_on_failure
    
    # 실패가 감지되면 -> 이메일 본문을 생성하고 -> 이메일을 보냅니다.
    join_on_failure >> generate_email_content >> send_alert_email
