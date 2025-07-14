from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

with DAG(
    dag_id="mongo_data_quality_check",
    # 매일 16:05에 실행되도록 스케줄 설정
    schedule="5 16 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
    doc_md="매일 MongoDB에 적재된 데이터의 품질을 검증합니다. (기본값: 오늘 날짜)",
    tags=["quality-check", "batch"],
    # UI에서 입력받을 파라미터를 정의합니다.
    params={
        "target_date": Param(
            # 기본값을 오늘 날짜로 설정
            default=pendulum.now("Asia/Seoul").strftime('%Y-%m-%d'),
            type="string",
            title="검증 대상 날짜",
            description="YYYY-MM-DD 형식으로 검증할 날짜를 입력하세요. (기본값: 오늘 날짜)",
        )
    },
) as dag:
    run_quality_validation = BashOperator(
        task_id="run_quality_validation_task",
        # {{ params.target_date }}를 사용하여 전달된 파라미터 값을 사용합니다.
        bash_command="docker exec loader python3 /app/pipeline/script/4.quality_test_mongo.py {{ params.target_date }}",
    )