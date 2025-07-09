# 실시간 주식 데이터 연계
<img src="/capture/pipeline_capture.png" width="1000"/> 

## 실시간 주식 데이터 자동화 분석을 위한 파이프라인 제작 및 배치 
- 국내 주식의 실시간 체결가를 수집해서 VWAP (거래량 가중 평균 가격)을 계산
- 장 시작시간을 고려하여 평일 08:50분 시작, 16:00에 종료
- 안정성과 다른 추가 분석을 위한 원본 데이터 별도 수집
### 데이터 수집 및 적재(한국투자증권 api --> kafka, MongoDB)
<img src="/capture/1.data_extract/Screenshot_2.png" width="1000"/>
<img src="/capture/2.mongo_load/Screenshot_1.png" width="1000"/>

1. 삼성전자, SK하이닉스, 카카오 주식의 실시간 호가, 체결가, 예상체결가를 websockets 방식으로 수집, 직렬화 합니다.
2. 직렬화 한 데이터는 몽고DB 와 kafka 로 각각 보내집니다.
    - 몽고DB에 따로 적재하는 이유
    1. 원시데이터의 스키마가 일정하지 않음
    2. 짧은 시간 동안 대량의 데이터를 적재하기에 몽고DB 가 적절하다고 생각했습니다.
    3. 적재하는 원시데이터의 종류가 늘어나는 경우에도 유연한 확장성
    4. 이후 추가 분석이나 오류로 인한 검증 작업을 위해 윈시데이터를 적재했습니다.

### 데이터 추출 and 가공(kafka --> Spark)
1. 카프카의 데이터 중에 실시간 체결가를 스파크 스트리밍을 이용해서 실시간 구독합니다.
2. 오늘 날짜로 필터링해서 실시간으로 분석합니다.
### 분석 결과 리포트 적재(Spark --> GCP ElasticSearch)
1. 집계한 결과는 GCP ElasticSearch 에 저장됩니다.
### 시각화(Kibana)
<img src="/capture/6.visualization/dashboards.png" width="1000"/>

## 배치 작업(AirFlow)
<img src="/capture/4.quality_test_mongo/test_mongo_log.png" width="1000"/>
<img src="/capture/5.data_monitoring/email_report.png" width="1000"/>

1. 매주 평일 오전 08시 50분에 전체 배치 시작, 오후 04시 00분에 배치가 종료됩니다.
2. 16:05 데이터의 이상치를 포함한 품질 검사합니다.
3. 5분마다 데이터가 제대로 적재되고있는지 확인하고 만약 5분 이상 적재되지 않으면 알람 이메일을 보냅니다.
## 회고
### 개발환경(AWS VS GCP)
- 처음 사용하는만큼 직관적인 UI 가 필요했음
- AWS 의 경우 세부 보안 설정이 어려웠음
- OpenSearch 를 연결할 때 발생한 에러 해결 못함
    - 스파크 버전 호환
    - OpenSearch 자격 증명

--> 이와 같은 이유로 GCP ElasticSearch 사용 결정
### Airflow와 Docker
- Docker를 이용해서 각 단계를 컨테이너로 감싸서 환경을 분리한다는건 좋았음
- 여기에 Airflow를 도입하면서 Airflow가 Docker Container 안에서 각 단계별 .py를 실행한다는 개념이 어려웠음

### 구현 예정
- api 를 호출하기 때문에 가능성은 낮지만 처리에 실패하는 메시지가 생길 수 있음
    - DLQ(Dead Letter Queue) 를 도입해서 실패한 메시지를 모아 패턴을 분석해서 대응하는 과정이 필요