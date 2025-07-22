# 실시간 주식 데이터 연계
<img src="/capture/pipeline_capture.png" width="1000"/> 

## 실시간 주식 데이터 자동화 분석을 위한 파이프라인 제작 및 배치 
- 국내 주식의 실시간 체결가를 수집해서 VWAP (거래량 가중 평균 가격)을 계산, GCP ElasticSearch 에 적재, Kibana 로 시각화
- VWAP 를 통해 각 종목의 가격 변동을 실시간 모니터링
- 수집 종목 개수 - 41 개
- 수집 데이터 크기: 1시간에 약 700MB
- 장 시작시간을 고려하여 평일 08:50분 시작, 16:00에 종료
- 안정성과 다른 추가 분석을 위한 원본 데이터 별도 수집
### 데이터 수집 및 적재(한국투자증권 api --> kafka, MongoDB)
<img src="/capture/1.data_extract/Screenshot_2.png" width="1000"/>
<img src="/capture/2.mongo_load/Screenshot_1.png" width="1000"/>

1. 삼성전자, SK하이닉스를 포함한 41개 종목의 실시간 체결가를 websockets 방식으로 수집, 직렬화 합니다.
2. 직렬화 한 데이터는 몽고DB 와 kafka 로 각각 보내집니다.
    - 몽고DB에 따로 적재하는 이유
    1. 원시데이터의 스키마가 일정하지 않음
    2. 짧은 시간 동안 대량의 데이터를 적재하기에 몽고DB 가 적절하다고 생각했습니다.
    3. 적재하는 원시데이터의 종류가 늘어나는 경우에도 유연한 확장성
    4. 이후 추가 분석이나 오류로 인한 검증 작업을 위해 윈시데이터를 적재했습니다.

### 데이터 추출 and 가공(kafka --> Spark)
1. 카프카의 실시간 체결가 토픽을 스파크 스트리밍을 이용해서 구독합니다.
2. 오늘 날짜로 필터링해서 실시간으로 분석합니다.
### 분석 결과 리포트 적재(Spark --> GCP ElasticSearch, Kafka)
1. 집계한 결과는 GCP ElasticSearch 에 저장됩니다.
2. 임의의 이상치 기준을 설정, 기준에서 벗어나는 데이터는 원본 메세지와 함께 카프카의 별도 토픽에 재적재 합니다.
<img src="/capture/3.report_daily/anomal_test1.png" width="1000"/>
<img src="/capture/3.report_daily/anomal_test2.png" width="1000"/>
### 시각화(Kibana)
- 각 항목으로 구분
<img src="/capture/6.visualization/dashboards.png" width="1000"/>
<img src="/capture/6.visualization/dashboards2.png" width="1000"/>

## 배치 작업(AirFlow)
<img src="/capture/5.data_monitoring/email_report.png" width="1000"/>

1. 매주 평일 오전 08시 50분에 전체 배치 시작, 오후 04시 00분에 배치가 종료됩니다.
2. 5분마다 데이터가 제대로 적재되고있는지 확인하고 만약 5분 이상 적재되지 않으면 알람 이메일을 보냅니다.
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

### 포트 충돌 문제
상황: 개발 환경을 기존 EC2에서 로컬로 변경 후 몽고 DB의 데이터가 컨테이너에서는 조회 되지만 로컬 Mongo Compass 에서는 조회되지 않음
- 구글링을 통해 포트 충돌의 가능성을 알게 되어 윈도우 로컬에서 포트 조회
- 도커 컨테이너의 몽고DB 와 동일한 27017 포트를 사용 중인 몽고DB 서비스 확인
- 해당 서비스를 종료 후 재연결하니 데이터가 조회됨
- 기존 윈도우 로컬의 몽고DB 가 사용 중인 포트 번호를 도커 컨테이너에서 설정한 상태에서 연결 시도를 해서 데이터는 컨테이너에 쌓이고 조회는 윈도우 로컬의 몽고DB에서 시도했음(포트 충돌)  

결론: 윈도우 서비스를 종료하던지 도커의 포트 번호를 다르게 하자

### 시간 type 문제
상황: 실시간 데이터를 확인하기 위한 'update_time' 컬럼을 생성,  ElasticSearch 에 적재했음, 하지만 
1. type = long 으로 생성, 적재됨
2. type = text 으로 생성, 적재됨
3. UTC 형식으로 생성, 적재됨
```python
spark = (...
        .config("spark.sql.session.timeZone", "Asia/Seoul") 
        ...)

.withColumn("update_time", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX")) # 'T':날짜와 시간을 구분하기 위한 문자, "XXX":UTC 시간으로부터 한국 시간과의 차이를 나타낸다 Elasticsearch 에서 한국 시간을 인지하기 위한 옵션
```
--> 위 두 옵션을 주고 Kibana에서 update_time 컬럼의 set Format 을 수정해서 해결


### 구현 예정
- api 를 호출하기 때문에 가능성은 낮지만 처리에 실패하는 메시지가 생길 수 있음
    - DLQ(Dead Letter Queue) 를 도입해서 실패한 메시지를 모아 패턴을 분석해서 대응하는 과정이 필요