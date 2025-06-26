# 실시간 주식 데이터 연계
<img src="/capture/pipeline_capture.png" width="1500"/>

## 실시간 주식 데이터 자동화 분석을 위한 파이프라인 제작 및 배치 
- 국내 주식의 실시간 체결가를 수집해서 누적 거래량, 누적 거래 대금, 거래량 가중 평균 가격, 체결강도, 매수/매도 압력을 기록
- 장 시작시간을 고려하여 평일 08:50분 시작, 16:00에 종료
- 안정성과 다른 추가 분석을 위한 원본 데이터 별도 수집
### 데이터 수집(한국투자증권 api --> kafka)
1. 삼성전자, SK하이닉스, 카카오 주식의 실시간 호가, 체결가, 예상체결가를 websockets 방식으로 수집, kafka 로 보냅니다.
### 원시 데이터 적재(kafka --> MongoDB)
1. 수집한 원시 데이터는 직렬화 해서 몽고DB stock_db 컬렉션에 저장됩니다.
    - 이유
    1. 이후 가공 단계에서 오류가 발생할 경우 몽고DB의 원시 데이터를 이용해서 안정적으로 데이터를 재공급할 수 있음
    2. 추가 분석, 집계 작업이 필요한 경우 활용할 수 있습니다.
### 데이터 추출 and 가공(kafka --> Spark)
1. 카프카의 데이터 중에 실시간 체결가를 스파크 스트리밍을 이용해서 실시간 구독합니다.
2. 오늘 날짜로 필터링해서 실시간으로 분석합니다.
### 데이터 적재(Spark --> AWS s3)
1. 집계한 결과는 aws s3 버킷에 저장됩니다.
2. daily_report/stock_code= 와 같이 종목 별로 디렉토리가 나뉘고 그 안에 yyyy-mm-dd.csv 형식의 단일 파일로 집계결과가 추가됩니다.
    - 실시간 리포트를 s3에 업데이트 및 .csv 을 선택한 이유 
    1. 결과 리포트를 사람이 직접 눈으로 확인하기 위해서는 .csv 형식이 적절하다고 생각했습니다.
    2. 실시간 데이터라는 특성에 맞게 s3에 실시간으로 공유하도록 했습니다.
    3. 이후 데이터양이 증가함에 따라 .parquet 형식으로 변경, 해당 결과리포트를 자바나 파이썬 등으로 한번에 불러오는 등 확장을 고려할 수 있습니다.
## 데이터 검증
1. 장이 마감된 후 16:10 에 몽고DB의 데이터 품질 검사를 합니다.
## 배치 작업(AirFlow)
1. 매주 평일 오전 08시 50분에 전체 배치 시작, 오후 04시 00분에 배치가 종료됩니다.
### 미구현 기능(추가 예정)
1. 수집이나 가공에 오류가 있는 날짜의 데이터를 재작업하는 기능
2. 일일 데이터 검증 리포트를 메일로 전송
## 결과 
<img src="/capture/1.data_extract/Screenshot_1.png" width="1000"/>
<img src="/capture/1.data_extract/Screenshot_2.png" width="1000"/>
<img src="/capture/2.mongo_load/Screenshot_1.png" width="1000"/>
<img src="/capture/3.report_daily/Screenshot_1.png" width="1000"/>
<img src="/capture/3.report_daily/Screenshot_2.png" width="1000"/>
<img src="/capture/3.report_daily/Screenshot_3.png" width="1000"/>