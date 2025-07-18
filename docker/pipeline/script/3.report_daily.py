# -*- coding: utf-8 -*-
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, when, current_timestamp, explode, length, to_date, current_date, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType

"""
Kafka의 체결가 토픽(H0STCNT0) 하나만 구독하여,
VWAP, 체결강도, 매수/매도 압력 등 종합적인 시장 지표를 계산하고
그 결과를 GCP ElasticSearch 에 적재하는 Spark Structured Streaming 애플리케이션.
"""
def main():
    # 0. 환경 변수에서 설정 값 로드 및 검증
    test_env_var = [
        'KAFKA_BROKER_INTERNAL',
        'KAFKA_TOPICS',
        'ES_ENDPOINT',
        'ES_PORT',
        'ES_USERNAME',
        'ES_PASSWORD'
    ]

    missing_vars = []
    for var in test_env_var:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    KAFKA_BROKER = os.getenv('KAFKA_BROKER_INTERNAL')
    TRADE_TOPIC = os.getenv('KAFKA_TOPICS')

    ES_ENDPOINT_URL = os.getenv('ES_ENDPOINT')
    if ES_ENDPOINT_URL and "://" in ES_ENDPOINT_URL:
        ES_HOST = ES_ENDPOINT_URL.split("://")[1]
    else:
        ES_HOST = ES_ENDPOINT_URL

    ES_PORT = os.getenv('ES_PORT')
    ES_USERNAME = os.getenv('ES_USERNAME')
    ES_PASSWORD = os.getenv('ES_PASSWORD')

    # 1. Spark Session 생성
    spark = (SparkSession.builder
             .appName("RealtimeMarketAnalysisStateless")
             .config("spark.sql.session.timeZone", "Asia/Seoul")
             .getOrCreate())

    # 2. sector_info 파일을 읽어 DataFrame으로 만들고, 
    sector_df = spark.read.option("multiline", "true") \
                     .json("/app/pipeline/sector/sector_info.json") \
                     .withColumn("sector", explode(col("sectors"))) \
                     .drop("sectors")
    
    broadcasted_sector_df = broadcast(sector_df) # broadcast로 모든 노드에 배포

    # 데이터 스키마 정의 (kafka의 H0STCNT0 JSON 파싱용)
    trade_schema = StructType([
        StructField("stck_prpr", StringType()),
        StructField("acml_vol", StringType()),
        StructField("acml_tr_pbmn", StringType()),
        StructField("cttr", StringType()),
        StructField("total_askp_rsqn", StringType()),
        StructField("total_bidp_rsqn", StringType()),
    ])

    # 3. Kafka 스트림 읽기
    kafka_stream = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TRADE_TOPIC)
            .option("failOnDataLoss", "false")
            .load())
    
    # 4. 오늘 날짜로 필터
    today_df = kafka_stream.filter(to_date(col("timestamp")) == current_date())

    # 5. kafka 메시지를 파싱하여 데이터프레임으로 변환
    parsed_df = today_df.select(
        # col("value").cast("string"), # 원본
        from_json(col("value").cast("string"), trade_schema).alias("data"), 
        col("key").cast("string").alias("stock_code"),
        length(col("value")).alias("message_size_bytes")
    )

    # 6. sector 와 조인
    joined_df = parsed_df.join(
        broadcasted_sector_df,
        parsed_df.stock_code == broadcasted_sector_df.stock_code,
        "left"
    ).select(parsed_df["*"], broadcasted_sector_df["sector"], broadcasted_sector_df["name"])

    # 7.지표 계산, 컬럼 이름을 부여
    result_df = (joined_df
        .select(
            col("stock_code"),
            col("sector"),
            col("name"),
            col("message_size_bytes"),
            col("data.acml_vol").cast(LongType()).alias("cumulative_volume"),
            col("data.acml_tr_pbmn").cast(LongType()).alias("cumulative_value"),
            col("data.cttr").cast(LongType()).alias("trade_strength"),
            col("data.total_askp_rsqn").cast(LongType()).alias("total_ask_qty"),
            col("data.total_bidp_rsqn").cast(LongType()).alias("total_bid_qty"),
        ).filter(col("cumulative_volume").isNotNull() & (col("cumulative_volume") > 0)) # 0으로 나누는 오류를 방지
        .withColumn("vwap", col("cumulative_value") / col("cumulative_volume")) # VWAP (거래량 가중 평균 가격)
        .withColumn("buy_sell_pressure", when(col("total_ask_qty") > 0, col("total_bid_qty") / col("total_ask_qty")).otherwise(0)) # 매수/매도 압력: total_ask_qty가 0보다 클 때만 계산하고, 아니면 0을 반환)
        # .withColumn("update_time", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("update_time", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssXXX")) # 'T':날짜와 시간을 구분하기 위한 문자, "XXX":UTC 시간으로부터 한국 시간과의 차이를 나타낸다 Elasticsearch 에서 한국 시간을 인지하기 위한 옵션
    )

    # 8. ElasticSearch 적재
    es_resource = f"stock_report_{datetime.now().strftime('%Y-%m-%d')}"
    query = (result_df.writeStream
            .outputMode("append")
            # .format("console")                     # 데이터프레임 콘솔 출력
            .format("org.elasticsearch.spark.sql") # 데이터프레임 ElasticSearch 로 보냄
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.net.ssl", "true")
            .option("es.net.http.auth.user", ES_USERNAME)
            .option("es.net.http.auth.pass", ES_PASSWORD)
            .option("es.nodes.wan.only", "true")
            .option("es.nodes.discovery", "false")
            .option("es.resource", es_resource)
            .option("es.mapping.properties.sector", "keyword")
            .option("es.mapping.properties.name", "keyword")
            .option("es.mapping.properties.update_time", "date")
            .option("checkpointLocation", "./checkpoints/es_report")
            .start())
    print(f"스트리밍 쿼리 시작. 결과를 GCP ElasticSearch 으로 전송합니다.")
    query.awaitTermination() # 스트리밍이 종료될때까지 계속 실행

if __name__ == "__main__":
    main()
