# -*- coding: utf-8 -*-
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, length, to_date, current_date, broadcast, window, to_json, struct, collect_list
from pyspark.sql import functions as F
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
    ANOMALY_TOPIC = os.getenv('ANOMALY_TOPIC')

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
        StructField("cntg_vol", StringType()),
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
        col("timestamp"),
        from_json(col("value").cast("string"), trade_schema).alias("data"), 
        col("key").cast("string").alias("stock_code"),
        length(col("value")).alias("message_size_bytes"),
        col("value").cast("string").alias("original_message") # 원본 카프카 메세지
    )

    # 6. sector 와 조인
    joined_df = parsed_df.join(
        broadcasted_sector_df,
        parsed_df.stock_code == broadcasted_sector_df.stock_code,
        "left"
    ).select(parsed_df["*"], broadcasted_sector_df["sector"], broadcasted_sector_df["name"])

    # ---------------------------------------------------------------------------------------------------------------------------------------------------
    # 7. 데이터 계산 부분
    # ---------------------------------------------------------------------------------------------------------------------------------------------------

    ## 7-1.일반 VWAP 데이터 계산
    window_agg_df = joined_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window("timestamp", "30 seconds"),
            col("stock_code"),
            col("sector"),
            col("name")
        ).agg(
            F.sum(F.col("data.stck_prpr").cast(LongType()) * F.col("data.cntg_vol").cast(LongType())).alias("sum_price_volume"),
            F.sum(F.col("data.cntg_vol")).cast(LongType()).alias("total_volume"),
            F.min(F.col("data.stck_prpr")).cast(LongType()).alias("min_price"),
            F.max(F.col("data.stck_prpr")).cast(LongType()).alias("max_price"),
            F.avg(F.col("data.stck_prpr")).cast(LongType()).alias("avg_price"),
            collect_list("original_message").alias("raw_messages")
        )
    
    window_agg_df = window_agg_df \
        .withColumn("vwap", F.col("sum_price_volume") / F.col("total_volume")) \
        .withColumn("update_time", F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ssXXX")) # 'T':날짜와 시간을 구분하기 위한 문자, "XXX":UTC 시간으로부터 한국 시간과의 차이를 나타낸다 Elasticsearch 에서 한국 시간을 인지하기 위한 옵션
    
    ## 7-2.일반 VWAP 데이터 계산 후 필요한 컬럼만 최종 select
    vwap_agg_df = window_agg_df.select("update_time", "stock_code", "sector", "name", "vwap", "total_volume")

    ## 7-3.이상치 급등락 데이터 - 일반 VWAP 데이터에서 필터
    anomaly_df = window_agg_df.filter(
        ((col("max_price") - col("min_price")) / col("min_price") >= 0.05) &
        ((col("max_price") - col("avg_price")) / col("avg_price") >= 0.03)
    ).select("update_time", "stock_code", "sector", "name", "min_price", "avg_price", "max_price", "total_volume", "raw_messages")

    # ---------------------------------------------------------------------------------------------------------------------------------------------------
    # 8. 데이터 적재
    # ---------------------------------------------------------------------------------------------------------------------------------------------------

    ## 8-1.공통 옵션
    es_common_options = {
        "es.nodes": ES_HOST, "es.port": ES_PORT, "es.net.ssl": "true",
        "es.net.http.auth.user": ES_USERNAME, "es.net.http.auth.pass": ES_PASSWORD,
        "es.nodes.wan.only": "true", "es.nodes.discovery": "false",
        "es.mapping.properties.name": "keyword", "es.mapping.properties.update_time": "date"
    }

    ## 8-2.일반 VWAP 데이터 ElasticSearch 전송
    es_resource_agg = f"stock_report_agg_{datetime.now().strftime('%Y-%m-%d')}"
    query_agg = (vwap_agg_df.writeStream
            .outputMode("append")
            .format("org.elasticsearch.spark.sql")
            .options(**es_common_options)
            .option("es.resource", es_resource_agg)
            .option("checkpointLocation", "./checkpoints/es_report_agg")
            .start())
    
    ## 8-3.이상치 데이터 ElasticSearch 전송
    es_resource_anomalies = f"stock_report_anomal_{datetime.now().strftime('%Y-%m-%d')}"
    query_anomal = (anomaly_df.writeStream
            .outputMode("append")
            .format("org.elasticsearch.spark.sql")
            .options(**es_common_options)
            .option("es.resource", es_resource_anomalies)
            .option("checkpointLocation", "./checkpoints/es_report_anomalies")
            .start())

    ## 8-3.이상치 데이터 원본 메세지와 함께 kafka 전송
    kafka_anomaly_df = anomaly_df.select(
        col("stock_code").alias("key"),
        to_json(struct(
            "update_time","stock_code", "sector", "name", 
            "min_price", "avg_price", "max_price", "total_volume", "raw_messages"
        )).alias("value")
    )

    query_anomalies_to_kafka = (kafka_anomaly_df.writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", ANOMALY_TOPIC)
        .option("checkpointLocation", "./checkpoints/kafka_report_anomalies")
        .start())

    print("스트리밍 쿼리 시작. VWAP 데이터는 ES로, 이상치 데이터는 Kafka로 전송합니다.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
