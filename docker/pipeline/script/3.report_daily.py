# -*- coding: utf-8 -*-
import os
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_format, lit, round as spark_round, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import s3fs

def main():
    """
    [최종] Kafka의 체결가 토픽(H0STCNT0) 하나만 구독하여,
    VWAP, 체결강도, 매수/매도 압력 등 종합적인 시장 지표를 계산하고
    그 결과를 S3의 단일 CSV 파일에 추가하는 Spark Structured Streaming 애플리케이션.
    """
    # 1. 환경 변수에서 설정 값 로드 및 검증
    required_vars = [
        'KAFKA_BROKER_INTERNAL', 'SPARK_TRADE_TOPIC', 'S3_BUCKET'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    KAFKA_BROKER = os.getenv('KAFKA_BROKER_INTERNAL')
    TRADE_TOPIC = os.getenv('SPARK_TRADE_TOPIC')
    # S3_BUCKET = os.getenv('S3_BUCKET')

    ES_ENDPOINT_URL = os.getenv('ES_ENDPOINT')
    if ES_ENDPOINT_URL and "://" in ES_ENDPOINT_URL:
        ES_HOST = ES_ENDPOINT_URL.split("://")[1]
    else:
        ES_HOST = ES_ENDPOINT_URL

    ES_PORT = os.getenv('ES_PORT')
    ES_USERNAME = os.getenv('ES_USERNAME')
    ES_PASSWORD = os.getenv('ES_PASSWORD')
    
    KST = timezone(timedelta(hours=9))

    # 2. Spark Session 생성
    spark = (SparkSession.builder
             .appName("RealtimeMarketAnalysisStateless")
             .getOrCreate())
    
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # 3. 데이터 스키마 정의 (H0STCNT0 JSON 파싱용)
    trade_schema = StructType([
        StructField("stck_prpr", StringType()),
        StructField("acml_vol", StringType()),
        StructField("acml_tr_pbmn", StringType()),
        StructField("cttr", StringType()),
        StructField("total_askp_rsqn", StringType()),
        StructField("total_bidp_rsqn", StringType()),
    ])

    # 4. Kafka 스트림 읽기
    kafka_stream = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TRADE_TOPIC)
            .option("failOnDataLoss", "false")
            .load())

    # 5. 최종 지표 계산 (Stateless 변환)
    # JSON 메시지를 파싱하여 데이터프레임으로 변환
    parsed_df = kafka_stream.select(
        # col("value").cast("string"), # 원본
        from_json(col("value").cast("string"), trade_schema).alias("data"), 
        col("key").cast("string").alias("stock_code")
    )

    # 지표를 계산하고 컬럼 이름을 부여합니다.
    result_df = (parsed_df
        .select(
            col("stock_code"),
            col("data.acml_vol").cast(LongType()).alias("cumulative_volume"),
            col("data.acml_tr_pbmn").cast(LongType()).alias("cumulative_value"),
            col("data.cttr").cast(LongType()).alias("trade_strength"),
            col("data.total_askp_rsqn").cast(LongType()).alias("total_ask_qty"),
            col("data.total_bidp_rsqn").cast(LongType()).alias("total_bid_qty"),
        ).filter(col("cumulative_volume").isNotNull() & (col("cumulative_volume") > 0)) # 0으로 나누는 오류를 방지
        .withColumn("vwap", col("cumulative_value") / col("cumulative_volume")) # VWAP (거래량 가중 평균 가격)
        .withColumn("buy_sell_pressure", when(col("total_ask_qty") > 0, col("total_bid_qty") / col("total_ask_qty")).otherwise(0)) # 매수/매도 압력: total_ask_qty가 0보다 클 때만 계산하고, 아니면 0을 반환)
        .withColumn("update_time", current_timestamp())
    )

    es_resource = f"stock_report_{datetime.now().strftime('%Y-%m-%d')}"
    query = (result_df.writeStream
            .outputMode("append")
            .format("console") 
            .format("org.elasticsearch.spark.sql")
            .option("es.nodes", ES_HOST)
            .option("es.port", ES_PORT)
            .option("es.net.ssl", "true")
            .option("es.net.http.auth.user", ES_USERNAME)
            .option("es.net.http.auth.pass", ES_PASSWORD)
            .option("es.nodes.wan.only", "true")
            .option("es.nodes.discovery", "false")
            .option("es.resource", es_resource)
            # .option("es.mapping.id", "stock_code")
            .option("es.mapping.properties.update_time", "date")
            .option("checkpointLocation", "./checkpoints/es_report")
            .start())
    print(f"스트리밍 쿼리 시작. 결과를 GCP ElasticSearch 으로 전송합니다.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
