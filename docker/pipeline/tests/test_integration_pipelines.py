import unittest
import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
from pymongo import MongoClient
import s3fs
from io import StringIO
from datetime import datetime

class TestPipelineIntegration(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        required_vars = [
            'KAFKA_BROKER_INTERNAL', 'MONGO_HOST', 'MONGO_PORT', 
            'MONGO_DATABASE', 'SPARK_TRADE_TOPIC', 'S3_BUCKET'
        ]
        for var in required_vars:
            if not os.getenv(var):
                raise unittest.SkipTest(f"필수 환경 변수 '{var}'가 설정되지 않아 통합 테스트를 건너뜁니다.")

        cls.kafka_broker = os.getenv('KAFKA_BROKER_INTERNAL')
        cls.mongo_host = os.getenv('MONGO_HOST')
        cls.mongo_port = int(os.getenv('MONGO_PORT'))
        cls.mongo_db_name = os.getenv('MONGO_DATABASE')
        cls.topic_name = os.getenv('SPARK_TRADE_TOPIC')
        cls.s3_bucket = os.getenv('S3_BUCKET')

        cls.producer = KafkaProducer(
            bootstrap_servers=cls.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        cls.mongo_client = MongoClient(host=cls.mongo_host, port=cls.mongo_port)
        cls.s3 = s3fs.S3FileSystem()

    @classmethod
    def tearDownClass(cls):
        cls.producer.close()
        cls.mongo_client.close()

    def setUp(self):
        print(f"\n--- Running setUp for {self.id()} ---")
        db = self.mongo_client[self.mongo_db_name]
        collection = db[self.topic_name]
        print(f"'{self.topic_name}' 컬렉션의 모든 문서를 삭제합니다.")
        collection.delete_many({})
        
        stock_code = "035720"
        today_str = datetime.now().strftime("%Y-%m-%d")
        s3_test_path = f"{self.s3_bucket}/daily_report/stock_code={stock_code}/{today_str}.csv"
        if self.s3.exists(s3_test_path):
            self.s3.rm(s3_test_path)
        time.sleep(1)

    def test_mongo_loader_pipeline(self):
        """[mongo_load] Kafka -> MongoDB 파이프라인 통합 테스트"""
        print("\n--- [mongo_load] 통합 테스트 시작 ---")
        db = self.mongo_client[self.mongo_db_name]
        collection = db[self.topic_name]
        test_message = {"stck_prpr": "91000", "cntg_vol": "150", "acml_vol": "20000"}
        test_key = "005930"

        print(f"테스트 메시지를 '{self.topic_name}' 토픽으로 전송합니다.")
        self.producer.send(topic=self.topic_name, key=test_key.encode('utf-8'), value=test_message)
        self.producer.flush()

        print("loader가 데이터를 처리하도록 5초간 대기합니다...")
        time.sleep(5)

        print("MongoDB에 데이터가 올바르게 저장되었는지 검증합니다.")
        doc_count = collection.count_documents({'metadata.key': test_key})
        self.assertEqual(doc_count, 1, f"test_key '{test_key}'를 가진 문서가 1개가 아닙니다 (발견된 수: {doc_count}).")
        
        saved_doc = collection.find_one({'metadata.key': test_key})
        self.assertIsNotNone(saved_doc)
        self.assertEqual(json.loads(saved_doc['raw_data']), test_message)
        print("--- [mongo_load] 통합 테스트 성공 ---")
        
    def test_spark_reporter_pipeline(self):
        """[report_daily] Kafka -> Spark -> S3 파이프라인 통합 테스트"""
        print("\n--- [report_daily] 통합 테스트 시작 ---")
        spark_test_topic = self.topic_name
        stock_code = "035720"
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        s3_test_path = f"{self.s3_bucket}/daily_report/stock_code={stock_code}/{today_str}.csv"

        test_messages = [
            {"stck_prpr": "100000", "acml_vol": "10", "acml_tr_pbmn": "1000000", "cttr": "110", "total_askp_rsqn": "100", "total_bidp_rsqn": "120"},
            {"stck_prpr": "200000", "acml_vol": "20", "acml_tr_pbmn": "3000000", "cttr": "115", "total_askp_rsqn": "80", "total_bidp_rsqn": "150"}
        ]
        
        print(f"Spark 테스트용 메시지를 '{spark_test_topic}' 토픽으로 전송합니다.")
        for msg in test_messages:
            self.producer.send(topic=spark_test_topic, key=stock_code.encode('utf-8'), value=msg)
        self.producer.flush()

        print("Spark가 데이터를 처리하고 S3에 쓸 때까지 45초간 대기합니다...")
        time.sleep(45)

        print("S3에 리포트 파일이 올바르게 생성되었는지 검증합니다.")
        self.assertTrue(self.s3.exists(s3_test_path), f"S3에 리포트 파일({s3_test_path})이 생성되지 않았습니다.")

        with self.s3.open(s3_test_path, 'r') as f:
            report_df = pd.read_csv(f)
            latest_report = report_df.iloc[-1]
            expected_vwap = 3000000 / 20
            self.assertAlmostEqual(latest_report['vwap'], expected_vwap, places=2)
            self.assertEqual(float(latest_report['trade_strength']), 115.0)

        print("--- [report_daily] 통합 테스트 성공 ---")