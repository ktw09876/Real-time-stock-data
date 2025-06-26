# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient

"""
Kafka 토픽에서 원본 데이터를 구독하여 MongoDB에 저장하는 클래스.
    - MongoDB 네이티브 날짜 타입 사용
"""
class MongoLoader:
    """
    클래스 초기화. 환경 변수에서 설정을 로드하고 MongoDB 및 Kafka 연결을 생성
        :param batch_size: MongoDB에 한 번에 쓸 최대 문서 수
        :param flush_interval: 배치 크기에 도달하지 않더라도 데이터를 쓸 시간 간격(초)
    """
    def __init__(self):
        # 0. 환경 변수 확인
        self._test_env_var()

        # 1. 환경 변수에서 설정 로드
        self.KAFKA_BROKER = os.getenv('KAFKA_BROKER_INTERNAL')
        self.MONGO_HOST = os.getenv('MONGO_HOST')
        self.MONGO_PORT = int(os.getenv('MONGO_PORT'))
        self.MONGO_DB_NAME = os.getenv('MONGO_DATABASE')
        
        kafka_topics = os.getenv('KAFKA_TOPICS') # 구독할  목록
        self.topics = []
        for tr_id in kafka_topics.split(','):
            self.topics.append(tr_id.strip())
        
        # 2. MongoDB 및 Kafka 클라이언트 초기화
        self.mongo_client, self.db = self._init_mongo()
        self.consumer = self._init_kafka()

        # 3. 배치 처리를 위한 설정
        self.batch_size = 100 # MongoDB에 한 번에 쓸 문서(데이터) 개수
        self.flush_interval = 5 # 데이터를 쓸 시간 간격, 배치(기본 100개)가 다 안 차더라도 실행
        self.buffer = []
        self.last_flush_time = time.time()

    """
    필수 환경 변수 확인
    """
    def _test_env_var(self):
        test_env_var = [
            'KAFKA_BROKER_INTERNAL',
            'MONGO_HOST',
            'MONGO_PORT',
            'MONGO_DATABASE',
            'KAFKA_TOPICS'
        ]

        missing_vars = []
        for var in test_env_var:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    """
    MongoDB에 연결하고 클라이언트와 데이터베이스 객체를 반환
    """
    def _init_mongo(self):
        try:
            client = MongoClient(host = self.MONGO_HOST, port = self.MONGO_PORT)
            db = client[self.MONGO_DB_NAME]
            print(f"MongoDB 연결 성공 (Host: {self.MONGO_HOST}, DB: {self.MONGO_DB_NAME})")
            return client, db
        except Exception as e:
            print(f"MongoDB 연결 실패: {e}")
            return None, None

    """
    Kafka Consumer를 초기화하고 객체를 반환
    """
    def _init_kafka(self):
        try:
            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.KAFKA_BROKER,
                group_id = 'mongo-raw-loader-group',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: v.decode('utf-8')
            )
            print(f"Kafka Consumer 초기화 성공. 구독 토픽: {self.topics}")
            return consumer
        except Exception as e:
            print(f"Kafka Consumer 초기화 실패: {e}")
            return None

    """
    버퍼에 쌓인 데이터를 MongoDB에 저장
    """
    def _flush_buffer(self):
        if not self.buffer:
            return
        
        try:
            # 토픽별로 문서를 그룹화
            docs_by_collection = {}
            for doc in self.buffer:
                collection_name = doc.pop("target_collection")
                if collection_name not in docs_by_collection:
                    docs_by_collection[collection_name] = []
                docs_by_collection[collection_name].append(doc)
            
            # 각 컬렉션에 대해 배치 삽입 실행
            for collection_name, docs in docs_by_collection.items():
                self.db[collection_name].insert_many(docs)
            
            print(f"MongoDB에 {len(self.buffer)}개 문서 저장 완료.")
            self.buffer.clear()
            self.last_flush_time = time.time()

        except Exception as e:
            print(f"MongoDB 배치 저장 중 오류 발생: {e}")

    """
    Kafka로부터 메시지를 읽어와 MongoDB에 저장
    """
    def run(self):
        if self.consumer is None or self.db is None:
            print("초기화 실패로 실행을 중단합니다.")
            return

        print("MongoDB 원본 데이터 적재를 시작합니다...")
        try:
            for message in self.consumer:
                try:
                    KST = timezone(timedelta(hours=9))
                    kafka_dt = datetime.fromtimestamp(message.timestamp / 1000)
                    insert_dt = datetime.now(KST)

                    # MongoDB에 저장할 문서 생성
                    document_to_save = {
                        "target_collection": message.topic, # 저장 후 삭제될 임시 필드
                        "raw_data": message.value,
                        "metadata": {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "key": message.key.decode('utf-8') if message.key else None,
                        },
                        # 몽고DB 네이티브
                        "kafka_timestamp": kafka_dt,
                        "insert_time": insert_dt,

                        # 2. 사람을 위한 YYYY-MM-DD 형식의 문자열
                        "insert_date": insert_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    self.buffer.append(document_to_save)
                    
                    # 버퍼가 가득 찼거나, 일정 시간이 지나면 flush
                    time_since_flush = time.time() - self.last_flush_time
                    if len(self.buffer) >= self.batch_size or time_since_flush >= self.flush_interval:
                        self._flush_buffer()

                except Exception as e:
                    print(f"메시지 처리 중 오류 발생: {e}, 데이터: {message.value}")

        except KeyboardInterrupt:
            print("사용자에 의해 데이터 적재가 중단되었습니다.")
        finally:
            print("남아있는 버퍼 데이터를 저장합니다...")
            self._flush_buffer() # 종료 전 버퍼에 남은 데이터 모두 저장
            if self.consumer: self.consumer.close()
            if self.mongo_client: self.mongo_client.close()
            print("Kafka Consumer 및 MongoDB 연결이 종료되었습니다.")

if __name__ == "__main__":
    try:
        loader_app = MongoLoader()
        loader_app.run()
    except Exception as e:
        print(f"Loader 애플리케이션 실행 실패: {e}")
