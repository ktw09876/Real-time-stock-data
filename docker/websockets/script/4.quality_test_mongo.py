# -*- coding: utf-8 -*-
import os
import sys
import logging # 로깅 모듈 import
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import Union

class MongoRawDataValidator:
    """
    MongoDB에 저장된 원본(raw) 데이터의 품질을 검증하고, 그 결과를 로그 파일로 저장하는 클래스.
    - 문자열로 저장된 raw_data를 파싱하여 결측치 및 음수 값을 찾습니다.
    """
    def __init__(self, target_date_str: str):
        """초기화. 환경 변수에서 설정을 로드하고 로거 및 MongoDB를 설정합니다."""
        self._setup_logging(target_date_str)
        self._load_and_validate_env_vars()

        # MongoDB 클라이언트 초기화
        self.mongo_client = MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.db = self.mongo_client[self.mongo_db_name]
        self.collection = self.db[self.collection_name]
        
        self.logger.info("MongoDB 연결 설정 완료.")

    def _setup_logging(self, target_date_str: str):
        """로거를 설정하여 콘솔과 파일에 모두 출력되도록 합니다."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        # 이미 핸들러가 설정되어 있으면 중복 추가 방지
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # 콘솔 핸들러
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        
        # 파일 핸들러
        log_dir = os.getenv('LOG_PATH', '.')
        os.makedirs(log_dir, exist_ok = True) # 로그 디렉토리가 존재하지 않으면 생성
        log_file_name = f"mongo_test_{target_date_str}.log"
        log_file_path = os.path.join(log_dir, log_file_name)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _load_and_validate_env_vars(self):
        """필수 환경 변수를 로드하고 검증합니다."""
        required_vars = [
            'MONGO_HOST', 'MONGO_PORT', 'MONGO_DATABASE', 'SPARK_TOPIC'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

        self.mongo_host = os.getenv('MONGO_HOST')
        self.mongo_port = int(os.getenv('MONGO_PORT'))
        self.mongo_db_name = os.getenv('MONGO_DATABASE')
        self.collection_name = os.getenv('SPARK_TOPIC')

    """
    지정된 날짜의 데이터에서 결측치 또는 음수 값을 가진 값 탐지
    """
    def find_anomalies(self, target_date_str: str):
        self.logger.info(f"===== {target_date_str} 날짜의 원본 데이터 이상치 검사 시작 =====")
        self.logger.info(f"대상 컬렉션: {self.collection_name}")

        try:
            KST = timezone(timedelta(hours = 9))
            start_dt_kst = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=KST)
            end_dt_kst = start_dt_kst + timedelta(days=1)
            start_dt_utc = start_dt_kst.astimezone(timezone.utc)
            end_dt_utc = end_dt_kst.astimezone(timezone.utc)

            pipeline = [
                {'$match': {'insert_time': {'$gte': start_dt_utc, '$lt': end_dt_utc}}},
                {'$project': {
                    'raw_data': 1, 
                    'insert_date': 1,
                    'parsed_fields': {'$split': [{'$arrayElemAt': [{'$split': ['$raw_data', '|']}, 3]}, '^']}
                }},
                {'$project': {
                    'raw_data': 1, 
                    'insert_date': 1,
                    'price': {'$convert': {'input': {'$arrayElemAt': ['$parsed_fields', 2]}, 'to': 'double', 'onError': "값 없음", 'onNull': "값 없음"}},
                    'volume': {'$convert': {'input': {'$arrayElemAt': ['$parsed_fields', 12]}, 'to': 'long', 'onError': "값 없음", 'onNull': "값 없음"}}
                }},
                {'$match': {'$or': [{'price': "값 없음"}, {'volume': "값 없음"}, {'price': {'$lt': 0}}, {'volume': {'$lt': 0}}]}}
            ]

            anomalous_docs = list(self.collection.aggregate(pipeline))

            self.logger.info("===== 검사 결과 =====")
            if not anomalous_docs:
                self.logger.info(f" {target_date_str} 날짜의 데이터에서 이상치를 찾지 못했습니다.")
            else:
                self.logger.warning(f" 총 {len(anomalous_docs)}개의 이상 데이터를 발견했습니다:")
                for doc in anomalous_docs:
                    price = doc.get('price', 'N/A')
                    volume = doc.get('volume', 'N/A')
                    time_str = doc.get('insert_date', 'N/A')

                    self.logger.warning(f"  - Document ID: {doc['_id']}, Insert Time (KST): {time_str}")
                    self.logger.warning(f"    - Parsed Values: Price = {price}, Volume = {volume}")
                    self.logger.warning(f"    - Raw Data: \"{doc['raw_data']}\"")
                    print("")
            self.logger.info("=====================")
            
        except Exception as e:
            self.logger.error(f"검증 중 오류 발생: {e}", exc_info=True)
        finally:
            self.mongo_client.close()
            self.logger.info("MongoDB 연결이 종료되었습니다.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        KST = timezone(timedelta(hours=9))
        target_date = (datetime.now(KST)).strftime('%Y-%m-%d') #오늘 날짜를 기본값으로
        
    try:
        validator = MongoRawDataValidator(target_date_str = target_date)
        validator.find_anomalies(target_date_str = target_date)
    except ValueError as e:
        # 로거가 생성되기 전이므로 print 사용
        print(f"스크립트 실행 실패: {e}")
