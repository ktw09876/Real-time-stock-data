# -*- coding: utf-8 -*-
import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import Union, List

class MongoDataValidator:
    """
    MongoDB에 저장된 구조화된 데이터의 품질을 검증하고,
    그 결과를 로그 파일로 저장하는 클래스.
    - 결측치/음수, 논리적 일관성, 값 범위, 카테고리 값 유효성 검증
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
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        
        log_dir = os.getenv('LOG_PATH', '.')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file_name = f"mongo_validation_{target_date_str}.log"
        log_file_path = os.path.join(log_dir, log_file_name)
        
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        self.logger.info(f"로그 파일이 '{log_file_path}' 경로에 저장됩니다.")

    def _load_and_validate_env_vars(self):
        """필수 환경 변수를 로드하고 검증합니다."""
        required_vars = [
            'MONGO_HOST', 'MONGO_PORT', 'MONGO_DATABASE', 'KAFKA_PROCESS_TOPIC'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

        self.mongo_host = os.getenv('MONGO_HOST')
        self.mongo_port = int(os.getenv('MONGO_PORT'))
        self.mongo_db_name = os.getenv('MONGO_DATABASE')
        # .env 파일과 변수 이름을 일치시킵니다.
        self.collection_name = os.getenv('KAFKA_PROCESS_TOPIC')

    def find_anomalies(self, target_date_str: str):
        """지정된 날짜의 데이터에서 다양한 유형의 이상 데이터를 찾습니다."""
        self.logger.info(f"===== {target_date_str} 날짜의 데이터 이상치 검사 시작 =====")
        self.logger.info(f"대상 DB: {self.mongo_db_name}, 컬렉션: {self.collection_name}")

        try:
            KST = timezone(timedelta(hours=9))
            start_dt_kst = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=KST)
            end_dt_kst = start_dt_kst + timedelta(days=1)
            start_dt_utc = start_dt_kst.astimezone(timezone.utc)
            end_dt_utc = end_dt_kst.astimezone(timezone.utc)

            # [수정] 구조화된 JSON 데이터를 검사하도록 파이프라인 재설계
            pipeline = [
                {'$match': {'insert_time': {'$gte': start_dt_utc, '$lt': end_dt_utc}}},
                {
                    # 1. 필요한 필드를 추출하고 안전하게 타입 변환
                    '$project': {
                        'payload': 1, 'insert_date': 1,
                        'price': {'$convert': {'input': '$payload.stck_prpr', 'to': 'double', 'onError': None, 'onNull': None}},
                        'volume': {'$convert': {'input': '$payload.cntg_vol', 'to': 'long', 'onError': None, 'onNull': None}},
                        'high_price': {'$convert': {'input': '$payload.stck_hgpr', 'to': 'double', 'onError': None, 'onNull': None}},
                        'low_price': {'$convert': {'input': '$payload.stck_lwpr', 'to': 'double', 'onError': None, 'onNull': None}},
                        'prdy_vrss_sign': '$payload.prdy_vrss_sign'
                    }
                },
                # 2. 각 검증 규칙에 대한 결과를 boolean 필드로 추가
                {
                    '$addFields': {
                        'validation_results': {
                            'is_missing': {'$or': [{'$eq': ['$price', None]}, {'$eq': ['$volume', None]}]},
                            'is_negative': {'$or': [{'$lt': ['$price', 0]}, {'$lt': ['$volume', 0]}]},
                            'is_hilo_inverted': {'$lt': ['$high_price', '$low_price']},
                            'is_price_out_of_range': {'$or': [{'$lt': ['$price', '$low_price']}, {'$gt': ['$price', '$high_price']}]},
                            'is_sign_invalid': {'$not': {'$in': ['$prdy_vrss_sign', ['1', '2', '3', '4', '5']]}}
                        }
                    }
                },
                # 3. 하나라도 문제가 있는 경우만 필터링
                {
                    '$match': {
                        '$or': [
                            {'validation_results.is_missing': True},
                            {'validation_results.is_negative': True},
                            {'validation_results.is_hilo_inverted': True},
                            {'validation_results.is_price_out_of_range': True},
                            {'validation_results.is_sign_invalid': True}
                        ]
                    }
                }
            ]

            anomalous_docs = list(self.collection.aggregate(pipeline))

            self.logger.info("===== 검사 결과 =====")
            if not anomalous_docs:
                self.logger.info(f"데이터에서 이상치를 찾지 못했습니다. ({target_date_str})")
            else:
                self.logger.warning(f"총 {len(anomalous_docs)}개의 이상 데이터를 발견했습니다:")
                for doc in anomalous_docs:
                    validation = doc.get('validation_results', {})
                    issues = [k for k, v in validation.items() if v]
                    
                    self.logger.warning(f"  - Document ID: {doc['_id']}, Insert Time: {doc.get('insert_date', 'N/A')}")
                    self.logger.warning(f"    - 문제 유형: {', '.join(issues)}")
                    self.logger.warning(f"    - Payload Data: {doc.get('payload')}")
                    self.logger.warning("")

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
        target_date = (datetime.now(KST) - timedelta(days=1)).strftime('%Y-%m-%d')
        
    try:
        validator = MongoDataValidator(target_date_str=target_date)
        validator.find_anomalies(target_date_str=target_date)
    except ValueError as e:
        print(f"스크립트 실행 실패: {e}")
