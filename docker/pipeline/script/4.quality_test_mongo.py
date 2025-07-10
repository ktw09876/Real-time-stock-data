# -*- coding: utf-8 -*-
import os
import sys
import logging
import json # JSON 파싱을 위해 import
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient

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
        
        log_file_name = f"test_mongo_{target_date_str}.log"
        log_file_path = os.path.join(log_dir, log_file_name)
        
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        self.logger.info(f"로그 파일이 '{log_file_path}' 경로에 저장됩니다.")

    def _load_and_validate_env_vars(self):
        """필수 환경 변수를 로드하고 검증합니다."""
        required_vars = [
            'MONGO_HOST', 'MONGO_PORT', 'MONGO_DATABASE', 'SPARK_TRADE_TOPIC'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

        self.mongo_host = os.getenv('MONGO_HOST')
        self.mongo_port = int(os.getenv('MONGO_PORT'))
        self.mongo_db_name = os.getenv('MONGO_DATABASE')
        # .env 파일과 변수 이름을 일치시킵니다.
        self.collection_name = os.getenv('SPARK_TRADE_TOPIC')

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

            # 파이프라인에서 JSON 파싱 단계를 제거하고, 필요한 모든 데이터를 가져옵니다.
            pipeline = [
                {'$match': {'insert_time': {'$gte': start_dt_utc, '$lt': end_dt_utc}}},
                {
                    '$project': {
                        'raw_data': {'$ifNull': ['$payload', '$raw_data']}, # payload 또는 raw_data 필드를 사용
                        'insert_date': 1
                    }
                }
            ]

            all_docs = list(self.collection.aggregate(pipeline))
            anomalous_docs = []


            for doc in all_docs:
                try:
                    data_source = doc.get('raw_data')
                    if isinstance(data_source, str):
                        payload = json.loads(data_source)
                    elif isinstance(data_source, dict):
                        payload = data_source
                    else:
                        raise TypeError("Data is not in a recognizable format (dict or json string)")
                    
                    price_str = payload.get('stck_prpr')
                    volume_str = payload.get('cntg_vol')
                    high_price_str = payload.get('stck_hgpr')
                    low_price_str = payload.get('stck_lwpr')
                    sign = payload.get('prdy_vrss_sign')

                    price = float(price_str) if price_str and price_str.strip() else None
                    volume = int(volume_str) if volume_str and volume_str.strip() else None
                    high_price = float(high_price_str) if high_price_str and high_price_str.strip() else None
                    low_price = float(low_price_str) if low_price_str and low_price_str.strip() else None

                    issues = []

                    if price is None:
                        issues.append(f"is_missing(stck_prpr: {price_str})")
                    elif price < 0:
                        issues.append(f"is_negative(stck_prpr: {price})")
                    
                    if volume is None:
                        issues.append(f"is_missing(cntg_vol: {volume_str})")
                    elif volume < 0:
                        issues.append(f"is_negative(cntg_vol: {volume})")

                    if high_price is not None and low_price is not None:
                        if high_price < low_price:
                            issues.append(f"is_hilo_inverted(high: {high_price}, low: {low_price})")
                        if price is not None and (price < low_price or price > high_price):
                             issues.append(f"is_price_out_of_range(price: {price}, low: {low_price}, high: {high_price})")
                    
                    if sign not in ['1', '2', '3', '4', '5']:
                        issues.append(f"is_sign_invalid(prdy_vrss_sign: {sign})")
                    
                    if issues:
                        doc['issues'] = issues
                        anomalous_docs.append(doc)

                except (json.JSONDecodeError, TypeError, KeyError) as e:
                    self.logger.error(f"Document ID {doc['_id']} 파싱 오류: {e}")
                    doc['issues'] = ['parsing_error']
                    anomalous_docs.append(doc)

            self.logger.info("===== 검사 결과 =====")
            if not anomalous_docs:
                self.logger.info(f"데이터에서 이상치를 찾지 못했습니다. ({target_date_str})")
            else:
                self.logger.warning(f"총 {len(anomalous_docs)}개의 이상 데이터를 발견했습니다:")
                for doc in anomalous_docs:
                    self.logger.warning(f"  - Document ID: {doc['_id']}, Insert Time: {doc.get('insert_date', 'N/A')}")
                    self.logger.warning(f"    - 문제 유형: {', '.join(doc.get('issues', []))}")
                    self.logger.warning(f"    - Raw Data: {doc.get('raw_data')}")
                    self.logger.warning("")

            self.logger.info("=====================")
            
        except Exception as e:
            self.logger.error(f"검증 중 오류 발생: {e}", exc_info=True)
        finally:
            self.mongo_client.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        KST = timezone(timedelta(hours=9))
        target_date = (datetime.now(KST)).strftime('%Y-%m-%d')
        
    try:
        validator = MongoDataValidator(target_date_str=target_date)
        validator.find_anomalies(target_date_str=target_date)
    except ValueError as e:
        print(f"스크립트 실행 실패: {e}")

