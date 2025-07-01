import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

def check_latency():
    """
    MongoDB의 최신 데이터 지연 시간을 확인하고,
    정상이면 종료 코드 0, 비정상이면 1을 반환하는 함수.
    """
    # --- 로거 설정 ---
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        stream=sys.stdout)

    # --- 환경 변수 로드 ---
    MONGO_HOST = os.getenv('MONGO_HOST')
    MONGO_PORT = int(os.getenv('MONGO_PORT'))
    MONGO_DB_NAME = os.getenv('MONGO_DATABASE')
    COLLECTION_NAME = os.getenv('SPARK_TRADE_TOPIC')

    if not all([MONGO_HOST, MONGO_DB_NAME, COLLECTION_NAME]):
        logging.error("MongoDB 연결에 필요한 환경 변수가 설정되지 않았습니다.")
        sys.exit(1)

    client = None
    try:
        logging.info(f"MongoDB에 연결 시도: {MONGO_HOST}:{MONGO_PORT}")
        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logging.info("MongoDB 연결에 성공했습니다.")
        
        db = client[MONGO_DB_NAME]
        collection = db[COLLECTION_NAME]
        
        KST = timezone(timedelta(hours=9))
        today_start_kst = datetime.now(KST).replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start_kst.astimezone(timezone.utc)

        latest_doc = collection.find_one(
            {'insert_time': {'$gte': today_start_utc}},
            sort=[('insert_time', -1)]
        )
        
        if not latest_doc:
            logging.warning(f"오늘({today_start_kst.strftime('%Y-%m-%d')})의 데이터가 아직 수집되지 않았습니다.")
            sys.exit(1)

        # 몽고DB 데이터와 현재 시간 비교 ing...
        last_insert_time = latest_doc.get('insert_time').replace(tzinfo=timezone.utc)
        current_time_utc = datetime.now(timezone.utc)
        time_diff_seconds = (current_time_utc - last_insert_time).total_seconds()
        
        logging.info(f"마지막 데이터 저장 시간: {last_insert_time}")
        logging.info(f"시간 차이: {time_diff_seconds:.2f} 초")

        latency_threshold = 300 # 5분

        if time_diff_seconds < latency_threshold:
            logging.info(f"데이터 수집이 정상입니다 (지연 시간 < {latency_threshold}초).")
            sys.exit(0) # 성공 시 종료 코드 0
        else:
            logging.error(f"경고: 데이터 수집이 {latency_threshold}초 이상 지연되었습니다.")
            sys.exit(1) # 실패 시 종료 코드 1

    except Exception as e:
        logging.error(f"오류 발생: {e}")
        sys.exit(1) # 에러 발생 시 종료 코드 1
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    check_latency()