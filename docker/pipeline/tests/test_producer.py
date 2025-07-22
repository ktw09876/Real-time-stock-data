# -*- coding: utf-8 -*-
import os
import json
import time
from kafka import KafkaProducer

"""
테스트용 Kafka 메시지(JSON 문자열)를 생성
"""
def create_test_message(price, volume):
    message = {
        "stck_prpr": str(price),
        "cntg_vol": str(volume),
        "acml_vol": "10000",
        "acml_tr_pbmn": "100000000",
        "cttr": "100.00",
        "total_askp_rsqn": "5000",
        "total_bidp_rsqn": "5000"
    }
    return json.dumps(message).encode('utf-8')

"""
테스트 데이터를 Kafka로 전송
"""
def run_test():
    KAFKA_BROKER = os.getenv('KAFKA_BROKER_INTERNAL')
    TRADE_TOPIC = os.getenv('KAFKA_TOPICS')
    ANOMALY_TOPIC = os.getenv('ANOMALY_TOPIC')
    
    try:
        producer  =  KafkaProducer(bootstrap_servers = KAFKA_BROKER)
        print("Kafka Producer에 연결되었습니다.")
    except Exception as e:
        print(f"Kafka Producer 연결 실패: {e}")
        return

    # 테스트할 종목 코드
    test_stock_code  =  "005930".encode('utf-8') # 삼성전자

    # --- 테스트 시나리오 ---
    # 1. 정상 데이터 전송
    print("1. 정상 데이터 2건을 전송합니다...")
    producer.send(TRADE_TOPIC, key = test_stock_code, value = create_test_message(70000, 100))
    producer.send(TRADE_TOPIC, key = test_stock_code, value = create_test_message(70100, 120))
    producer.flush()
    print("   -> 전송 완료. 5초 대기...")
    time.sleep(5)

    # 2. 이상치 데이터 3건을 30초 이내에 전송
    #    - 최소가: 70200, 최대가: 80000 -> 변동률 13.9% (> 5%)
    #    - 평균가: 74400, 최대가: 80000 -> 변동률 7.5% (> 3%)
    print("2. 이상치 데이터 3건을 전송합니다...")
    producer.send(ANOMALY_TOPIC, key = test_stock_code, value = create_test_message(70200, 150))
    producer.send(ANOMALY_TOPIC, key = test_stock_code, value = create_test_message(73000, 500))
    producer.send(ANOMALY_TOPIC, key = test_stock_code, value = create_test_message(80000, 200))
    producer.flush()
    print("   -> 전송 완료.")
    
    producer.close()
    print("테스트 데이터 전송을 모두 완료했습니다.")

if __name__ == "__main__":
    run_test()
