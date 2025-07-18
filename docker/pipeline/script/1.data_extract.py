# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))
import logging
import json
import time
import requests
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal

from models.H0STCNT0_response import CheGyeolGa
# from models.H0STASP0_response import HoGa
# from models.H0STANC0_response import ExpectedCheGyeolGa

"""
한국투자증권 WebSocket API로부터 실시간 주식 데이터를 수신하여
원본(raw) 데이터 그대로 Kafka로 전송하는 Producer 클래스
    - 접속 토큰(Approval Key) 자동 갱신
"""
class KisApiProducer:
    """
    클래스 초기화. 환경 변수에서 설정을 로드하고 Kafka Producer를 생성
    """
    def __init__(self): 
        # 0. 환경 변수 확인
        self._test_env_var()

        # 1. .env에서 설정 값 로드
        self.APP_KEY = os.getenv('KIS_APP_KEY')
        self.APP_SECRET = os.getenv('KIS_APP_SECRET')
        self.BASE_URL = os.getenv('KIS_BASE_URL') # 모의투자 기본 URL
        self.WS_URL = os.getenv('KIS_WS_URL') # 모의투자 웹소켓 URL
        self.KAFKA_BROKER = os.getenv('KAFKA_BROKER_INTERNAL')
        kafka_topics = os.getenv('KAFKA_TOPICS') # 구독할  목록
        self.topics = []
        for tr_id in kafka_topics.split(','):
            self.topics.append(tr_id.strip())

        kafka_stock_codes = os.getenv('KAFKA_STOCK_CODES') # 구독할 종목 코드 목록
        self.stock_codes = []
        for code in kafka_stock_codes.split(','):
            self.stock_codes.append(code.strip())

        # 2 로그 세팅
        self._setup_logging()

        # 3. Kafka Producer 및 토큰 정보 초기화
        self.producer = self._init_kafka()
        self.approval_key = None
        self.key_issued_time = None

    """
    로거
    """
    def _setup_logging(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.hasHandlers():
            logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    def _json_default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if is_dataclass(obj):
            return asdict(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    """
    필수 환경 변수가 설정되었는지 확인
    """
    def _test_env_var(self):
        test_env_var = [
            'KIS_APP_KEY',
            'KIS_APP_SECRET',
            'KIS_BASE_URL',
            'KIS_WS_URL',
            'KAFKA_BROKER_INTERNAL',
            'KAFKA_TOPICS',
            'KAFKA_STOCK_CODES'
        ]

        missing_vars = []
        for var in test_env_var:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    """
    Kafka Producer를 초기화하고 반환
    """
    def _init_kafka(self):
        try:
            producer = KafkaProducer(
                bootstrap_servers = self.KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=self._json_default).encode('utf-8'),
                acks='all',
                retries=5
            )
            self.logger.info(f"Kafka Producer 초기화 성공 (Broker: {self.KAFKA_BROKER})")
            return producer
        except Exception as e:
            self.logger.error(f"Kafka Producer 초기화 실패: {e}")
            return None

    """
    신규 접속 토큰(Approval Key)을 발급
    """
    def _get_approval_key(self):
        print("신규 Approval Key 발급을 시도합니다...")
        headers = {
            "content-type": "application/json"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": self.APP_KEY,
            "secretkey": self.APP_SECRET
        }
        url = f"{self.BASE_URL}/oauth2/Approval"

        try:
            time.sleep(0.1)
            response = requests.post(url, headers = headers, data = json.dumps(body))
            response.raise_for_status() # HTTP 오류 발생 시 예외 발생
            res = response.json()
            
            key = res.get('approval_key')
            if key:
                print(f"Approval Key 발급 성공")
                self.approval_key = key
                self.key_issued_time = datetime.now()
            else:
                print(f"Approval Key 발급 실패: {res}")
        except Exception as e:
            print(f"Approval Key 발급 중 예외 발생: {e}")

    """
    현재 보유한 토큰이 유효한지 확인
    """
    def _is_key_valid(self):
        if not self.approval_key or not self.key_issued_time:
            return False
        
        # 유효 시간을 24시간보다 짧게 설정하여 만료 전에 갱신하도록 유도
        if datetime.now() > self.key_issued_time + timedelta(hours = 23):
            print("Approval Key가 만료되었습니다. 갱신이 필요합니다.")
            return False
        
        return True

    """
    토큰의 유효성을 검사하고, 만료된 경우 새로 발급
    """
    def _ensure_valid_key(self):    
        if not self._is_key_valid():
            self._get_approval_key()

    """
    구독할 종목들 리스트 생성
    """
    def _build_subscribe_requests(self):
        senddata_list = []

        for tr_id in self.topics:
            for code in self.stock_codes:
                temp_data = {
                    "header": {
                        "approval_key": self.approval_key,
                        "custtype": "P",
                        "tr_type": "1", # 실시간 등록
                        "content-type": "utf-8"
                    },
                    "body": {
                        "input": {
                            "tr_id": tr_id,
                            "tr_key": code
                        }
                    }
                }
                senddata_list.append(json.dumps(temp_data))
        return senddata_list

    """
    웹소켓에 접속하여 데이터를 받아 Kafka로 전송
    """
    async def run(self):
        if not self.producer:
            raise ConnectionError("Kafka Producer가 초기화되지 않아 실행을 중단합니다.")
        
        # 1. 유효한 토큰 확보
        self._ensure_valid_key()
        if not self.approval_key:
            raise ConnectionError("유효한 Approval Key를 발급받지 못했습니다.")

        # 2. 구독 요청 생성
        subscribe_requests = self._build_subscribe_requests() 

        # 3. 웹소켓 연결 및 데이터 수신/전송
        async with websockets.connect(self.WS_URL, ping_interval=60) as websocket:
            print(f"WebSockets 연결 성공 ({self.WS_URL})")
            
            # 구독 요청 전송
            for request in subscribe_requests:
                await websocket.send(request)

            print(f"{len(self.stock_codes)}개 종목에 대한 {len(subscribe_requests)}개 구독 요청 완료.")

            while True:
                data = await websocket.recv()

                # 실시간 시세 데이터 (암호화되지 않은 일반 데이터)
                if data and data[0] == '0': # 실시간 데이터
                    recvstr = data.split('|')
                    tr_id = recvstr[1]
                    raw_payload = recvstr[3].split('^')
                    stock_code = raw_payload[0]
                    message_object = None
                    
                    if tr_id == 'H0STCNT0':
                        message_object = CheGyeolGa._parse_h0stcnt0(raw_payload)

                    # elif tr_id == 'H0STASP0':
                    #     message_object = HoGa._parse_h0stasp0(raw_payload)

                    # elif tr_id == 'H0STANC0':
                    #     message_object = ExpectedCheGyeolGa._parse_h0stanc0(raw_payload)

                    else:
                        message_object = "확인필요"

                    if message_object:
                        # 객체를 JSON으로 직렬화하여 Kafka로 전송
                        self.producer.send(topic = tr_id, key = stock_code.encode('utf-8'), value = message_object)


                # PINGPONG 또는 응답 메시지
                elif data and data[0] == '{':
                    body = json.loads(data)
                    if body.get('header', {}).get('tr_id') == 'PINGPONG':
                        await websocket.pong(data)
                    else:
                        print(f"Received Info/Response: {data}")
                else:
                    print(f"Unknown Data Type Received: {data}")


"""
애플리케이션의 메인 진입점. 무한 루프를 돌며 재연결을 관리
"""
async def main():
    try:
        producer_app = KisApiProducer()
    except (ValueError, ConnectionError) as e:
        print(f"초기화 실패: {e}. 프로그램을 종료합니다.")
        return

    while True:
        try:
            await producer_app.run()
        except ConnectionClosed as e:
            print(f"웹소켓 연결이 종료되었습니다 (Code: {e.code}). 재연결을 시도합니다.")
        except Exception as e:
            print(f"메인 루프에서 예기치 않은 오류 발생: {e.__class__.__name__}: {e}")
        
        print("5초 후 재연결을 시도합니다...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n사용자에 의해 프로그램이 종료되었습니다.")
