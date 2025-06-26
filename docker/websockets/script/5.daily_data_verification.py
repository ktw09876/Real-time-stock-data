# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime, timezone, timedelta
import pandas as pd
import s3fs
from pymongo import MongoClient

class DailyReconciler:
    """
    S3의 최종 집계 결과와 MongoDB의 전체 원본 집계 결과를 비교하여
    일일 데이터의 정합성을 최종 검증하는 클래스.
    """
    def __init__(self, target_date_str: str):
        self._test_env_var()

        # 1. 환경 변수에서 설정 로드
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.mongo_host = os.getenv('MONGO_HOST')
        self.mongo_port = int(os.getenv('MONGO_PORT'))
        self.mongo_db_name = os.getenv('MONGO_DATABASE')
        self.mongo_collection_name = os.getenv('SPARK_TOPIC')

        self.s3 = s3fs.S3FileSystem()
        self.target_date_str = target_date_str
        self.report_base_path = f"{self.s3_bucket}/daily_report"
        self.comparison_results = [] # 비교 결과를 저장할 리스트
        
        # MongoDB 클라이언트 초기화
        self.mongo_client = MongoClient(host=self.mongo_host, port=self.mongo_port)
        self.db = self.mongo_client[self.mongo_db_name]
        self.raw_data_collection = self.db[self.mongo_collection_name]

    """
    필수 환경 변수 확인
    """
    def _test_env_var(self):
        test_env_var = [
            'S3_BUCKET',
            'MONGO_HOST',
            'MONGO_PORT',
            'MONGO_DATABASE',
            'SPARK_TOPIC'
        ]

        missing_vars = []
        for var in test_env_var:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")

    def _get_s3_final_snapshot(self, stock_code: str):
        """S3 리포트 파일에서 가장 마지막 줄의 집계 결과를 가져옵니다."""
        s3_file_path = f"{self.report_base_path}/stock_code={stock_code}/{self.target_date_str}.csv"
        print(f"  - S3 데이터 로딩: {s3_file_path}")
        if not self.s3.exists(s3_file_path):
            print("    - S3 파일 없음.")
            return None
        try:
            with self.s3.open(s3_file_path, 'r') as f:
                df = pd.read_csv(f)
            
            return df.iloc[-1].to_dict() # 마지막 행을 딕셔너리로 변환하여 반환
        except Exception as e:
            print(f"    - S3 파일 읽기 오류: {e}")
            return None

    def _get_mongodb_final_batch_agg(self, stock_code: str):
        """MongoDB의 당일 전체 원본 데이터를 집계합니다."""
        print(f"  - MongoDB 전체 데이터 집계 시작: {stock_code}")
        KST = timezone(timedelta(hours=9))
        start_dt_kst = datetime.strptime(self.target_date_str, "%Y-%m-%d").replace(tzinfo=KST)
        end_dt_kst = start_dt_kst + timedelta(days=1)
        start_dt_utc = start_dt_kst.astimezone(timezone.utc)
        end_dt_utc = end_dt_kst.astimezone(timezone.utc)

        pipeline = [
            {'$match': {'metadata.key': stock_code, 'insert_time': {'$gte': start_dt_utc, '$lt': end_dt_utc}}},
            {'$project': {
                'fields': {'$split': [{'$arrayElemAt': [{'$split': ['$raw_data', '|']}, 3]}, '^']}
            }},
            {'$project': {
                'price': {'$toDouble': {'$arrayElemAt': ['$fields', 2]}},
                'volume': {'$toLong': {'$arrayElemAt': ['$fields', 12]}}
            }},
            {'$group': {
                '_id': None,
                'total_volume': {'$sum': '$volume'},
                'avg_price': {'$avg': '$price'}
            }}
        ]

        try:
            result = list(self.raw_data_collection.aggregate(pipeline))
            print(f"    - DB: {self.mongo_db_name}, Collection: {self.raw_data_collection.name}")
            if result:
                result[0]['avg_price'] = round(result[0]['avg_price'], 2)
                print(f"    - MongoDB 집계 완료: Volume={result[0]['total_volume']}")
                return result[0]
            return None
        except Exception as e:
            print(f"    - MongoDB 집계 오류: {e}")
            return None

    def run_reconciliation(self):
        """모든 종목에 대해 S3와 MongoDB의 최종 집계 결과를 비교합니다."""
        print(f"\n===== {self.target_date_str} 데이터 정합성 최종 검증 시작 =====")
        try:
            stock_dirs = self.s3.glob(f"{self.report_base_path}/stock_code=*")
            for stock_dir in stock_dirs:
                stock_code = stock_dir.split('=')[-1]
                print(f"\n- 검증 대상 종목: {stock_code}")
                
                s3_result = self._get_s3_final_snapshot(stock_code)
                mongo_result = self._get_mongodb_final_batch_agg(stock_code)

                print("    ------------------------------------------")
                print(f"    [S3 최종 결과]    : {s3_result}")
                print(f"    [MongoDB 전체 결과] : {mongo_result}")
                print("    ------------------------------------------")

                result_row = {'stock_code': stock_code}
                if s3_result and mongo_result: # 두 결과가 모두 있을 경우, 값을 비교
                    vol_match = s3_result['total_volume'] == mongo_result['total_volume']
                    # 소수점 오차를 고려하여 비교
                    price_match = abs(s3_result['avg_price'] - mongo_result['avg_price']) < 0.01

                    result_row['s3_volume'] = s3_result['total_volume']
                    result_row['mongo_volume'] = mongo_result['total_volume']
                    result_row['s3_avg_price'] = s3_result['avg_price']
                    result_row['mongo_avg_price'] = mongo_result['avg_price']
                    result_row['status'] = "적합 일치!!" if vol_match and price_match else "부적합 불일치!!"
                else:
                    result_row['status'] = "경고 데이터 누락"
                
                self.comparison_results.append(result_row)
        finally:
            self.mongo_client.close()
            self._generate_summary_report()
            print("===== 최종 검증 작업 종료 =====")
    
    def _generate_summary_report(self):
        """Airflow XCom으로 전달할 최종 요약 리포트를 생성합니다."""
        summary = f"""
        <h2>주식 데이터 파이프라인 일일 정합성 리포트 ({self.target_date_str})</h2>
        <hr>
        <p>장 마감 후, S3의 최종 스트리밍 집계 결과와 MongoDB의 전체 원본 배치 집계 결과를 비교한 내용입니다.</p>
        
        <h3>정합성 비교 결과:</h3>
        <table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; width: 100%;'>
            <tr style='background-color:#f2f2f2;'>
                <th>종목코드</th><th>S3 거래량</th><th>MongoDB 거래량</th><th>S3 평균가</th><th>MongoDB 평균가</th><th>상태</th>
            </tr>
        """
        if not self.comparison_results:
            summary += "<tr><td colspan='6' style='text-align:center;'>검증할 데이터가 없습니다.</td></tr>"
        else:
            for res in self.comparison_results:
                status_color = 'lightgreen' if res['status'] == " 일치" else 'lightcoral'
                summary += f"""
                <tr>
                    <td>{res.get('stock_code', 'N/A')}</td>
                    <td>{res.get('s3_volume', 'N/A')}</td>
                    <td>{res.get('mongo_volume', 'N/A')}</td>
                    <td>{res.get('s3_avg_price', 'N/A')}</td>
                    <td>{res.get('mongo_avg_price', 'N/A')}</td>
                    <td style='background-color:{status_color};'>{res['status']}</td>
                </tr>
                """
        summary += "</table><hr><p><em>* 이 메일은 Airflow에 의해 자동으로 발송되었습니다.</em></p>"
        # Airflow XCom으로 리포트 내용을 전달하기 위해 print() 사용
        print(summary)

if __name__ == "__main__":
    KST = timezone(timedelta(hours=9))
    target_date_arg = sys.argv[1] if len(sys.argv) > 1 else (datetime.now(KST) - timedelta(days=1)).strftime('%Y-%m-%d')
    
    reconciler = DailyReconciler(target_date_str=target_date_arg)
    reconciler.run_reconciliation()
