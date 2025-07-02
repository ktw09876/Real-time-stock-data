import unittest
from decimal import Decimal
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from models.H0STCNT0_response import CheGyeolGa
from models.H0STANC0_response import ExpectedCheGyeolGa

"""
API 응답 파서 클래스들에 대한 단위 테스트
"""
class TestResponseParsers(unittest.TestCase):

    """
    [성공 케이스] H0STCNT0 (실시간 체결가) 데이터가 정상적으로 파싱되는지 테스트합니다.
    """
    def test_parse_h0stcnt0_success(self):
        
        # 1. 준비 (Arrange): API 명세에 맞는 H0STCNT0 샘플 데이터를 정의합니다.
        #                  _parse_h0stcnt0는 data_parts[1]부터 사용하므로, 첫 요소는 비워둡니다.
        data_parts = [
            "005930",       # [0] iscd (사용되지 않음)
            "090001",       # [1] stck_cntg_hour (주식 체결 시간)
            "85000",        # [2] stck_prpr (주식 현재가)
            "2",            # [3] prdy_vrss_sign (전일 대비 부호)
            "1200",         # [4] prdy_vrss (전일 대비)
            "1.43",         # [5] prdy_ctrt (전일 대비율)
            "85100.50",     # [6] wghn_avrg_stck_prc (가중 평균 주식 가격)
            "84000",        # [7] stck_oprc (주식 시가)
            "86000",        # [8] stck_hgpr (주식 최고가)
            "83000",        # [9] stck_lwpr (주식 최저가)
            "85000",        # [10] askp1 (매도호가1)
            "84900",        # [11] bidp1 (매수호가1)
            "100",          # [12] cntg_vol (체결 거래량)
            "1234567",      # [13] acml_vol (누적 거래량)
            "104938195000", # [14] acml_tr_pbmn (누적 거래 대금)
            "5000",         # [15] seln_cntg_csnu (매도 체결 건수)
            "6000",         # [16] shnu_cntg_csnu (매수 체결 건수)
            "1000",         # [17] ntby_cntg_csnu (순매수 체결 건수)
            "120.00",       # [18] cttr (체결강도)
            "500000",       # [19] seln_cntg_smtn (총 매도 수량)
            "600000",       # [20] shnu_cntg_smtn (총 매수 수량)
            "1",            # [21] ccld_dvsn (체결구분)
            "54.55",        # [22] shnu_rate (매수비율)
            "110.15",       # [23] prdy_vol_vrss_acml_vol_rate (전일 거래량 대비 등락율)
            "090000",       # [24] oprc_hour (시가 시간)
            "2",            # [25] oprc_vrss_prpr_sign (시가대비구분)
            "1000",         # [26] oprc_vrss_prpr (시가대비)
            "090500",       # [27] hgpr_hour (최고가 시간)
            "5",            # [28] hgpr_vrss_prpr_sign (고가대비구분)
            "-1000",        # [29] hgpr_vrss_prpr (고가대비)
            "090200",       # [30] lwpr_hour (최저가 시간)
            "2",            # [31] lwpr_vrss_prpr_sign (저가대비구분)
            "2000",         # [32] lwpr_vrss_prpr (저가대비)
            "20250702",     # [33] bsop_date (영업 일자)
            "2",            # [34] new_mkop_cls_code (신 장운영 구분 코드)
            "N",            # [35] trht_yn (거래정지 여부)
            "1500",         # [36] askp_rsqn1 (매도호가 잔량1)
            "2500",         # [37] bidp_rsqn1 (매수호가 잔량1)
            "15000",        # [38] total_askp_rsqn (총 매도호가 잔량)
            "25000",        # [39] total_bidp_rsqn (총 매수호가 잔량)
            "1.23",         # [40] vol_tnrt (거래량 회전율)
            "1100000",      # [41] prdy_smns_hour_acml_vol (전일 동시간 누적 거래량)
            "112.23",       # [42] prdy_smns_hour_acml_vol_rate (전일 동시간 누적 거래량 비율)
            "0",            # [43] hour_cls_code (시간 구분 코드)
            "00",           # [44] mrkt_trtm_cls_code (임의종료구분코드)
            "85500"         # [45] vi_stnd_prc (정적vi발동기준가)
        ]

        expected_output = CheGyeolGa(
            stck_cntg_hour='090001', stck_prpr=Decimal('85000'), prdy_vrss_sign='2',
            prdy_vrss=Decimal('1200'), prdy_ctrt=Decimal('1.43'),
            wghn_avrg_stck_prc=Decimal('85100.50'), stck_oprc=Decimal('84000'),
            stck_hgpr=Decimal('86000'), stck_lwpr=Decimal('83000'), askp1=Decimal('85000'),
            bidp1=Decimal('84900'), cntg_vol=Decimal('100'), acml_vol=Decimal('1234567'),
            acml_tr_pbmn=Decimal('104938195000'), seln_cntg_csnu=Decimal('5000'),
            shnu_cntg_csnu=Decimal('6000'), ntby_cntg_csnu=Decimal('1000'), cttr=Decimal('120.00'),
            seln_cntg_smtn=Decimal('500000'), shnu_cntg_smtn=Decimal('600000'), ccld_dvsn='1',
            shnu_rate=Decimal('54.55'), prdy_vol_vrss_acml_vol_rate=Decimal('110.15'),
            oprc_hour='090000', oprc_vrss_prpr_sign='2', oprc_vrss_prpr=Decimal('1000'),
            hgpr_hour='090500', hgpr_vrss_prpr_sign='5', hgpr_vrss_prpr=Decimal('-1000'),
            lwpr_hour='090200', lwpr_vrss_prpr_sign='2', lwpr_vrss_prpr=Decimal('2000'),
            bsop_date='20250702', new_mkop_cls_code='2', trht_yn='N',
            askp_rsqn1=Decimal('1500'), bidp_rsqn1=Decimal('2500'),
            total_askp_rsqn=Decimal('15000'), total_bidp_rsqn=Decimal('25000'),
            vol_tnrt=Decimal('1.23'), prdy_smns_hour_acml_vol=Decimal('1100000'),
            prdy_smns_hour_acml_vol_rate=Decimal('112.23'), hour_cls_code='0',
            mrkt_trtm_cls_code='00', vi_stnd_prc=Decimal('85500')
        )

        # 2. 실행 (Act)
        actual_output = CheGyeolGa._parse_h0stcnt0(data_parts)

        # 3. 단언 (Assert)
        self.assertEqual(actual_output, expected_output)

    """
    H0STANC0 (예상체결가) 데이터가 정상적으로 파싱되는지 테스트합니다.
    """
    def test_parse_h0stanc0_success(self):
        # 1. 준비: 테스트에 사용할 입력 데이터와 기대하는 결과값을 정의
        data_parts = [
            "005930",       # 유가증권단축종목코드
            "085959",       # 주식체결시간
            "85000",        # 예상체결가
            "2",            # 전일대비구분 (상승)
            "1200",         # 전일대비
            "1.43",         # 등락율
            "1234567",      # 누적거래량
            "500",          # 예상 체결 수량
            "12000",        # 총 매도호가 잔량
            "8000",         # 총 매수호가 잔량
            "90000",        # 상한가
            "70000",        # 하한가
            "84000",        # 시가
            "86000",        # 고가
            "83000"         # 저가
        ]

        expected_output = ExpectedCheGyeolGa(
            mksc_shrn_iscd="005930",
            stck_cntg_hour="085959",
            stck_prpr=Decimal("85000"),
            prdy_vrss_sign="2",
            prdy_vrss=Decimal("1200"),
            prdy_ctrt=Decimal("1.43"),
            acml_vol=Decimal("1234567"),
            antc_cnqn=Decimal("500"),
            total_askp_rsqn=Decimal("12000"),
            total_bidp_rsqn=Decimal("8000"),
            hshr_stprf=Decimal("90000"),
            lshr_stprf=Decimal("70000"),
            stck_oprc=Decimal("84000"),
            stck_hgpr=Decimal("86000"),
            stck_lwpr=Decimal("83000")
        )

        # 2. 실행 (Act): 테스트할 함수를 실제로 실행합니다.
        actual_output = ExpectedCheGyeolGa._parse_h0stanc0(data_parts)

        # 3. 단언 (Assert): 실행 결과가 기대하는 결과와 동일한지 확인합니다.
        self.assertEqual(actual_output, expected_output)

    """
    [엣지 케이스] H0STANC0 데이터에 빈 문자열이 포함되었을 때 0으로 처리되는지 테스트
    """
    def test_parse_h0stanc0_with_empty_values(self):
        # 1. 준비 (Arrange): 숫자 필드에 빈 문자열('')이 있는 입력 데이터를 정의합니다.
        data_parts = [
            "005930", "085959", "85000", "2", "1200", "", "1234567", "500",
            "12000", "8000", "90000", "70000", "84000", "86000", "83000"
        ]

        # 기대 결과: 빈 문자열이었던 등락율(prdy_ctrt)이 Decimal('0')으로 변환되어야 합니다.
        expected_output = ExpectedCheGyeolGa(
            mksc_shrn_iscd="005930",
            stck_cntg_hour="085959",
            stck_prpr=Decimal("85000"),
            prdy_vrss_sign="2",
            prdy_vrss=Decimal("1200"),
            prdy_ctrt=Decimal('0'), # <-- 이 부분이 0으로 처리되는지 확인
            acml_vol=Decimal("1234567"),
            antc_cnqn=Decimal("500"),
            total_askp_rsqn=Decimal("12000"),
            total_bidp_rsqn=Decimal("8000"),
            hshr_stprf=Decimal("90000"),
            lshr_stprf=Decimal("70000"),
            stck_oprc=Decimal("84000"),
            stck_hgpr=Decimal("86000"),
            stck_lwpr=Decimal("83000")
        )

        # 2. 실행 (Act)
        actual_output = ExpectedCheGyeolGa._parse_h0stanc0(data_parts)

        # 3. 단언 (Assert)
        self.assertEqual(actual_output, expected_output)

# 이 파일을 직접 실행할 경우, unittest를 실행합니다.
if __name__ == '__main__':
    unittest.main()