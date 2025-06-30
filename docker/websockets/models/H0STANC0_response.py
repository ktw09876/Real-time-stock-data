from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from typing import List

"""
문자열을 Decimal로 변환, 
빈 문자열이면 0을 반환
"""
def _to_decimal(value: str) -> Decimal:
    if value.strip():
        return Decimal(value)
    else:
        return Decimal('0')

@dataclass
class ExpectedCheGyeolGa: # 클래스 이름을 더 명확하게 변경
    mksc_shrn_iscd: str           # 유가증권단축종목코드
    stck_cntg_hour: str           # 주식체결시간
    stck_prpr: Decimal            # 예상체결가
    prdy_vrss_sign: str           # 전일대비구분
    prdy_vrss: Decimal            # 전일대비
    prdy_ctrt: Decimal            # 등락율
    acml_vol: Decimal             # 누적거래량
    antc_cnqn: Decimal            # 예상 체결 수량
    total_askp_rsqn: Decimal      # 총 매도호가 잔량
    total_bidp_rsqn: Decimal      # 총 매수호가 잔량
    hshr_stprf: Decimal           # 상한가
    lshr_stprf: Decimal           # 하한가
    stck_oprc: Decimal            # 시가
    stck_hgpr: Decimal            # 고가
    stck_lwpr: Decimal            # 저가

    """
    API 원시 데이터(raw_data)의 payload 부분을 split한 리스트를 받아,
    ExpectedTradeData 클래스 인스턴스를 생성하여 반환합니다.
    
    한국투자증권 API 명세에 따라 각 인덱스의 데이터를 파싱합니다.
    """
    @classmethod
    def _parse_h0stanc0(cls, data_parts: List[str]):
        return cls(
            mksc_shrn_iscd=data_parts[0],
            stck_cntg_hour=data_parts[1],
            stck_prpr=_to_decimal(data_parts[2]),
            prdy_vrss_sign=data_parts[3],
            prdy_vrss=_to_decimal(data_parts[4]),
            prdy_ctrt=_to_decimal(data_parts[5]),
            acml_vol=_to_decimal(data_parts[6]),
            antc_cnqn=_to_decimal(data_parts[7]),
            total_askp_rsqn=_to_decimal(data_parts[8]),
            total_bidp_rsqn=_to_decimal(data_parts[9]),
            hshr_stprf=_to_decimal(data_parts[10]),
            lshr_stprf=_to_decimal(data_parts[11]),
            stck_oprc=_to_decimal(data_parts[12]),
            stck_hgpr=_to_decimal(data_parts[13]),
            stck_lwpr=_to_decimal(data_parts[14])
        )