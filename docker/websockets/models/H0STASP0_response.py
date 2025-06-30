from dataclasses import dataclass
from typing import Optional
from decimal import Decimal
from typing import List

"""
국내 주식 실시간 호가(H0STASP0)
"""
"""
문자열을 Decimal로 변환
빈 문자열이면 0을 반환
"""
def _to_decimal(value: str) -> Decimal:
    if value.strip():
        return Decimal(value)
    else:
        return Decimal('0')

"""
문자열을 Decimal로 변환
빈 문자열이면 None을 반환
"""
def _to_decimal_optional(value: str) -> Optional[Decimal]:
    if value.strip():
        return Decimal(value) 
    else:
        return None
    
@dataclass
class ResponseHeader:
    pass

@dataclass
class HoGa:
    MKSC_SHRN_ISCD: str    #유가증권 단축 종목코드
    BSOP_HOUR: str    #영업 시간
    HOUR_CLS_CODE: str    #시간 구분 코드
    ASKP1: Decimal    #매도호가1
    ASKP2: Decimal    #매도호가2
    ASKP3: Decimal    #매도호가3
    ASKP4: Decimal    #매도호가4
    ASKP5: Decimal    #매도호가5
    ASKP6: Decimal    #매도호가6
    ASKP7: Decimal    #매도호가7
    ASKP8: Decimal    #매도호가8
    ASKP9: Decimal    #매도호가9
    ASKP10: Decimal    #매도호가10
    BIDP1: Decimal    #매수호가1
    BIDP2: Decimal    #매수호가2
    BIDP3: Decimal    #매수호가3
    BIDP4: Decimal    #매수호가4
    BIDP5: Decimal    #매수호가5
    BIDP6: Decimal    #매수호가6
    BIDP7: Decimal    #매수호가7
    BIDP8: Decimal    #매수호가8
    BIDP9: Decimal    #매수호가9
    BIDP10: Decimal    #매수호가10
    ASKP_RSQN1: Decimal    #매도호가 잔량1
    ASKP_RSQN2: Decimal    #매도호가 잔량2
    ASKP_RSQN3: Decimal    #매도호가 잔량3
    ASKP_RSQN4: Decimal    #매도호가 잔량4
    ASKP_RSQN5: Decimal    #매도호가 잔량5
    ASKP_RSQN6: Decimal    #매도호가 잔량6
    ASKP_RSQN7: Decimal    #매도호가 잔량7
    ASKP_RSQN8: Decimal    #매도호가 잔량8
    ASKP_RSQN9: Decimal    #매도호가 잔량9
    ASKP_RSQN10: Decimal    #매도호가 잔량10
    BIDP_RSQN1: Decimal    #매수호가 잔량1
    BIDP_RSQN2: Decimal    #매수호가 잔량2
    BIDP_RSQN3: Decimal    #매수호가 잔량3
    BIDP_RSQN4: Decimal    #매수호가 잔량4
    BIDP_RSQN5: Decimal    #매수호가 잔량5
    BIDP_RSQN6: Decimal    #매수호가 잔량6
    BIDP_RSQN7: Decimal    #매수호가 잔량7
    BIDP_RSQN8: Decimal    #매수호가 잔량8
    BIDP_RSQN9: Decimal    #매수호가 잔량9
    BIDP_RSQN10: Decimal    #매수호가 잔량10
    TOTAL_ASKP_RSQN: Decimal    #총 매도호가 잔량
    TOTAL_BIDP_RSQN: Decimal    #총 매수호가 잔량
    OVTM_TOTAL_ASKP_RSQN: Decimal    #시간외 총 매도호가 잔량
    OVTM_TOTAL_BIDP_RSQN: Decimal    #시간외 총 매수호가 잔량
    ANTC_CNPR: Optional[Decimal]    #예상 체결가
    ANTC_CNQN: Optional[Decimal]    #예상 체결량
    ANTC_VOL: Optional[Decimal]    #예상 거래량
    ANTC_CNTG_VRSS: Optional[Decimal]    #예상 체결 대비
    ANTC_CNTG_VRSS_SIGN: Optional[str]    #예상 체결 대비 부호
    ANTC_CNTG_PRDY_CTRT: Decimal    #예상 체결 전일 대비율
    ACML_VOL: Decimal    #누적 거래량
    TOTAL_ASKP_RSQN_ICDC: Decimal    #총 매도호가 잔량 증감
    TOTAL_BIDP_RSQN_ICDC: Decimal    #총 매수호가 잔량 증감
    OVTM_TOTAL_ASKP_ICDC: Decimal    #시간외 총 매도호가 증감
    OVTM_TOTAL_BIDP_ICDC: Decimal    #시간외 총 매수호가 증감
    STCK_DEAL_CLS_CODE: str    #주식 매매 구분 코드

    """
    API 원시 데이터(raw_data)의 데이터 부분을 split한 리스트를 받아,
    클래스 인스턴스를 생성하여 반환
    """
    @classmethod
    def _parse_h0stasp0(cls, data_parts: List[str]):
        return cls(
            MKSC_SHRN_ISCD=data_parts[0],
            BSOP_HOUR=data_parts[1],
            HOUR_CLS_CODE=data_parts[2],
            ASKP1=_to_decimal(data_parts[3]),
            ASKP2=_to_decimal(data_parts[4]),
            ASKP3=_to_decimal(data_parts[5]),
            ASKP4=_to_decimal(data_parts[6]),
            ASKP5=_to_decimal(data_parts[7]),
            ASKP6=_to_decimal(data_parts[8]),
            ASKP7=_to_decimal(data_parts[9]),
            ASKP8=_to_decimal(data_parts[10]),
            ASKP9=_to_decimal(data_parts[11]),
            ASKP10=_to_decimal(data_parts[12]),
            BIDP1=_to_decimal(data_parts[13]),
            BIDP2=_to_decimal(data_parts[14]),
            BIDP3=_to_decimal(data_parts[15]),
            BIDP4=_to_decimal(data_parts[16]),
            BIDP5=_to_decimal(data_parts[17]),
            BIDP6=_to_decimal(data_parts[18]),
            BIDP7=_to_decimal(data_parts[19]),
            BIDP8=_to_decimal(data_parts[20]),
            BIDP9=_to_decimal(data_parts[21]),
            BIDP10=_to_decimal(data_parts[22]),
            ASKP_RSQN1=_to_decimal(data_parts[23]),
            ASKP_RSQN2=_to_decimal(data_parts[24]),
            ASKP_RSQN3=_to_decimal(data_parts[25]),
            ASKP_RSQN4=_to_decimal(data_parts[26]),
            ASKP_RSQN5=_to_decimal(data_parts[27]),
            ASKP_RSQN6=_to_decimal(data_parts[28]),
            ASKP_RSQN7=_to_decimal(data_parts[29]),
            ASKP_RSQN8=_to_decimal(data_parts[30]),
            ASKP_RSQN9=_to_decimal(data_parts[31]),
            ASKP_RSQN10=_to_decimal(data_parts[32]),
            BIDP_RSQN1=_to_decimal(data_parts[33]),
            BIDP_RSQN2=_to_decimal(data_parts[34]),
            BIDP_RSQN3=_to_decimal(data_parts[35]),
            BIDP_RSQN4=_to_decimal(data_parts[36]),
            BIDP_RSQN5=_to_decimal(data_parts[37]),
            BIDP_RSQN6=_to_decimal(data_parts[38]),
            BIDP_RSQN7=_to_decimal(data_parts[39]),
            BIDP_RSQN8=_to_decimal(data_parts[40]),
            BIDP_RSQN9=_to_decimal(data_parts[41]),
            BIDP_RSQN10=_to_decimal(data_parts[42]),
            TOTAL_ASKP_RSQN=_to_decimal(data_parts[43]),
            TOTAL_BIDP_RSQN=_to_decimal(data_parts[44]),
            OVTM_TOTAL_ASKP_RSQN=_to_decimal(data_parts[45]),
            OVTM_TOTAL_BIDP_RSQN=_to_decimal(data_parts[46]),
            ANTC_CNPR=_to_decimal_optional(data_parts[47]),
            ANTC_CNQN=_to_decimal_optional(data_parts[48]),
            ANTC_VOL=_to_decimal_optional(data_parts[49]),
            ANTC_CNTG_VRSS=_to_decimal_optional(data_parts[50]),
            ANTC_CNTG_VRSS_SIGN=data_parts[51],
            ANTC_CNTG_PRDY_CTRT=_to_decimal(data_parts[52]),
            ACML_VOL=_to_decimal(data_parts[53]),
            TOTAL_ASKP_RSQN_ICDC=_to_decimal(data_parts[54]),
            TOTAL_BIDP_RSQN_ICDC=_to_decimal(data_parts[55]),
            OVTM_TOTAL_ASKP_ICDC=_to_decimal(data_parts[56]),
            OVTM_TOTAL_BIDP_ICDC=_to_decimal(data_parts[57]),
            STCK_DEAL_CLS_CODE=data_parts[58]
        )