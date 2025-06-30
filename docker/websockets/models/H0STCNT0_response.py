from dataclasses import dataclass
from decimal import Decimal
from typing import List

"""
국내주식 실시간체결가 response
"""
@dataclass
class ResponseHeader:
    pass

@dataclass
class CheGyeolGa:
    stck_cntg_hour: str    #주식 체결 시간
    stck_prpr: Decimal    #주식 현재가
    prdy_vrss_sign: str    #전일 대비 부호
    prdy_vrss: Decimal    #전일 대비
    prdy_ctrt: Decimal    #전일 대비율
    wghn_avrg_stck_prc: Decimal    #가중 평균 주식 가격
    stck_oprc: Decimal    #주식 시가
    stck_hgpr: Decimal    #주식 최고가
    stck_lwpr: Decimal    #주식 최저가
    askp1: Decimal    #매도호가1
    bidp1: Decimal    #매수호가1
    cntg_vol: Decimal    #체결 거래량
    acml_vol: Decimal    #누적 거래량
    acml_tr_pbmn: Decimal    #누적 거래 대금
    seln_cntg_csnu: Decimal    #매도 체결 건수
    shnu_cntg_csnu: Decimal    #매수 체결 건수
    ntby_cntg_csnu: Decimal    #순매수 체결 건수
    cttr: Decimal    #체결강도
    seln_cntg_smtn: Decimal    #총 매도 수량
    shnu_cntg_smtn: Decimal    #총 매수 수량
    ccld_dvsn: str    #체결구분
    shnu_rate: Decimal    #매수비율
    prdy_vol_vrss_acml_vol_rate: Decimal    #전일 거래량 대비 등락율
    oprc_hour: str    #시가 시간
    oprc_vrss_prpr_sign: str    #시가대비구분
    oprc_vrss_prpr: Decimal    #시가대비
    hgpr_hour: str    #최고가 시간
    hgpr_vrss_prpr_sign: str    #고가대비구분
    hgpr_vrss_prpr: Decimal    #고가대비
    lwpr_hour: str    #최저가 시간
    lwpr_vrss_prpr_sign: str    #저가대비구분
    lwpr_vrss_prpr: Decimal    #저가대비
    bsop_date: str    #영업 일자
    new_mkop_cls_code: str    #신 장운영 구분 코드
    trht_yn: str    #거래정지 여부
    askp_rsqn1: Decimal    #매도호가 잔량1
    bidp_rsqn1: Decimal    #매수호가 잔량1
    total_askp_rsqn: Decimal    #총 매도호가 잔량
    total_bidp_rsqn: Decimal    #총 매수호가 잔량
    vol_tnrt: Decimal    #거래량 회전율
    prdy_smns_hour_acml_vol: Decimal    #전일 동시간 누적 거래량
    prdy_smns_hour_acml_vol_rate: Decimal    #전일 동시간 누적 거래량 비율
    hour_cls_code: str    #시간 구분 코드
    mrkt_trtm_cls_code: str    #임의종료구분코드
    vi_stnd_prc: Decimal    #정적vi발동기준가
    
    """
    API 원시 데이터(raw_data)의 데이터 부분을 split한 리스트를 받아,
    클래스 인스턴스를 생성하여 반환
    """
    @classmethod
    def _parse_h0stcnt0(cls, data_parts: List[str]):
        # API 명세에 따라 각 인덱스의 데이터를 정확한 타입으로 변환
        return cls(
            stck_cntg_hour=data_parts[1],
            stck_prpr=Decimal(data_parts[2]),
            prdy_vrss_sign=data_parts[3],
            prdy_vrss=Decimal(data_parts[4]),
            prdy_ctrt=Decimal(data_parts[5]),
            wghn_avrg_stck_prc=Decimal(data_parts[6]),
            stck_oprc=Decimal(data_parts[7]),
            stck_hgpr=Decimal(data_parts[8]),
            stck_lwpr=Decimal(data_parts[9]),
            askp1=Decimal(data_parts[10]),
            bidp1=Decimal(data_parts[11]),
            cntg_vol=Decimal(data_parts[12]),
            acml_vol=Decimal(data_parts[13]),
            acml_tr_pbmn=Decimal(data_parts[14]),
            seln_cntg_csnu=Decimal(data_parts[15]),
            shnu_cntg_csnu=Decimal(data_parts[16]),
            ntby_cntg_csnu=Decimal(data_parts[17]),
            cttr=Decimal(data_parts[18]),
            seln_cntg_smtn=Decimal(data_parts[19]),
            shnu_cntg_smtn=Decimal(data_parts[20]),
            ccld_dvsn=data_parts[21],
            shnu_rate=Decimal(data_parts[22]),
            prdy_vol_vrss_acml_vol_rate=Decimal(data_parts[23]),
            oprc_hour=data_parts[24],
            oprc_vrss_prpr_sign=data_parts[25],
            oprc_vrss_prpr=Decimal(data_parts[26]),
            hgpr_hour=data_parts[27],
            hgpr_vrss_prpr_sign=data_parts[28],
            hgpr_vrss_prpr=Decimal(data_parts[29]),
            lwpr_hour=data_parts[30],
            lwpr_vrss_prpr_sign=data_parts[31],
            lwpr_vrss_prpr=Decimal(data_parts[32]),
            bsop_date=data_parts[33],
            new_mkop_cls_code=data_parts[34],
            trht_yn=data_parts[35],
            askp_rsqn1=Decimal(data_parts[36]),
            bidp_rsqn1=Decimal(data_parts[37]),
            total_askp_rsqn=Decimal(data_parts[38]),
            total_bidp_rsqn=Decimal(data_parts[39]),
            vol_tnrt=Decimal(data_parts[40]),
            prdy_smns_hour_acml_vol=Decimal(data_parts[41]),
            prdy_smns_hour_acml_vol_rate=Decimal(data_parts[42]),
            hour_cls_code=data_parts[43],
            mrkt_trtm_cls_code=data_parts[44],
            vi_stnd_prc=Decimal(data_parts[45])
        )
