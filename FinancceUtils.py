import pandas as pd
import numpy as np
from sqlalchemy import Integer, String, Numeric


# sqlalchemy.types에서 필요한 타입들을 미리 import 해두는 것이 좋습니다.
from sqlalchemy.types import Integer, Numeric, String, DateTime, Boolean
import pandas as pd


def generate_dtype_map(self, df):
    """
    (수정됨) 시가총액 등 큰 숫자를 다루기 위해 데이터 타입 매핑 범위를 확장합니다.
    """
    dtype_map = {}

    # 1. 큰 숫자가 확실한 컬럼 목록 (하드코딩으로 지정하여 안전성 확보)
    large_number_cols = [
        '시가총액', '거래대금', '거래량', '상장주식수', '전체',
        '기관합계', '기타법인', '개인', '외국인합계'
    ]

    for col, dtype in df.dtypes.items():
        # (A) 대형 숫자 컬럼 처리
        if col in large_number_cols:
            # 전체 30자리, 소수점 0~2자리 (충분히 큼)
            dtype_map[col] = Numeric(30, 2)
            continue

        # (B) 일반 Float 처리 (PER, PBR 등 비율 지표)
        if pd.api.types.is_float_dtype(dtype):
            if df[col].isnull().all():
                dtype_map[col] = Numeric(18, 4)  # 기본값도 약간 늘림
            else:
                # PER 등이 100억을 넘을 일은 없지만 안전하게 18, 4 정도로 설정
                dtype_map[col] = Numeric(18, 4)

        # (C) 일반 Integer 처리
        elif pd.api.types.is_integer_dtype(dtype):
            dtype_map[col] = Numeric(30, 0)  # 정수도 넉넉하게

        # (D) 문자열 처리
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            if col == 'tdate':
                dtype_map[col] = String(8)
            elif col == 'symbol':
                dtype_map[col] = String(10)
            else:
                dtype_map[col] = String(20)

    return dtype_map
