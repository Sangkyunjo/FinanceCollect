from datetime import datetime

import pandas as pd
from pykrx import stock
from sqlalchemy import text, inspect
from sqlalchemy.types import Numeric, String
from tqdm.auto import tqdm
import time
import random


class PykrxMultiCollector:
    def __init__(self, engine):
        self.engine = engine
        print("Start Korean Stock collector (Cross-sectional Mode).")

    def _get_table_name_for_year(self, year, prefix):
        start_year_block = (year // 5) * 5
        end_year_block = start_year_block + 4
        table_suffix = f"{str(start_year_block)[-2:]}{str(end_year_block)[-2:]}"
        return f"{prefix}{table_suffix}"

    def generate_dtype_map(self, df):
        """
        (수정됨) 시가총액 등 큰 숫자를 다루기 위해 데이터 타입 매핑 범위를 확장합니다.
        """
        dtype_map = {}

        # 1. 큰 숫자가 확실한 컬럼 목록 (여기에 포함되면 무조건 30자리 할당)
        large_number_cols = [
            '시가총액', '거래대금', '거래량', '상장주식수', '전체',
            '기관합계', '기타법인', '개인', '외국인합계'
        ]

        for col, dtype in df.dtypes.items():
            # (A) 대형 숫자 컬럼 처리
            if col in large_number_cols:
                dtype_map[col] = Numeric(30, 2)  # 30자리로 설정
                continue

            # (B) 일반 Float 처리 (PER, PBR 등 비율 지표)
            if pd.api.types.is_float_dtype(dtype):
                if df[col].isnull().all():
                    dtype_map[col] = Numeric(18, 4)
                else:
                    dtype_map[col] = Numeric(18, 4)

            # (C) 일반 Integer 처리
            elif pd.api.types.is_integer_dtype(dtype):
                dtype_map[col] = Numeric(30, 0)

            # (D) 문자열 처리
            elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
                if col == 'tdate':
                    dtype_map[col] = String(8)
                elif col == 'symbol':
                    dtype_map[col] = String(10)
                else:
                    dtype_map[col] = String(20)

        return dtype_map

    def _fetch_daily_price_all(self, target_date, market="KOSPI"):
        try:
            df = stock.get_market_ohlcv_by_ticker(target_date, market=market)
            if df.empty: return None

            df = df.reset_index()
            df['tdate'] = target_date
            df.rename(columns={'티커': 'symbol'}, inplace=True)

            df_subset = df[['tdate', '시가', '고가', '저가', '종가', '거래량', 'symbol']].copy()
            return df_subset
        except Exception as e:
            print(f"Error fetching price data for {target_date}: {e}")
            return None

    def _fetch_daily_fundamental_all(self, target_date, market='KOSPI'):
        try:
            df_fund = stock.get_market_fundamental_by_ticker(target_date, market=market)
            df_cap = stock.get_market_cap_by_ticker(target_date, market=market)

            investor_list = ['기관합계', '기타법인', '개인', '외국인']
            df_trades = []
            for inv in investor_list:
                try:
                    df_inv = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, market,
                                                                                  inv)
                    col_name = '외국인합계' if inv == '외국인' else inv
                    df_inv = df_inv[['순매수거래대금']].rename(columns={'순매수거래대금': col_name})
                    df_trades.append(df_inv)
                except:
                    continue

            df_merge = pd.merge(df_fund, df_cap, left_index=True, right_index=True, how='outer')
            for df_inv in df_trades:
                df_merge = pd.merge(df_merge, df_inv, left_index=True, right_index=True, how='outer')

            df_merge = df_merge.reset_index()
            df_merge['tdate'] = target_date
            df_merge.rename(columns={'티커': 'symbol', 'index': 'symbol'}, inplace=True)

            rename_map = {'BPS': 'bps', 'PER': 'per', 'PBR': 'pbr', 'EPS': 'eps', 'DIV': 'div', 'DPS': 'dps'}
            df_merge.rename(columns=rename_map, inplace=True)

            final_columns = [
                'tdate', 'bps', 'per', 'pbr', 'eps', 'div', 'dps',
                '기관합계', '기타법인', '개인', '외국인합계',
                '시가총액', '거래량', '거래대금', '상장주식수', 'symbol'
            ]
            # DataFrame에 없는 컬럼이 있다면 NaN(None)으로 강제 생성
            for col in final_columns:
                if col not in df_merge.columns:
                    df_merge[col] = None  # 혹은 np.nan

            return df_merge[final_columns]

        except Exception as e:
            print(f"Error fetching fundamental data for {target_date}: {e}")
            return None

    def _store_data_bulk(self, df_all, table_prefix):
        if df_all.empty: return

        df_all['tdate_dt'] = pd.to_datetime(df_all['tdate'], format='%Y%m%d')
        df_all['TARGET_TABLE'] = df_all['tdate_dt'].dt.year.apply(self._get_table_name_for_year, args=(table_prefix,))

        grouped = df_all.groupby('TARGET_TABLE')

        for table_name, df_group in grouped:
            try:
                inspector = inspect(self.engine)
                table_exists = inspector.has_table(table_name, schema='public')

                with self.engine.connect() as conn:
                    df_to_insert = df_group.drop(columns=['TARGET_TABLE', 'tdate_dt'])

                    if table_exists:
                        existing_dates_query = text(f"SELECT DISTINCT tdate FROM \"public\".{table_name}")
                        existing_dates_df = pd.read_sql_query(existing_dates_query, conn)
                        if not existing_dates_df.empty:
                            existing_dates_set = set(existing_dates_df['tdate'].values)
                            df_to_insert = df_to_insert[~df_to_insert['tdate'].isin(existing_dates_set)]

                    if not df_to_insert.empty:
                        if not table_exists:
                            print(f"Creating table '{table_name}'...")
                            dtype_map = self.generate_dtype_map(df_to_insert)

                            # 디버깅용 출력 (이 부분이 중요)
                            print(f"DEBUG: '시가총액' type mapping -> {dtype_map.get('시가총액', 'Not Found')}")

                            df_to_insert.to_sql(table_name, conn, schema='public', if_exists='fail', index=False,
                                                dtype=dtype_map)
                            print(f"Table created and {len(df_to_insert)} records inserted.")
                        else:
                            df_to_insert.to_sql(table_name, conn, schema='public', if_exists='append', index=False)
                            print(f"[{table_name}] Appended {len(df_to_insert)} records.")

                        conn.commit()
                    else:
                        print(f"[{table_name}] All dates already exist. Skipping.")

            except Exception as e:
                print(f"Error storing bulk data into {table_name}: {e}")

    def collect_and_store_all(self, start_date, end_date, market="KOSPI"):
        """날짜별 루프 -> 연도별 저장"""

        # Market에 따라 테이블 접두어(Prefix) 분기 처리
        if market == "KOSDAQ":
            price_prefix = "korprice_kosdaq"
            fund_prefix = "kor_pykrx_kosdaq"
        else:
            # KOSPI인 경우 기존 테이블명 유지
            price_prefix = "korprice"
            fund_prefix = "kor_pykrx"

        print(f"영업일 목록을 조회 중입니다... (Market: {market})")
        print(f"Target Tables Prefix -> Price: {price_prefix} / Fund: {fund_prefix}")

        if isinstance(start_date, datetime):
            start_str = start_date.strftime("%Y%m%d")
        else:
            start_str = start_date

        if isinstance(end_date, datetime):
            end_str = end_date.strftime("%Y%m%d")
        else:
            end_str = end_date

        # 영업일 조회 (삼성전자 기준 - KOSPI/KOSDAQ 휴장일은 동일하므로 그대로 둠)
        try:
            # 005930은 삼성전자로 KOSPI 종목이지만, 휴장일 체크용으로만 사용하므로 유지해도 무방
            valid_days = stock.get_market_ohlcv_by_date(start_str, end_str, "005930", adjusted=False).index
        except:
            print("영업일 조회 실패, 날짜 범위를 확인하세요.")
            return

        print(f"총 영업일 수: {len(valid_days)}일")
        unique_years = valid_days.year.unique()

        for year in unique_years:
            yearly_price = []
            yearly_fund = []

            target_days = valid_days[valid_days.year == year]
            print(f"\n=== Processing Year: {year} ({len(target_days)} days) ===")

            for date in tqdm(target_days, desc=f"{year} Data Collection"):
                target_date_str = date.strftime("%Y%m%d")

                # (A) 주가 데이터 수집
                df_price = self._fetch_daily_price_all(target_date_str, market=market)
                if df_price is not None:
                    yearly_price.append(df_price)
                    time.sleep(random.uniform(0.5, 1.5))

                # (B) Fundamental 데이터 수집
                df_fund = self._fetch_daily_fundamental_all(target_date_str, market=market)  # market 인자 전달 확인
                if df_fund is not None:
                    yearly_fund.append(df_fund)
                    time.sleep(random.uniform(0.5, 1.5))

            # DB 저장
            print(f"\nSaving {year} data to Database...")

            # if yearly_price:
            #     df_price_all = pd.concat(yearly_price, ignore_index=True)
            #     # 설정한 prefix 변수 사용
            #     self._store_data_bulk(df_price_all, table_prefix=price_prefix)
            #     print(f"-> Price data saved: {len(df_price_all)} rows to tables starting with '{price_prefix}'")
            #     del df_price_all, yearly_price

            if yearly_fund:
                df_fund_all = pd.concat(yearly_fund, ignore_index=True)
                # 설정한 prefix 변수 사용
                self._store_data_bulk(df_fund_all, table_prefix=fund_prefix)
                print(f"-> Fundamental data saved: {len(df_fund_all)} rows to tables starting with '{fund_prefix}'")
                del df_fund_all, yearly_fund

        print("\nAll collection finished successfully!")