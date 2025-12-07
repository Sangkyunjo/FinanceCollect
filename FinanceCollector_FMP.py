#!/usr/bin/env python
import json
import os
from datetime import datetime
from urllib.request import urlopen

import certifi
import oracledb
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table
from sqlalchemy import text, inspect
from tqdm import tqdm

# from sqlalchemy.dialects.oracle import insert as oracle_insert
from FinancceUtils import *

oracledb.version = "8.3.0"

class Fmp_load:
    def __init__(self, engine_in, start_date, end_date):
        load_dotenv()
        self.base_url = "https://financialmodelingprep.com/api/v3"
        self.engine_in = engine_in
        self.start_date = start_date
        self.end_date = end_date
        self.today_YYYYMMDD = (str(datetime.today().year) + str(datetime.today().month).zfill(2)
                               + str(datetime.today().day).zfill(2))
        self.api_key = os.environ.get('fmpkey')  # 실제 API 키로 변경해주세요.
        self.DB_SCHEMA_COLUMNS = ['ticker', 'tdate', 'open', 'high', 'low', 'close', 'adjClose',
                                  'volume', 'unadjustedVolume', 'change', 'changePercent', 'vwap',
                                  'label', 'changeOverTime']

    def get_jsonparsed_data(self, url):
        response = urlopen(url, cafile=certifi.where())
        data = response.read().decode("utf-8")
        return json.loads(data)

    def get_all_us_tickers(self):
        url = f"https://financialmodelingprep.com/stable/stock-list?apikey={self.api_key}"
        data = self.get_jsonparsed_data(url)
        tickers = sorted([item['symbol'] for item in data])
        return tickers

    def get_sp500_tickers(self) -> set:
        """
        현재 S&P 500 지수를 구성하는 모든 티커를 set 형태로 반환합니다.
        """
        url = f"{self.base_url}/sp500_constituent?apikey={self.api_key}"
        data = self.get_jsonparsed_data(url)
        return {item['symbol'] for item in data}

    def get_nasdaq100_tickers(self) -> set:
        """
        현재 NASDAQ 100 지수를 구성하는 모든 티커를 set 형태로 반환합니다.
        """
        url = f"{self.base_url}/nasdaq_constituent?apikey={self.api_key}"
        data = self.get_jsonparsed_data(url)
        return {item['symbol'] for item in data}

    def get_sp500_and_nasdaq100_tickers(self) -> list:
        """
        S&P 500과 NASDAQ 100의 모든 티커를 가져와 합친 후,
        중복을 제거하고 알파벳 순으로 정렬하여 리스트로 반환합니다.
        """
        print("S&P 500 티커를 가져오는 중...")
        sp500_tickers = self.get_sp500_tickers()
        print(f"S&P 500 티커 {len(sp500_tickers)}개 확인.")

        print("\nNASDAQ 100 티커를 가져오는 중...")
        nasdaq100_tickers = self.get_nasdaq100_tickers()
        print(f"NASDAQ 100 티커 {len(nasdaq100_tickers)}개 확인.")

        # 두 set을 합집합(union) 연산자로 합쳐 중복을 자동으로 제거
        combined_tickers = sp500_tickers.union(nasdaq100_tickers)

        print(f"\n총 {len(combined_tickers)}개의 고유한 티커를 추출했습니다.")

        # 알파벳 순으로 정렬하여 반환
        return sorted(list(combined_tickers))

    def _get_table_name_for_year(self, year):
        """주어진 연도를 기반으로 5년 단위의 테이블 이름을 반환합니다."""
        # 5년 단위 블록의 시작 연도 계산 (e.g., 2023 -> 2020, 2019 -> 2015)
        start_year_block = (year // 5) * 5
        end_year_block = start_year_block + 4

        # 테이블 이름 접미사 생성 (e.g., '2024', '1519')
        table_suffix = f"{str(start_year_block)[-2:]}{str(end_year_block)[-2:]}"

        return f"fmp_us_stockprice{table_suffix}"

    def _store_data_by_year_partition(self, df_new):
        if df_new.empty:
            print("DataFrame is empty, nothing to store.")
            return True

        ticker = df_new['ticker'].iloc[0]

        # 중복 제거 및 연산용 날짜 객체 컬럼 생성
        df_new = df_new.drop_duplicates(subset=['ticker', 'tdate'], keep='first')
        df_new['TDATE_obj'] = pd.to_datetime(df_new['tdate'])

        df_new['TARGET_TABLE'] = df_new['TDATE_obj'].dt.year.apply(self._get_table_name_for_year)
        grouped = df_new.groupby('TARGET_TABLE')

        for table_name, df_group in grouped:
            try:
                # SQLAlchemy inspector로 테이블이 실제로 DB에 존재하는지 확인
                inspector = inspect(self.engine_in)
                table_exists = inspector.has_table(table_name, schema='public')

                with self.engine_in.connect() as conn:
                    df_to_insert = df_group

                    # --- 핵심 로직 1: 테이블이 이미 존재할 경우 ---
                    if table_exists:
                        # DB에 저장된 날짜를 조회하여 중복되지 않은 데이터만 필터링
                        df_existing = pd.read_sql_query(
                            text(f"SELECT tdate FROM \"public\".{table_name} WHERE TICKER='{ticker}'"), conn)
                        # df_existing = df_existing.rename({'tdate':'TDATE'}, axis=1)

                        if not df_existing.empty:
                            existing_dates_set = set(df_existing['tdate'])
                            df_to_insert = df_group[~df_group['tdate'].isin(existing_dates_set)]

                    # 삽입할 데이터가 있을 때만 아래 로직 실행
                    if not df_to_insert.empty:
                        df_to_insert_final = df_to_insert.drop(columns=['TARGET_TABLE', 'TDATE_obj'])

                        # --- 핵심 로직 2: 테이블 생성 vs 데이터 추가 ---
                        if not table_exists:
                            # [테이블 생성]
                            print(f"Table '{table_name}' not found. Creating table with data from Ticker '{ticker}'...")
                            # dtype_map = generate_dtype_map(df_to_insert_final)
                            meta = MetaData()
                            source_tbn = table_name[:-4]+'2024'
                            # TODO : 2024가 어디서 나온건지 확인

                            source_table = Table(
                                source_tbn.lower(),
                                meta,
                                autoload_with=self.engine_in,
                                schema='public'
                            )

                            new_table = Table(
                                table_name,
                                meta,
                                schema='public'
                            )

                            for column in source_table.c:
                                new_table.append_column(column.copy())

                            new_table.create(bind=self.engine_in)
                            df_to_insert_final.to_sql(table_name, conn, schema='public',
                                                      if_exists='append', index=False)

                            print(f"Table created and {len(df_to_insert_final)} initial records inserted.")
                        else:
                            df_to_insert_final.to_sql(table_name, conn, schema='public',
                                                      if_exists='append', index=False)
                            print(f"[{ticker}]: {len(df_to_insert_final)} new records appended.")

                        conn.commit()  # 모든 작업을 최종적으로 DB에 저장
                        return None
                    else:
                        print(f"[{ticker}]: No new records for {table_name}")
                        return None

            except Exception as e:
                print(f"An error occurred while processing data for {ticker} in {table_name}: {e}")
                return None
        return None

    def _store_new_data_by_date(self, ticker, df_new, table_name, dtyp):
        if not df_new.empty:
            conn = self.engine_in.connect()
            try:
                df_existing = pd.read_sql_query(
                    text(f'''SELECT tdate FROM \"public\".{table_name} WHERE ticker='{ticker}' '''), conn)
                conn.close()

                if not df_existing.empty:
                    existing_dates = pd.to_datetime(df_existing['tdate']).dt.date.tolist()
                    new_dates = pd.to_datetime(df_new['tdate']).dt.date
                    df_to_insert = df_new[~new_dates.isin(existing_dates)]
                else:
                    df_to_insert = df_new

                if not df_to_insert.empty:
                    # df_to_insert['SYMBOL'] = ticker
                    df_to_insert = df_to_insert.drop(columns=['period', 'acceptedDate'], errors='ignore')
                    df_to_insert.drop(columns=['finalLink', 'link'], errors='ignore').to_sql(table_name, self.engine_in, if_exists='append', index=False, dtype=dtyp, schema='public')
                    print(f"{ticker}: {len(df_to_insert)} new date records added to {table_name}")
                else:
                    print(f"{ticker}: No new date records for {table_name}")
                return True
            except Exception as e:
                print(f"Error storing new date data for {ticker} in {table_name}: {e}")
                return False
        else:
            print(f"{ticker}: No data fetched for {table_name}")
            return True

    def fetch_and_store_stock_price(self, ticker, start_date, end_date):
        """
        (수정) API 데이터를 가져와 최종 스키마에 맞게 정규화합니다.
        """
        stdate, e_date = start_date[:4]+'-'+start_date[4:6]+'-'+start_date[6:], end_date[:4]+'-'+end_date[4:6]+'-'+end_date[6:]
        url = f'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={ticker}&from={stdate}&to={e_date}&apikey={self.api_key}'
        data = self.get_jsonparsed_data(url)
        if not data:
            print(f"[{ticker}] No data returned from API.")
            return True

        df_new = pd.DataFrame(data)

        if 'date' not in df_new.columns:
            print(f"[{ticker}] Error: API response is missing 'date' column. Skipping.")
            return True

        df_new = df_new.rename(columns={'date': 'tdate'})

        if 'symbol' in df_new.columns:
            df_new = df_new.rename(columns={'symbol': 'ticker'})
        else:
            df_new['ticker'] = ticker

        # --- 2. 데이터 정규화 ---
        # API 응답을 우리가 정의한 최종 스키마(DB_SCHEMA_COLUMNS)에 맞게 재구성합니다.
        # API 응답에 없는 컬럼은 NaN 값을 가진 컬럼으로 자동 추가됩니다.
        df_new = df_new.reindex(columns=self.DB_SCHEMA_COLUMNS)

        return self._store_data_by_year_partition(df_new)

    def fetch_and_store_split_history(self, ticker):
        table_name = 'fmp_us_stocksplit'
        url = f"{self.base_url}/historical-price-full/stock_split/{ticker}?apikey={self.api_key}"
        data = self.get_jsonparsed_data(url).get('historical', [])
        df_new = pd.DataFrame(data).rename(columns={'date': 'tdate'})
        dtyp = {col: sqla.types.VARCHAR(20) if df_new[col].dtype == 'object' else sqla.FLOAT for col in df_new.columns}
        return self._store_new_data_by_date(ticker, df_new, table_name, dtyp)

    def fetch_and_store_financial_data(self, ticker, endpoint, table_name, limit=None):
        url = f"{self.base_url}/{endpoint}/{ticker}?period=quarter"
        if limit:
            url += f'&limit={limit}'
        url += f'&apikey={self.api_key}'
        data = self.get_jsonparsed_data(url)
        df_new = pd.DataFrame(data)
        if not df_new.empty and 'date' in df_new.columns and 'symbol' in df_new.columns:
            # df_new = df_new.rename(columns={'date': 'tdate', 'symbol': 'SYMBOL'})
            dtyp = {col: sqla.types.VARCHAR(df_new[col].astype(str).str.len().max()) if df_new[col].dtype == 'object' else sqla.FLOAT for col in df_new.columns}
            return self._store_new_data_by_date(ticker, df_new, table_name, dtyp)
        return False

    def comm_et(self, start_date, end_date, comm_filepath=None):
        commodity_list = self._get_symbol_list(
            url=f"{self.base_url}/symbol/available-commodities?apikey={self.api_key}",
            filepath=comm_filepath,
            default_filepath='commoditylist.csv',
            symbol_key='symbol'
        )
        table_name = 'fmp_us_commodity'
        for ticker in tqdm(commodity_list, desc="Collecting Commodities"):
            url = f"{self.base_url}/historical-price-full/{ticker}?from={start_date}&to={end_date}&apikey={self.api_key}"
            data = self.get_jsonparsed_data(url).get('historical', [])
            df_new = pd.DataFrame(data).rename(columns={'date': 'tdate', 'symbol':'ticker'})
            dtyp = {col: sqla.types.VARCHAR(20) if df_new[col].dtype == 'object' else sqla.FLOAT for col in df_new.columns}
            self._store_new_data_by_date(ticker, df_new, table_name, dtyp)

    def exchage_et(self, start_date, end_date):
        exchange_list = ['USDCNY', 'USDJPY', 'USDEUR', 'USDAUD', 'USDKRW', 'KRWJPY']
        table_name = 'fmp_forex'
        for ticker in tqdm(exchange_list, desc="Collecting Exchange Rates"):
            url = f"{self.base_url}/historical-price-full/{ticker}?from={start_date}&to={end_date}&apikey={self.api_key}"
            data = self.get_jsonparsed_data(url).get('historical', [])
            df_new = pd.DataFrame(data).rename(columns={'date': 'tdate', 'symbol':'ticker'})
            dtyp = {col: sqla.types.VARCHAR(20) if df_new[col].dtype == 'object' else sqla.FLOAT for col in df_new.columns}
            self._store_new_data_by_date(ticker, df_new, table_name, dtyp)

    def crypto_et(self, start_date, end_date):
        crypto_list = ['BTCUSD', 'ETHUSD', 'RVNUSD', 'DOGEUSD', 'USDTUSD', 'BUSDUSD']
        table_name = 'fmp_crypto'
        for ticker in tqdm(crypto_list, desc="Collecting Cryptocurrencies"):
            url = f"{self.base_url}/historical-price-full/{ticker}?from={start_date}&to={end_date}&apikey={self.api_key}"
            data = self.get_jsonparsed_data(url).get('historical', [])
            df_new = pd.DataFrame(data).rename(columns={'date': 'tdate'})
            dtyp = {col: sqla.types.VARCHAR(20) if df_new[col].dtype == 'object' else sqla.FLOAT for col in df_new.columns}
            self._store_new_data_by_date(ticker, df_new, table_name, dtyp)

    def _get_symbol_list(self, url, filepath=None, default_filepath=None, symbol_key='symbol'):
        if filepath:
            try:
                return list(pd.read_csv(filepath)[symbol_key])
            except FileNotFoundError:
                print(f"File not found: {filepath}. Fetching from API...")
                return []
        elif default_filepath:
            try:
                return list(pd.read_csv(default_filepath)[symbol_key])
            except FileNotFoundError:
                print(f"File not found: {default_filepath}. Fetching from API...")
                return []
        try:
            temp_data = self.get_jsonparsed_data(url)
            df = pd.DataFrame(temp_data)
            if not df.empty and symbol_key in df.columns:
                if default_filepath:
                    df.to_csv(default_filepath, index=False)
                return list(df[symbol_key])
            else:
                return []
        except Exception as e:
            print(f"Error retrieving symbol list from API: {e}")
            return []



