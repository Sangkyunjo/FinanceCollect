import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import String, Numeric, Date
from datetime import datetime


class YahooAssetCollector:
    def __init__(self, engine):
        self.engine = engine
        print("Yahoo Multi-Asset Collector initialized.")

    def _get_table_name_for_year(self, year, prefix):
        """5년 단위 테이블명 생성"""
        start_year_block = (year // 5) * 5
        end_year_block = start_year_block + 4
        suffix = f"{str(start_year_block)[-2:]}{str(end_year_block)[-2:]}"
        return f"{prefix}_{suffix}"

    def generate_dtype_map(self, df):
        """데이터 타입 매핑"""
        dtype_map = {}
        # 가격은 소수점이 중요하므로 정밀도 확보 (환율은 소수점 4~6자리 중요)
        large_cols = ['Volume', 'Adj Close', 'Close', 'High', 'Low', 'Open']

        for col in df.columns:
            if col in large_cols:
                dtype_map[col] = Numeric(30, 8)  # 암호화폐/환율은 소수점이 길 수 있음 (8자리)
            elif col == 'Date':
                dtype_map[col] = Date()
            elif col == 'Ticker':
                dtype_map[col] = String(20)  # 티커 길이가 길 수 있음 (BTC-USD)
            else:
                dtype_map[col] = String(50)
        return dtype_map

    def _store_to_db(self, df, year, table_prefix):
        """DB 저장 로직 (중복 제거 포함)"""
        table_name = self._get_table_name_for_year(year, prefix=table_prefix)

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])

        try:
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table_name, schema='public')

            with self.engine.connect() as conn:
                # 중복 제거
                if table_exists:
                    existing_dates_query = text(f'SELECT DISTINCT "Date" FROM "public".{table_name}')
                    try:
                        existing_dates = pd.read_sql(existing_dates_query, conn)
                        if not existing_dates.empty:
                            existing_dates_set = set(pd.to_datetime(existing_dates['Date']))
                            df = df[~df['Date'].isin(existing_dates_set)]
                    except Exception:
                        pass

                if not df.empty:
                    print(f"   Saving {len(df)} rows to '{table_name}'...")
                    dtype_map = self.generate_dtype_map(df)

                    if not table_exists:
                        df.to_sql(table_name, conn, schema='public', if_exists='fail', index=False, dtype=dtype_map)
                    else:
                        df.to_sql(table_name, conn, schema='public', if_exists='append', index=False, dtype=dtype_map)

                    conn.commit()
                else:
                    print(f"   [{table_name}] All dates already exist.")

        except Exception as e:
            print(f"   DB Error on {table_name}: {e}")

    def collect_assets(self, tickers, start_date, end_date, table_prefix):
        """
        [범용 수집 메서드]
        주어진 티커 리스트(tickers)에 대해 데이터를 수집하여 table_prefix 테이블에 저장
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        print(f"\n=== Collecting {table_prefix} ({start_dt.date()} ~ {end_dt.date()}) ===")
        print(f"Target Tickers: {tickers}")

        start_year = start_dt.year
        end_year = end_dt.year

        # 5년 단위 블록 루프
        first_block_start = (start_year // 5) * 5
        last_block_start = (end_year // 5) * 5

        for block_start_year in range(first_block_start, last_block_start + 1, 5):
            block_end_year = block_start_year + 4

            # 교집합 기간 계산
            block_min_dt = pd.Timestamp(f"{block_start_year}-01-01")
            block_max_dt = pd.Timestamp(f"{block_end_year}-12-31")

            current_fetch_start = max(start_dt, block_min_dt)
            current_fetch_end = min(end_dt, block_max_dt)

            if current_fetch_start > current_fetch_end:
                continue

            t_start_str = current_fetch_start.strftime('%Y-%m-%d')
            t_end_str = current_fetch_end.strftime('%Y-%m-%d')

            print(f">> Processing Block {block_start_year} ({t_start_str} ~ {t_end_str})")

            try:
                # 데이터 다운로드 (History Tool 없이 바로 티커로 요청)
                df = yf.download(tickers, start=t_start_str, end=t_end_str,
                                 auto_adjust=True, progress=False, threads=True)

                if df.empty:
                    print("   Download result is empty.")
                    continue

                # 전처리 (Stacking)
                if isinstance(df.columns, pd.MultiIndex):
                    # 전체 NaN 컬럼 제거
                    df = df.dropna(axis=1, how='all')
                    if df.empty: continue
                    try:
                        df_stacked = df.stack(level=1, future_stack=True).reset_index()
                    except ValueError:
                        continue
                else:
                    df_stacked = df.reset_index()
                    df_stacked['Ticker'] = tickers[0]

                # 컬럼 정리
                if 'Date' not in df_stacked.columns:
                    df_stacked.rename(columns={df_stacked.columns[0]: 'Date'}, inplace=True)
                df_stacked.columns = [c.capitalize() if isinstance(c, str) else c for c in df_stacked.columns]

                if 'Close' in df_stacked.columns:
                    df_stacked = df_stacked.dropna(subset=['Close'])

                # DB 저장 (prefix 전달)
                self._store_to_db(df_stacked, block_start_year, table_prefix)

            except Exception as e:
                print(f"   Error: {e}")


if __name__ == "__main__":
    # DB 연결
    # PostgreSQL 연결 정보 (사용자 환경에 맞게 수정)
    db_id = 'postgres'  # PostgreSQL 사용자 ID
    db_pw = '0212'  # PostgreSQL 비밀번호
    db_host = 'localhost'  # DB 서버 주소 (e.g., 'localhost' or '192.168.1.10')
    db_port = '5432'  # PostgreSQL 기본 포트
    db_name = 'FDATA'  # 연결할 데이터베이스 이름

    # PostgreSQL용 create_engine 호출
    engine = create_engine(f'postgresql://{db_id}:{db_pw}@{db_host}:{db_port}/{db_name}')


if __name__=='__main__':
    # 수집기 초기화
    asset_collector = YahooAssetCollector(engine)

    # 공통 수집 기간 설정
    START_DATE = "2000-01-01"
    END_DATE = "2024-12-31"

    # ----------------------------------------
    # 1. 원자재 (Commodities)
    # ----------------------------------------
    commodity_tickers = ['GC=F', 'SI=F', 'HG=F', 'CL=F', 'PL=F', 'ZC=F', 'ZS=F', 'ZW=F', 'ZM=F', 'ZL=F'
        , 'KC=F', 'SB=F', 'CC=F', 'LE=F', 'HE=F']
    asset_collector.collect_assets(
        tickers=commodity_tickers,
        start_date=START_DATE,
        end_date=END_DATE,
        table_prefix="Yahoo_Commodity"  # 테이블 예: Yahoo_Commodity_20202024
    )

    # ----------------------------------------
    # 2. 환율 (Forex)
    # ----------------------------------------
    forex_tickers = [
        'CNY=X', 'JPY=X', 'EUR=X', 'AUD=X', 'KRW=X', 'KRWJPY=X'
    ]
    asset_collector.collect_assets(
        tickers=forex_tickers,
        start_date=START_DATE,
        end_date=END_DATE,
        table_prefix="Yahoo_Forex"
    )

    # ----------------------------------------
    # 3. 가상화폐 (Crypto)
    # ----------------------------------------
    # 암호화폐는 보통 2010년 이후부터 데이터가 존재함
    crypto_tickers = [
        'BTC-USD', 'ETH-USD', 'RVN-USD', 'DOGE-USD', 'USDT-USD', 'BUSD-USD'
    ]
    asset_collector.collect_assets(
        tickers=crypto_tickers,
        start_date="2010-01-01",  # 암호화폐는 시작일 조정 가능
        end_date=END_DATE,
        table_prefix="Yahoo_Crypto"
    )