import pandas as pd
import requests
import yfinance as yf
from io import StringIO
from datetime import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import String, Numeric, Date
from tqdm import tqdm


# ==========================================
# 1. SP500History (Historical Ticker 추출기)
# ==========================================
class SP500History:
    def __init__(self):
        self.url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        self.current_tickers = set()
        self.changes = pd.DataFrame()

    def fetch_data(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        try:
            response = requests.get(self.url, headers=headers)
            response.raise_for_status()
            html_content = StringIO(response.text)
        except Exception as e:
            print(f"데이터 다운로드 실패: {e}")
            return

        payload = pd.read_html(html_content)
        df_current = None
        df_changes = None

        for df in payload:
            if 'Symbol' in df.columns and df_current is None:
                df_current = df
            cols_str = str(df.columns).lower()
            if df_changes is None and 'added' in cols_str and 'removed' in cols_str:
                df_changes = df

        if df_current is None or df_changes is None:
            print("Wikipedia 테이블 구조 변경 감지됨.")
            return

        self.current_tickers = set(df_current['Symbol'].str.replace('.', '-', regex=False).tolist())

        processed_changes = pd.DataFrame()
        try:
            # iloc 위치 기반 추출 (Date=0, Added=1, Removed=3)
            processed_changes['Date'] = pd.to_datetime(df_changes.iloc[:, 0], errors='coerce')
            processed_changes['Added'] = df_changes.iloc[:, 1]
            processed_changes['Removed'] = df_changes.iloc[:, 3]

            if isinstance(processed_changes['Added'], pd.DataFrame):
                processed_changes['Added'] = processed_changes['Added'].iloc[:, 0]
            if isinstance(processed_changes['Removed'], pd.DataFrame):
                processed_changes['Removed'] = processed_changes['Removed'].iloc[:, 0]

            processed_changes['Added'] = processed_changes['Added'].str.replace('.', '-', regex=False)
            processed_changes['Removed'] = processed_changes['Removed'].str.replace('.', '-', regex=False)
            processed_changes = processed_changes.dropna(subset=['Date'])
            self.changes = processed_changes.sort_values('Date', ascending=False)
        except Exception as e:
            print(f"변경 내역 처리 오류: {e}")

    def get_history(self, start_date='2000-01-01'):
        if not self.current_tickers:
            self.fetch_data()

        start_date = pd.to_datetime(start_date)
        current_set = self.current_tickers.copy()
        history = []

        # Wikipedia 변경 내역이 start_date보다 미래인 경우를 역산
        # (과거 -> 현재가 아니라, 현재 -> 과거로 가면서 상태 복원)

        # 1. 먼저 가장 최신 상태 저장
        # history.append({'Date': datetime.now(), 'Tickers': list(current_set)})

        # 변경 내역 루프 (최신 -> 과거 순)
        for date, group in self.changes.groupby('Date', sort=False):
            # 루프 도중 해당 날짜가 start_date보다 이전이면 중단하고 싶겠지만,
            # 정확한 시점 복원을 위해 start_date 직전까지는 계산해야 함.
            # 여기서는 편의상 전체를 다 돌고 나중에 필터링하거나 로직을 유지함.

            # 현재 시점(변경 직후) 상태 기록
            history.append({
                'Date': date,
                'Tickers': list(current_set)
            })

            # 역연산: (현재) - (추가된 것) + (삭제된 것) = (과거)
            for _, row in group.iterrows():
                added = row['Added']
                removed = row['Removed']
                if added in current_set:
                    current_set.remove(added)
                if pd.notna(removed) and removed != '':
                    current_set.add(removed)

            if date < start_date:
                # start_date 이전 시점의 상태까지 복원했으면 종료
                # 마지막 루프의 결과가 start_date 시점의 리스트가 됨
                history.append({'Date': start_date, 'Tickers': list(current_set)})
                break

        return pd.DataFrame(history)


# ==========================================
# 2. YahooSP500Collector (데이터 수집 및 저장)
# ==========================================
class YahooSP500Collector:
    def __init__(self, engine, sp500_tool):
        self.engine = engine
        self.sp500_tool = sp500_tool
        print("Yahoo S&P 500 Collector initialized.")

    def _get_table_name_for_year(self, year, prefix="YahooSP500_Price"):
        """
        [수정됨] prefix 인자를 받을 수 있도록 변경
        5년 단위 테이블명 생성 (예: YahooSP500_Price_20002004)
        """
        start_year_block = (year // 5) * 5
        end_year_block = start_year_block + 4
        suffix = f"{str(start_year_block)[-2:]}{str(end_year_block)[-2:]}"
        return f"{prefix}_{suffix}"

    def generate_dtype_map(self, df):
        """데이터 타입 매핑 (PostgreSQL 호환)"""
        dtype_map = {}
        large_cols = ['Volume', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Market_Cap', 'EBITDA', 'Revenue']

        for col in df.columns:
            if col in large_cols:
                dtype_map[col] = Numeric(30, 6)
            elif col == 'Date':
                dtype_map[col] = Date()
            elif col == 'Ticker':
                dtype_map[col] = String(10)
            else:
                dtype_map[col] = String(50)
        return dtype_map

    def _store_to_db(self, df, year):
        """
        [수정됨] 트랜잭션 관리 강화
        - engine.begin() 사용: 에러 발생 시 자동 Rollback 수행하여 'InFailedSqlTransaction' 방지
        - inspect(conn) 사용: 엔진이 아닌 현재 연결된 세션 안에서 테이블 존재 여부 확인
        """
        # prefix를 명시적으로 전달 (기본값 사용)
        table_name = self._get_table_name_for_year(year, prefix="YahooSP500_Price")

        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])

        try:
            # [핵심 1] 메인 트랜잭션 시작
            with self.engine.begin() as conn:

                # inspector 생성
                inspector = inspect(conn)
                table_exists = inspector.has_table(table_name, schema='public')

                # --- 중복 제거 로직 ---
                if table_exists:
                    existing_dates_query = text(f'SELECT DISTINCT "Date" FROM "public".{table_name}')
                    try:
                        with conn.begin_nested():
                            existing_dates = pd.read_sql(existing_dates_query, conn)
                            if not existing_dates.empty:
                                existing_dates_set = set(pd.to_datetime(existing_dates['Date']))
                                df = df[~df['Date'].isin(existing_dates_set)]
                    except Exception:
                        # begin_nested() 덕분에 여기서 에러가 나면
                        # 해당 내부 트랜잭션만 롤백되고, 바깥의 conn은 "정상" 상태 유지
                        print(f"   Note: Could not read existing dates from {table_name}. Inserting all.")
                        pass

                # --- 데이터 저장 ---
                if not df.empty:
                    print(f"   Saving {len(df)} rows to '{table_name}'...")
                    dtype_map = self.generate_dtype_map(df)

                    if not table_exists:
                        df.to_sql(table_name, conn, schema='public', if_exists='fail', index=False, dtype=dtype_map)
                    else:
                        df.to_sql(table_name, conn, schema='public', if_exists='append', index=False, dtype=dtype_map)

                    # conn.commit() # begin()을 쓰면 명시적 commit 필요 없음 (자동 수행)
                    print("   Done.")
                else:
                    print(f"   [{table_name}] All dates already exist. Skipping insert.")

        except Exception as e:
            # begin()을 썼기 때문에, 여기서 에러가 잡혔다는 뜻은 이미 DB에서 Rollback이 완료되었다는 뜻입니다.
            # 따라서 다음 루프에서는 깨끗한 상태로 다시 시작할 수 있습니다.
            print(f"   DB Error on {table_name}: {e}")

    def collect_and_store(self, start_date, end_date):
        """
        날짜 단위로 정밀하게 데이터를 수집하여 5년 단위 테이블에 분산 저장
        """
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        start_year = start_dt.year
        end_year = end_dt.year

        print(f"=== Collection Range: {start_dt.date()} ~ {end_dt.date()} ===")
        print("Fetching Historical Ticker configurations...")

        history_df = self.sp500_tool.get_history(start_date=start_dt.strftime("%Y-%m-%d"))
        history_df = history_df.sort_values('Date')

        first_block_start = (start_year // 5) * 5
        last_block_start = (end_year // 5) * 5

        for block_start_year in range(first_block_start, last_block_start + 1, 5):
            block_end_year = block_start_year + 4
            block_min_dt = pd.Timestamp(f"{block_start_year}-01-01")
            block_max_dt = pd.Timestamp(f"{block_end_year}-12-31")

            current_fetch_start = max(start_dt, block_min_dt)
            current_fetch_end = min(end_dt, block_max_dt)

            if current_fetch_start > current_fetch_end:
                continue

            t_start_str = current_fetch_start.strftime('%Y-%m-%d')
            t_end_str = current_fetch_end.strftime('%Y-%m-%d')

            print(f"\n>> Processing Block [{block_start_year}~{block_end_year}]")
            print(f"   Actual Fetch Range: {t_start_str} ~ {t_end_str}")

            mask = (history_df['Date'] >= t_start_str) & (history_df['Date'] <= t_end_str)
            period_history = history_df[mask]

            initial_tickers = []
            pre_start_history = history_df[history_df['Date'] <= t_start_str]
            if not pre_start_history.empty:
                initial_tickers = pre_start_history.iloc[-1]['Tickers']

            tickers_in_period = set(x for row in period_history['Tickers'] for x in row)
            tickers_in_period.update(initial_tickers)
            target_tickers = list(tickers_in_period)

            if not target_tickers:
                print("   No tickers found. Skipping.")
                continue

            print(f"   Target Tickers: {len(target_tickers)}")

            try:
                df = yf.download(target_tickers, start=t_start_str, end=t_end_str,
                                 auto_adjust=True, progress=True, threads=True)

                if df.empty:
                    print("   Download result is empty.")
                    continue

                if isinstance(df.columns, pd.MultiIndex):
                    df = df.dropna(axis=1, how='all')
                    if df.empty: continue
                    try:
                        # [수정됨] future_stack=True 추가
                        df_stacked = df.stack(level=1, future_stack=True).reset_index()
                    except ValueError:
                        continue
                else:
                    df_stacked = df.reset_index()
                    df_stacked['Ticker'] = target_tickers[0]

                if 'Date' not in df_stacked.columns:
                    df_stacked.rename(columns={df_stacked.columns[0]: 'Date'}, inplace=True)
                df_stacked.columns = [c.capitalize() if isinstance(c, str) else c for c in df_stacked.columns]

                if 'Close' in df_stacked.columns:
                    df_stacked = df_stacked.dropna(subset=['Close'])

                self._store_to_db(df_stacked, block_start_year)

            except Exception as e:
                print(f"   Error in block: {e}")

    def collect_current_fundamentals(self):
        """
        [수정됨]
        1. Date -> Save_Date 로 변경
        2. Fiscal_Date (재무 기준일) 추가 및 이를 기준으로 중복 제거
        3. 이미 DB에 해당 분기(Fiscal_Date) 데이터가 있다면 저장하지 않음 (Strict Mode)
        """
        print("\n=== Collecting Current Fundamentals (Smart Deduplication) ===")

        # 1. 현재 Ticker 리스트 확보
        self.sp500_tool.fetch_data()
        current_tickers = list(self.sp500_tool.current_tickers)

        fundamental_data = []
        current_year = datetime.now().year
        table_name = f"YahooSP500_Fund_{current_year}"

        print(f"Target Tickers: {len(current_tickers)}")

        # 2. 데이터 수집
        for ticker in tqdm(current_tickers):
            try:
                stock = yf.Ticker(ticker)
                info = stock.info

                # (A) 재무 기준일 추출 (mostRecentQuarter)
                # Unix Timestamp를 날짜 객체로 변환
                mrq_timestamp = info.get('mostRecentQuarter')
                fiscal_date = None
                if mrq_timestamp:
                    fiscal_date = datetime.fromtimestamp(mrq_timestamp).date()

                data = {
                    'Save_Date': pd.Timestamp.now().normalize(),  # 수집 실행일
                    'Fiscal_Date': fiscal_date,  # 데이터의 기준 시점 (예: 2023-12-31)
                    'Ticker': ticker,
                    'Market_Cap': info.get('marketCap'),
                    'Forward_PE': info.get('forwardPE'),
                    'Trailing_PE': info.get('trailingPE'),
                    'Price_to_Book': info.get('priceToBook'),
                    'EBITDA': info.get('ebitda'),
                    'Revenue': info.get('totalRevenue'),
                    'Revenue_Growth': info.get('revenueGrowth'),
                    'Debt_to_Equity': info.get('debtToEquity'),
                    'Return_on_Equity': info.get('returnOnEquity')
                }
                fundamental_data.append(data)

            except Exception:
                continue

        if not fundamental_data:
            print("No fundamental data collected.")
            return

        df_fund = pd.DataFrame(fundamental_data)

        # Fiscal_Date가 없는 데이터(알 수 없음)는 제외할지 결정. 여기선 포함하되 NaT 처리.
        df_fund['Fiscal_Date'] = pd.to_datetime(df_fund['Fiscal_Date'])
        df_fund['Save_Date'] = pd.to_datetime(df_fund['Save_Date'])

        # 3. DB 중복 체크 및 저장
        try:
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table_name, schema='public')

            with self.engine.connect() as conn:
                df_to_insert = df_fund

                if table_exists:
                    # (B) DB에서 (Ticker, Fiscal_Date) 쌍을 조회하여 이미 존재하는지 확인
                    # Fiscal_Date는 날짜 타입이므로 형변환 주의
                    query = text(f'SELECT "Ticker", "Fiscal_Date" FROM "public".{table_name}')
                    existing_df = pd.read_sql(query, conn)

                    if not existing_df.empty:
                        # 비교를 위해 문자열 혹은 튜플로 변환하여 Set 생성
                        # Fiscal_Date가 NaT인 경우도 고려해야 함
                        existing_df['Fiscal_Date'] = pd.to_datetime(existing_df['Fiscal_Date'])

                        # (Ticker, Fiscal_Date) 튜플 집합 생성
                        existing_keys = set(zip(existing_df['Ticker'], existing_df['Fiscal_Date']))

                        # (C) 필터링: DB에 없는 (Ticker, Fiscal_Date) 조합만 남김
                        # apply를 써서 행별로 체크
                        df_to_insert = df_fund[
                            ~df_fund.apply(lambda row: (row['Ticker'], row['Fiscal_Date']) in existing_keys, axis=1)
                        ]

                if not df_to_insert.empty:
                    print(f"New quarterly data detected: Saving {len(df_to_insert)} rows to {table_name}...")

                    dtype_map = {
                        'Save_Date': Date(),
                        'Fiscal_Date': Date(),  # 핵심 기준일
                        'Ticker': String(10),
                        'Market_Cap': Numeric(30, 2),
                        'EBITDA': Numeric(30, 2),
                        'Revenue': Numeric(30, 2),
                        'Forward_PE': Numeric(18, 4),
                        'Trailing_PE': Numeric(18, 4),
                        # 필요한 경우 나머지 컬럼 추가
                    }

                    if not table_exists:
                        df_to_insert.to_sql(table_name, conn, schema='public', if_exists='fail', index=False,
                                            dtype=dtype_map)
                    else:
                        df_to_insert.to_sql(table_name, conn, schema='public', if_exists='append', index=False,
                                            dtype=dtype_map)

                    conn.commit()
                    print("Update complete.")
                else:
                    print("All tickers are up-to-date (No new fiscal quarters detected). Skipping insert.")

        except Exception as e:
            print(f"DB Error: {e}")

# ==========================================
# 3. 실행 코드
# ==========================================
if __name__ == "__main__":
    # PostgreSQL 연결 정보 (사용자 환경에 맞게 수정)
    db_id = 'postgres'  # PostgreSQL 사용자 ID
    db_pw = '0212'  # PostgreSQL 비밀번호
    db_host = 'localhost'  # DB 서버 주소 (e.g., 'localhost' or '192.168.1.10')
    db_port = '5432'  # PostgreSQL 기본 포트
    db_name = 'FDATA'  # 연결할 데이터베이스 이름

    # PostgreSQL용 create_engine 호출
    engine = create_engine(f'postgresql://{db_id}:{db_pw}@{db_host}:{db_port}/{db_name}')

    # 1. Historical Ticker Tool 준비
    sp500_tool = SP500History()

    # 2. Collector 준비
    collector = YahooSP500Collector(engine, sp500_tool)

    # 3. 수집 실행 (2000년 ~ 2024년)
    collector.collect_and_store(start_date="2000-01-01", end_date="2000-01-31")
    # collector.collect_current_fundamentals()