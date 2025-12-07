import os
import platform

from FinanceCollector_FMP import Fmp_load
from FinanceCollector_KRX_loader import *
from Yfinance_INDVIX import *
from FinanceCollector_yf_SP500_loader import *
from FinanceCollector_yf_CEC_loader import *

# PostgreSQL 연결 정보 (사용자 환경에 맞게 수정)
db_id = 'postgres'      # PostgreSQL 사용자 ID
db_pw = '0212' # PostgreSQL 비밀번호
db_host = 'localhost'   # DB 서버 주소 (e.g., 'localhost' or '192.168.1.10')
db_port = '5432'        # PostgreSQL 기본 포트
db_name = 'FDATA'  # 연결할 데이터베이스 이름

# PostgreSQL용 create_engine 호출
engine = create_engine(f'postgresql://{db_id}:{db_pw}@{db_host}:{db_port}/{db_name}')

startdate = '20250101'
enddate = '20251201'

# today_YYYYMMDD = datetime.today().strftime('%Y%m%d')
# enddate = today_YYYYMMDD
last_done_ticker = None # 모든 것 Scan하려면 None


def shutdown_computer():
    """
    현재 실행 중인 운영체제에 맞춰 시스템 종료 명령어를 실행합니다.
    """
    os_type = platform.system()

    print(f"현재 운영체제: {os_type}")

    # 운영체제별 종료 명령어 설정
    if os_type == "Windows":
        print("1분 후 윈도우를 종료합니다.")
        # /s: 종료, /t 60: 60초 후 실행
        os.system("shutdown /s /t 60")
    elif os_type == "Linux" or os_type == "Darwin":  # Darwin은 macOS를 의미합니다.
        print("1분 후 시스템을 종료합니다. (sudo 권한 필요)")
        # -h: 시스템 중지(halt), +1: 1분 후 실행
        os.system("sudo shutdown -h +1")
    else:
        print("지원하지 않는 운영체제입니다.")


# 진행 상황 저장 파일
done_file = f'done_tickers_{enddate}.txt'
if os.path.exists(done_file):
    with open(done_file, 'r') as f:
        done_tickers = set(line.strip() for line in f)


if __name__ == '__main__':

    ## 한국 주식시장 (PyKRX)
    collector = PykrxMultiCollector(engine=engine)
    collector.collect_and_store_all(startdate, enddate, market='KOSPI')
    collector.collect_and_store_all(startdate, enddate, market='KOSDAQ')

    ## 미국 주식시장 (Yahoo Finance)
    # 1. Historical Ticker Tool 준비
    sp500_tool = SP500History()

    # 2. Collector 준비
    collector = YahooSP500Collector(engine, sp500_tool)

    # 3. 수집 실행 (2000년 ~ 2024년)
    collector.collect_and_store(start_date=f"{startdate[:4]}-{startdate[4:6]}-{startdate[6:]}"
                                , end_date=f"{enddate[:4]}-{enddate[4:6]}-{enddate[6:]}")
    # Yahoo Finance Fundamental은 현시점의 데이터만 수집
    # collector.collect_current_fundamentals()

    # ## Commodity & CryptoCurrency & Exchange Rate
    # asset_collector = YahooAssetCollector(engine)
    # # ----------------------------------------
    # # 1. 원자재 (Commodities)
    # # ----------------------------------------
    # commodity_tickers = ['GC=F', 'SI=F', 'HG=F', 'CL=F', 'PL=F', 'ZC=F', 'ZS=F', 'ZW=F', 'ZM=F', 'ZL=F'
    #     , 'KC=F', 'SB=F', 'CC=F', 'LE=F', 'HE=F']
    # asset_collector.collect_assets(
    #     tickers=commodity_tickers,
    #     start_date=f"{startdate[:4]}-{startdate[4:6]}-{startdate[6:]}",
    #     end_date=f"{enddate[:4]}-{enddate[4:6]}-{enddate[6:]}",
    #     table_prefix="Yahoo_Commodity"  # 테이블 예: Yahoo_Commodity_20202024
    # )
    #
    # # ----------------------------------------
    # # 2. 환율 (Forex)
    # # ----------------------------------------
    # forex_tickers = [
    #     'CNY=X', 'JPY=X', 'EUR=X', 'AUD=X', 'KRW=X', 'KRWJPY=X'
    # ]
    # asset_collector.collect_assets(
    #     tickers=forex_tickers,
    #     start_date=f"{startdate[:4]}-{startdate[4:6]}-{startdate[6:]}",
    #     end_date=f"{enddate[:4]}-{enddate[4:6]}-{enddate[6:]}",
    #     table_prefix="Yahoo_Forex"
    # )

    # # ----------------------------------------
    # # 3. 가상화폐 (Crypto)
    # # ----------------------------------------
    # # 암호화폐는 보통 2010년 이후부터 데이터가 존재함
    # crypto_tickers = [
    #     'BTC-USD', 'ETH-USD', 'RVN-USD', 'DOGE-USD', 'USDT-USD', 'BUSD-USD'
    # ]
    # asset_collector.collect_assets(
    #     tickers=crypto_tickers,
    #     start_date="2010-01-01",  # 암호화폐는 시작일 조정 가능
    #     end_date=f"{enddate[:4]}-{enddate[4:6]}-{enddate[6:]}",
    #     table_prefix="Yahoo_Crypto"
    # )


    ## FMP 유료버전
    # fmp_instance = Fmp_load(engine, startdate, enddate)
    # all_tickers = fmp_instance.get_sp500_and_nasdaq100_tickers()
    # # all_tickers = fmp_instance.get_all_us_tickers() # 미국 전체 TICKER가 필요한 경우
    # if last_done_ticker is not None:
    #     assert last_done_ticker in all_tickers, 'last_done_ticker는 all_tickers 중 하나여야 합니다'
    #     tickers_left = all_tickers[all_tickers.index(last_done_ticker) + 1:]
    # else:
    #     tickers_left = all_tickers


    # print("Collecting US Stock Prices...")
    # for ticker in tqdm(tickers_left, desc="Stocks"):
    #     fmp_instance.fetch_and_store_stock_price(ticker, startdate, enddate)
    #
    #     with open(done_file, 'a') as f:
    #         f.write(ticker + '\n')

    # print("\nCollecting US Stock Split History...")
    # for ticker in tqdm(tickers_left, desc="Splits"):
    #     fmp_instance.fetch_and_store_split_history(ticker, startdate, enddate)

    # print("\nCollecting US Financial Statements...")
    # financial_endpoints = {
    #     "cash-flow-statement": "fmp_us_cashflow",
    #     "income-statement": "fmp_us_incomestatement",
    #     "balance-sheet-statement": "fmp_us_balancesheet",
    #     "key-metrics": "fmp_us_keymetric"
    # }
    # for endpoint, table_name in financial_endpoints.items():
    #     print(f"\nCollecting {endpoint}...")
    #     for ticker in tqdm(tickers_left, desc=endpoint):
    #         fmp_instance.fetch_and_store_financial_data(ticker, endpoint, table_name, limit='3') # limit 조정 가능

    # print("\nCollecting Commodity Prices...")
    # fmp_instance.comm_et(startdate, enddate)
    #
    # print("\nCollecting Exchange Rates...")
    # fmp_instance.exchage_et(startdate, enddate)

    # print("\nCollecting Crypto Prices...")
    # fmp_instance.crypto_et(startdate, enddate)

# shutdown_computer()






