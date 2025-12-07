import pandas as pd
import os

import pandas as pd
import sqlalchemy as sqla
from fredapi import Fred
from sqlalchemy import text
from tqdm import tqdm


# data = web.DataReader(indicators.keys(), 'fred', '19900101', '20240410')
# data = data.rename(columns=indicators)
# data = data.reset_index().rename({'DATE': 'TDATE'}, axis=1)
# data['TDATE'] = pd.to_datetime(data['TDATE']).dt.strftime('%Y%m%d')
#
# id = 'system'
# pw = '1234'
# dsn = "127.0.01:1521/XE"
# engine = create_engine(f'oracle://{id}:{pw}@{dsn}')
#
# dtyp = {}
# if engine.connect().dialect.name == 'oracle':
#     for column in data.columns:
#         if data[column].dtype == 'object':
#             dtyp[column] = sqla.types.VARCHAR(data[column].astype(str).str.len().max())
#         elif data[column].dtype in ['float', 'float64']:
#             dtyp[column] = sqla.FLOAT
#
# data.to_sql('FREDMACRO', engine, if_exists='append', index=False, dtype=dtyp)


def FREDDataExt(engine_in, start_date, end_date):
    fred = Fred(api_key=os.environ.get('fredkey'))
    indicators = {
        'GDPC1': 'GDPC1',  # Real Gross Domestic Product, Q
        'CBIC1': 'CBIC1',  # Change in Real Private Inventories, Q
        'DSPIC96': 'DSPIC96',  # Real Disposable Personal Income, M
        'USREC': 'USREC', # NBER based Recession Indicators for the United States from the Period following the Peak through the Trough, M
        'T10Y3M': 'T10Y3M',  # 10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity, D
        'T10Y2Y': 'T10Y2Y',  # 10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity, D
        'FEDTARRHLR': 'FEDTARRHLR',  # Longer Run FOMC Summary of Economic Projections for the Fed Funds Rate, Range, High, -
        'FEDTARRLLR': 'FEDTARRLLR',  # Longer Run FOMC Summary of Economic Projections for the Fed Funds Rate, Range, Low, -
        'FEDTARRMLR': 'FEDTARRMLR',  # Longer Run FOMC Summary of Economic Projections for the Fed Funds Rate, Range, Midpoint, -
        'DFEDTAR': 'DFEDTAR',  # Federal Funds Target Rate (DISCONTINUED), D
        'CPIAUCSL': 'CPIAUCSL',  # Consumer Price Index for All Urban Consumers: All Items in U.S. City Average, M
        'CPILFESL': 'CPILFESL',  # Consumer Price Index for All Urban Consumers: All Items Less Food and Energy in U.S. City Average, M
        'PPIACO': 'PPIACO',  # Producer Price Index by Commodity: All Commodities, M
        'PCE': 'PCE',  # Personal Consumption Expenditures, M
        'PCEDG': 'PCEDG',  # Personal Consumption Expenditures: Durable Goods, M
        'PCEND': 'PCEND',  # Personal Consumption Expenditures: Nondurable Goods, M
        'DPCCRC1M027SBEA': 'DPCCRC1M027SBEA',  # Personal consumption expenditures excluding food and energy, M
        'DCOILWTICO': 'DCOILWTICO', # Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma Dollars per Barrel, D
        'FEDFUNDS': 'FEDFUNDS',  # Federal Funds Effective Rate, M
        'M2REAL': 'M2REAL',  # Real M2 Money Stock, M
        'RRPTSYD': 'RRPTSYD',  # Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations, D
        'WRBWFRBL': 'WRBWFRBL', # Liabilities and Capital: Other Factors Draining Reserve Balances: Reserve Balances with Federal Reserve Banks: Wednesday Level, W (지급준비금)
        # 은행이 고객의 예금 인출 요구에 대비해 연준에 일정비율 예치 : 시중의 유동성 수준을 가늠. COVID이후 주가지수와 매우 밀접한 상관관계 (지준금의 증가 > 은행의 대출 증가)
        'WDTGAL': 'WDTGAL', # Liabilities and Capital: Liabilities: Deposits with F.R. Banks, Other Than Reserve Balances: U.S. Treasury, General Account: Wednesday Level, W (재무부의 TGA잔고)
        # TGA는 재무부의 수입과 지출이 오가는 통장을 연준에 예치해놓은 것. 재정 적자 발생 시 국채 발행: 시장 참여자들이 국채 입찰할 경우, 은행의 지급준비금 감소 > TGA 잔고 증가 / 재정지출할 경우 TGA잔고 감소 > 지준금 증가
        'WLRRAL': 'WLRRAL',  # Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Wednesday Level, W (연준의 역레포 계좌잔고) - MMF, 사모펀드, 딜러 등이 본인의 여유 자금을 연준에 예치
        'WSHOSHO': 'WSHOSHO', # Assets: Securities Held Outright: Securities Held Outright: Wednesday Level, W (연준의 SOMA계정)
        # SOMA - 연준이 보유하고 있는 증권을 관리하는 계좌 : 양적완화(QE)를 시행하면서 국채를 매입하면 SOMA 계정의 잔고가 증가 / 양적긴축(QT)를 시행하면서 국채를 축소하면 SOMA 계정의 잔고가 감소
        'WLCFLL': 'WLCFLL', # Assets: Liquidity and Credit Facilities: Loans: Wednesday Level, W (연준의 대출지원 창구 e.g. Discount Window, PPPLF, BTFP)
        'RRPONTSYD': 'RRPONTSYD', # Overnight Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve in the Temporary Open Market Operations
        'MYAGM2CNM189N': 'MYAGM2CNM189N',  # M2 for China, M
        'MYAGM2EZM196N': 'MYAGM2EZM196N',  # M2 for Europe, M
        'MYAGM2JPM189S': 'MYAGM2JPM189S',  # M2 for Japan, M
        'UNRATE': 'UNRATE',  # Unemployment Rate, M
        'NROU': 'NROU',  # Noncyclical Rate of Unemployment, Q
        'HOUST': 'HOUST',  # New Privately-Owned Housing Units Started: Total Units, M
        'CSUSHPINSA': 'CSUSHPINSA',  # S&P CoreLogic Case-Shiller U.S. National Home Price Index, M
        'WALCL': 'WALCL',  # Assets: Total Assets: Total Assets (Less Eliminations from Consolidation): Wednesday Level, W
        'NFCI': 'NFCI',  # Chicago Fed National Financial Conditions Index, W
        'ICSA': 'ICSA',  # Initial Claims, W
        'CCSA': 'CCSA',  # Continued Claims (Insured Unemployment), W
        'HSN1F': 'HSN1F',  # New One Family Houses Sold: United States, M
        'NHSUSSPT': 'NHSUSSPT',  # New Houses Sold by Sales Price in the United States, Total, M
        'NFCINONFINLEVERAGE': 'NFCINONFINLEVERAGE',  # Chicago Fed National Financial Conditions Index Nonfinancial Leveral Subindex,W
        'UMCSENT': 'UMCSENT',  # University of Michigan: Consumer Sentiment, M
        'BAMLH0A0HYM2': 'BAMLH0A0HYM2',  # ICE BofA US High Yield Index Option-Adjusted Spread, D
        'LNS11300060': 'LNS11300060',  # Labor Force Participation Rate - 25-54 Yrs, M
        'ADPWNUSNERSA': 'ADPWNUSNERSA',  # Total Nonfarm Private Payroll Employment, W
        'CES0500000003': 'CES0500000003',  # Average Hourly Earnings of All Employees, Total Private, M
        'DGORDER': 'DGORDER',  # Manufacturers' New Orders: Durable Goods, M
        'AMDMVS': 'AMDMVS',  # Manufacturers' Value of Shipments: Durable Goods, M
        'PCUOMFGOMFG': 'PCUOMFGOMFG',  # Producer Price Index by Industry: Total Manufacturing Industries, M
        'AMNMNO': 'AMNMNO',  # Manufacturers' New Orders: Nondurable Goods, M
        'PCOPPUSDM': 'PCOPPUSDM',  # Global price of Copper, M
        'DCOILWTICO': 'DCOILWTICO',  # Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma, D
        'DCOILBRENTEU': 'DCOILBRENTEU',  # Crude Oil Prices: Brent - Europe, D
        'GASREGW': 'GASREGW',  # US Regular All Formulations Gas Price, W
        'DHHNGSP': 'DHHNGSP',  # Henry Hub Natural Gas Spot Price, D
        'PWHEAMTUSDM': 'PWHEAMTUSDM',  # Global price of Wheat, M
        'PALUMUSDM': 'PALUMUSDM',  # Global price of Al, M
        'PMAIZMTUSDM': 'PMAIZMTUSDM',  # Global price of Corn, M
        'PSOYBUSDM': 'PSOYBUSDM',  # Global price of Soybeans, M
        'DHOILNYH': 'DHOILNYH',  # No. 2 Heating Oil Prices: New York Harbor, D
        'T10YIE': 'T10YIE',  # 10-Year Breakeven Inflation Rate, D
        'DFEDTARU': 'DFEDTARU',  # Federal Funds Target Range - Upper Limit, D
        'VIXCLS': 'VIXCLS',  # CBOE Volatility Index: VIX, D
        'DEXJPUS': 'DEXJPUS',  # Japanese Yen to U.S. Dollar Spot Exchange Rate, D
        'DEXCHUS': 'DEXCHUS',  # Chinese Yuan Renminbi to U.S. Dollar Spot Exchange Rate, D
        'CBBTCUSD': 'CBBTCUSD',  # Coinbase Bitcoin, D
        'USEPUINDXD': 'USEPUINDXD',  # Economic Policy Uncertainty Index for United States, D
        'BAMLC0A1CAAA': 'BAMLC0A1CAAA',  # ICE BofA AAA US Corporate Index Option-Adjusted Spread , D
        'GVZCLS': 'GVZCLS',  # CBOE Gold ETF Volatility Index, D
        'OVXCLS': 'OVXCLS',  # CBOE Crude Oil ETF Volatility Index, D
        'DEXUSAL': 'DEXUSAL',  # U.S. Dollars to Australian Dollar Spot Exchange Rate, D
        'AAAFF': 'AAAFF',  # Moody's Seasoned Aaa Corporate Bond Minus Federal Funds Rate, D
        'T3MFF': 'T3MFF',  # 3-Month Treasury Constant Maturity Minus Federal Funds Rate, D
        'CBETHUSD': 'CBETHUSD',  # Coinbase Ethereum, D
        'BAMLEMRACRPIASIAOAS': 'BAMLEMRACRPIASIAOAS', # ICE BofA Asia Emerging Markets Corporate Plus Index Option-Adjusted Spread, D
        'INFECTDISEMVTRACKD': 'INFECTDISEMVTRACKD',  # Equity Market Volatility: Infectious Disease Tracker, D
        'VXFXICLS': 'VXFXICLS',  # CBOE China ETF Volatility Index (DISCONTINUED), D
        'EVZCLS': 'EVZCLS',  # CBOE EuroCurrency ETF Volatility Index, D
        'BAMLEM1RAAA2ALCRPIUSEY': 'BAMLEM1RAAA2ALCRPIUSEY',  # ICE BofA AAA-A US Emerging Markets Liquid Corporate Plus Index Effective Yield, D
        'VXXLECLS': 'VXXLECLS',  # CBOE Energy Sector ETF Volatility Index (DISCONTINUED), D
        'FDHBFIN': 'FDHBFIN' # Federal Debt Held by Foreign and International Investors
    }

    extra_parameters = {
        "observation_start": start_date,
        "observation_end": end_date,
    }

    error_list = []
    for key, value in tqdm(indicators.items()):
        new_table = False
        try:
            conn = engine_in.connect()
            df_stored = pd.read_sql_query(
                text(f"SELECT * FROM \"C##FINANCE\".fred_macro_table WHERE TICKER='{key}'"), conn)
            conn.close()

        except:
            print('Oracle Table will be newly created!')
            new_table = True
        # 테이블 또는 뷰가 존재하지 않습니다

        try:
            temp_data = pd.DataFrame(fred.get_series(key, **extra_parameters), columns=['value'])
            if temp_data.shape[1] == 0:
                temp_data['Date'] = '19991231'
            temp_data['TICKER'] = key
            temp_data = temp_data.reset_index().rename(columns={'index': 'TDATE'})
            temp_data['TDATE'] = pd.to_datetime(temp_data['TDATE']).dt.strftime('%Y%m%d')
            dtyp = {}
            if engine_in.connect().dialect.name == 'oracle':
                for column in temp_data.columns:
                    if temp_data[column].dtype == 'object':
                        dtyp[column] = sqla.types.VARCHAR(22)  # max([len(str(i)) for i in NAS_SPTicker]) : 6
                    elif temp_data[column].dtype in ['float', 'float64']:
                        dtyp[column] = sqla.FLOAT

            # 해당 지표가 없는거랑 테이블이 없는거랑 다른 문제

            if new_table:
                temp_data.to_sql('fred_macro_table', engine_in, if_exists='append', index=False
                                 , dtype=dtyp, schema='C##FINANCE')
                print("There is no Table. All extrated data stored!", key)
            else:
                # A_temp와 A_stored의 ticker와 tdate 열을 기준으로 병합
                df_stored = df_stored.rename({'tdate': 'TDATE', 'ticker': 'TICKER'}, axis=1)
                merged_df = pd.merge(temp_data, df_stored[['TDATE', 'TICKER']], on=['TDATE', 'TICKER'], how='left',
                                     indicator=True)
                # A_stored에서 A_temp에 없는 행만 선택
                filtered_A_stored = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge']).dropna()

                if len(filtered_A_stored) != 0:
                    print(key, ': length: ', len(filtered_A_stored), 'Newly Added\n')
                    filtered_A_stored.to_sql('fred_macro_table', engine_in, if_exists='append', index=False
                                             , dtype=dtyp, schema='C##FINANCE')
                else:
                    print("There is no New record", key)

        except:
            print('error occured: ', key)
            error_list.append(key)
