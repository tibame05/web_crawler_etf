import pandas as pd
import yfinance as yf
from crawler.worker import app
from database.main import write_etf_dividend_to_db

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def crawler_etf_dps_us(etf_list_df):

    all_dividends = [] 
    for etf in etf_list_df:
        ticker = etf['etf_id']

        try: 
            dividends = yf.Ticker(ticker).dividends
            if not dividends.empty:
                dividends_df = dividends.reset_index()
                dividends_df.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
                dividends_df["date"] = dividends_df["date"].dt.date  # 只保留年月日
                dividends_df.insert(0, "etf_id", ticker)  # 新增股票代碼欄位，放第一欄
                dividends_df.insert(3, "currency", "USD")  # 新增欄位，放第一欄
                all_dividends.append(dividends_df)  # 加入到 list 中
            else:
                print(f"{ticker} 沒有配息資料")
        except Exception as e:
            print(f"{ticker} 發生錯誤: {e}")
    
    if all_dividends:
        etf_dividends_df = pd.concat(all_dividends, ignore_index=True)
        print(etf_dividends_df.head())
        write_etf_dividend_to_db(etf_dividends_df)
    else:
        print("沒有任何 ETF 有配息資料。")