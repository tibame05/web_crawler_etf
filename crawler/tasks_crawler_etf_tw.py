# crawler/tasks_crawler_etf_tw.py
import pandas as pd
import yfinance as yf

from crawler.worker import app
from database.main import write_etf_dividend_to_db

@app.task()
def fetch_tw_etf_daily_price(
    ticker: str, start_date: str = "2015-01-01", end_date: str = None
):
    """
    根據傳入的 ETF DataFrame（欄位需包含 'etf_id'）抓取單一 ETF 歷史價格。
    
    參數：
        etf_df (pd.DataFrame): ETF 基本資料表格，應包含欄位 'etf_id'

    回傳：
        pd.DataFrame: 所有 ETF 的歷史價格資料彙總結果（合併後的 DataFrame）
    """

    # 逐一處理每一檔 ETF
    if end_date is None:
        end_date = pd.Timestamp.today().strftime('%Y-%m-%d') # 結束日期為今天

    # 抓取歷史價格資料
    price_dataframe = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)

    if price_dataframe.empty or "Volume" not in price_dataframe:
        print(f"⚠️ {ticker} 沒有價格資料")
        return {}  # 若無資料則跳過該 ETF

    # 資料處理：去除成交量為 0 的列，並用前值補齊缺值
    price_dataframe = price_dataframe[price_dataframe["Volume"] > 0].ffill()
    price_dataframe.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

    # 處理表頭問題（如果多層表頭）
    if isinstance(price_dataframe.columns, pd.MultiIndex):
        price_dataframe.columns = price_dataframe.columns.droplevel(1)

    price_dataframe.reset_index(inplace=True)
    price_dataframe.insert(0, "etf_id", ticker)

    # 將所有欄位名稱轉為小寫
    price_dataframe.columns = price_dataframe.columns.str.lower()

    return price_dataframe.to_dict(orient="records")

@app.task()
def fetch_tw_etf_dividends(ticker: str):
    """
    根據傳入的 ETF DataFrame 抓取單一 ETF 配息資料。

    參數：
        etf_df (pd.DataFrame): ETF 基本資料表格，應包含欄位 'etf_id'

    回傳：
        pd.DataFrame: 所有 ETF 的配息資料合併結果
    """

    # 逐一處理每一檔 ETF
    # 抓取配息資料
    dividends_series = yf.Ticker(ticker).dividends
    if dividends_series.empty:
        return

    dividend_dataframe = dividends_series.reset_index()
    dividend_dataframe.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
    
    # 將日期轉為 "YYYY-MM-DD" 格式（去掉時間與時區）
    dividend_dataframe["date"] = dividend_dataframe["date"].dt.strftime("%Y-%m-%d")

    # 新增欄位：etf_id 和 currency
    dividend_dataframe.insert(0, "etf_id", ticker)
    dividend_dataframe["currency"] = "TWD"

    dividend_output_dataframe = dividend_dataframe[["etf_id", "date", "dividend_per_unit", "currency"]]

    if not dividend_output_dataframe.empty:
        write_etf_dividend_to_db(dividend_output_dataframe)
        print(f"✅ {ticker} 配息資料已儲存")
    else:
        print(f"⚠️ 無配息資料：{ticker}")

    # return dividend_output_dataframe.to_dict(orient="records")
