# crawler/tasks_crawler_etf_tw.py
import pandas as pd
import yfinance as yf

from crawler.worker import app
from database.main import write_etf_dividend_to_db

@app.task()
def crawler_etf_daily_price(
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
    df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)

    if df.empty or "Volume" not in df:
        print(f"⚠️ {ticker} 沒有價格資料")
        return {}  # 若無資料則跳過該 ETF

    # 資料處理：去除成交量為 0 的列，並用前值補齊缺值
    df = df[df["Volume"] > 0].ffill()
    df.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

    # 處理表頭問題（如果多層表頭）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    df.reset_index(inplace=True)
    df.insert(0, "etf_id", ticker)

    # 將所有欄位名稱轉為小寫
    df.columns = df.columns.str.lower()

    return df.to_dict(orient="records")

@app.task()
def crawler_etf_dividend(ticker: str):
    """
    根據傳入的 ETF DataFrame 抓取單一 ETF 配息資料。

    參數：
        etf_df (pd.DataFrame): ETF 基本資料表格，應包含欄位 'etf_id'

    回傳：
        pd.DataFrame: 所有 ETF 的配息資料合併結果
    """

    # 逐一處理每一檔 ETF
    # 抓取配息資料
    dividends = yf.Ticker(ticker).dividends
    if dividends.empty:
        return

    df = dividends.reset_index()
    df.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
    
    # 將日期轉為 "YYYY-MM-DD" 格式（去掉時間與時區）
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # 新增欄位：etf_id 和 currency
    df.insert(0, "etf_id", ticker)
    df["currency"] = "TWD"

    df_dividend = df[["etf_id", "date", "dividend_per_unit", "currency"]]

    if not df_dividend.empty:
        write_etf_dividend_to_db(df_dividend)
        print(f"✅ {ticker} 配息資料已儲存")
    else:
        print(f"⚠️ 無配息資料：{ticker}")

    # return df_dividend.to_dict(orient="records")
