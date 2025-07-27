# crawler/tasks_crawler_etf_tw.py
import os
import pandas as pd
import yfinance as yf

from crawler.worker import app

@app.task()
def crawler_etf_data(
    etf_df: pd.DataFrame, 
    save_csv: bool = False,
    start_date: str = "2015-01-01", end_date: str = None
) -> pd.DataFrame:
    """
    根據傳入的 ETF DataFrame（欄位需包含 'etf_id'）抓取歷史價格，必要時儲存為 CSV。
    
    參數：
        etf_df (pd.DataFrame): ETF 基本資料表格，應包含欄位 'etf_id'
        save_csv (bool): 是否儲存中繼 CSV 檔（預設 False）

    回傳：
        pd.DataFrame: 所有 ETF 的歷史價格資料彙總結果（合併後的 DataFrame）
    """

    # 建立歷史價格資料夾
    if save_csv:
        historical_dir = "crawler/output/output_historical_price_data"
        os.makedirs(historical_dir, exist_ok=True)

    # 讀取 ETF 編號清單（例如 etf_list.csv）
    etf_df.columns = etf_df.columns.str.strip()
    ticker_list = etf_df["etf_id"].dropna().tolist()

    etf_price_df_all = pd.DataFrame()

    # 逐一處理每一檔 ETF
    for ticker in ticker_list:
        print(f"下載：{ticker}")
        if end_date is None:
            end_date = pd.Timestamp.today().strftime('%Y-%m-%d') # 結束日期為今天

        # 1️⃣ 抓取歷史價格資料
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)

        if df.empty:
            print(f"⚠️ {ticker} 沒有價格資料")
            continue    # 若無資料則跳過該 ETF
        
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

        # 儲存價格資料
        if save_csv:
            df.to_csv(f"{historical_dir}/{ticker}.csv", index=False)

        # 將資料存入
        etf_price_df_all = pd.concat([etf_price_df_all, df], ignore_index=True)


    return etf_price_df_all

@app.task()
def crawler_etf_dividend_data(
    etf_df: pd.DataFrame, 
    save_csv: bool = False
) -> pd.DataFrame:
    """
    根據傳入的 ETF DataFrame 抓取配息資料，必要時儲存為 CSV。

    參數：
        etf_df (pd.DataFrame): ETF 基本資料表格，應包含欄位 'etf_id'
        save_csv (bool): 是否儲存中繼 CSV 檔（預設 False）

    回傳：
        pd.DataFrame: 所有 ETF 的配息資料合併結果
    """
    # 建立配息子資料夾
    if save_csv:
        dividend_dir = "crawler/output/output_dividends"
        os.makedirs(dividend_dir, exist_ok=True)

    # 讀取 ETF 編號清單（例如 etf_list.csv）
    etf_df.columns = etf_df.columns.str.strip()
    ticker_list = etf_df["etf_id"].dropna().tolist()
    etf_dividend_df = pd.DataFrame()

    # 逐一處理每一檔 ETF
    for ticker in ticker_list:
        print(f"下載：{ticker}")

        # 2️⃣ 抓取配息資料
        dividends = yf.Ticker(ticker).dividends
        if not dividends.empty:
            dividends_df = dividends.reset_index()
            dividends_df.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
            
            # 將日期轉為 "YYYY-MM-DD" 格式（去掉時間與時區）
            dividends_df["date"] = dividends_df["date"].dt.strftime("%Y-%m-%d")

            # 新增欄位：etf_id 和 currency
            dividends_df.insert(0, "etf_id", ticker)
            dividends_df["currency"] = "TWD"
            
            # 指定欄位順序
            dividends_df = dividends_df[["etf_id", "date", "dividend_per_unit", "currency"]]
            etf_dividend_df = pd.concat(
                [etf_dividend_df, dividends_df], ignore_index=True
            )

            # 儲存 CSV
            if save_csv:
                dividends_df.to_csv(f"{dividend_dir}/{ticker}_dividends.csv", index=False, encoding="utf-8-sig")
        else:
            print(f"{ticker} 沒有配息資料")

    return etf_dividend_df