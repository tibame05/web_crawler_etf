# crawler/tasks_crawler_etf_tw.py
from typing import Dict, Any, List, Optional
import pandas as pd
import yfinance as yf

from crawler import logger
from database.main import write_etf_daily_price_to_db, write_etf_dividend_to_db  
from crawler.worker import app
from crawler.tasks_etf_list_tw import _get_currency_from_region

def _today_str() -> str:
    return pd.Timestamp.today().strftime("%Y-%m-%d")

@app.task()
def fetch_daily_prices(etf_id: str, plan: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    依 plan 的區間抓取 ETF 的歷史日價格（trade_date），並寫入 DB。

    參數：
        etf_id (str): ETF 代號，例如 "0050.TW"
        plan (Dict[str, Any]): 包含抓取區間的字典，應包含 "start" (str, YYYY-MM-DD)
    
    回傳：
        Optional[Dict[str, str]]: 
            如果寫入成功，回傳包含 'etf_id', 'latest_date' (str) 和 'new_records_count' (int) 的字典；
            否則回傳 None。
    """

    # 若 plan 空，表示無新資料，不需抓
    if not plan:
        logger.info("[FETCH][PRICE] %s 無需抓取（plan 空）", etf_id)
        return [] # 回傳空的 list

    # 取得開始結束時間
    start_str = plan.get("start")
    end_str = _today_str()
    logger.info("[FETCH][PRICE] %s %s → %s", etf_id, start_str, end_str)

    # 抓取歷史價格資料
    price_dataframe = yf.download(etf_id, start=start_str, end=end_str, auto_adjust=False, progress=False)

    if price_dataframe.empty or "Volume" not in price_dataframe:
        logger.info("[FETCH][PRICE] %s 無資料", etf_id)
        return []   # 若無資料則跳過該 ETF

    # 處理表頭問題（如果多層表頭）
    if isinstance(price_dataframe.columns, pd.MultiIndex):
        price_dataframe.columns = price_dataframe.columns.droplevel(1)

    # 標準化欄位名稱：全部轉小寫並將空格轉為底線
    price_dataframe.columns = price_dataframe.columns.str.replace(" ", "_").str.lower()

    # 'date' 是 index，所以要先 reset_index
    price_dataframe.reset_index(inplace=True)

    # 資料處理：去除成交量為 0 的列，並用前值補齊缺值
    price_dataframe = price_dataframe[price_dataframe["volume"] > 0].ffill()

    # 統一欄位格式
    output = pd.DataFrame({
        "etf_id": etf_id,
        "trade_date": price_dataframe["date"].dt.strftime("%Y-%m-%d"),
        "high": price_dataframe["high"].astype(float),
        "low": price_dataframe["low"].astype(float),
        "open": price_dataframe["open"].astype(float),
        "close": price_dataframe["close"].astype(float),
        "adj_close": price_dataframe.get("adj_close", price_dataframe["close"]).astype(float),
        "volume": price_dataframe["volume"].astype("int64"),
    })

    rows: List[Dict[str, Any]] = output.to_dict(orient="records")
    new_records_count = len(rows) # 取得新增筆數

    # 寫入 DB
    if rows:
        write_etf_daily_price_to_db(rows)
        logger.info("✅ %s 日價格已寫入 DB（%d 筆）", etf_id, len(rows))

        # 取得最後一筆資料的 'trade_date'
        latest_date = output["trade_date"].iloc[-1]
        
        # 回傳包含 etf_id、最新日期和筆數的字典
        return {
            "etf_id": etf_id, 
            "price_latest_date": latest_date,
            "price_new_records_count": new_records_count
        }    
    # 如果 rows 為空，則回傳 None
    return None

@app.task()
def fetch_dividends(etf_id: str, plan: Dict[str, Any], region: str) -> Optional[Dict[str, str]]:
    """
    依 plan 的區間抓取 ETF 配息資料 (ex_date)，並寫入 DB。

    參數：
        etf_id (str): ETF 代號，例如 "0050.TW"
        plan (Dict[str, Any]): 包含抓取區間的字典，應包含 "start"。
        region (str): ETF 交易地區，用於判斷幣別 (例如 'TW' 或 'US')。

    回傳：
        Optional[Dict[str, Any]]: 
            如果寫入成功，回傳包含 'etf_id', 'latest_date' (str) 和 'new_records_count' (int) 的字典；
            否則回傳 None。
    """
    # 若 plan 空，表示無新資料，不需抓
    if not plan:
        logger.info("[FETCH][DIV] %s 無需抓取（plan 空）", etf_id)
        return [] # 回傳空的 list

    # 取得開始結束時間
    start_str = plan.get("start")
    end_str = _today_str()
    logger.info("[FETCH][DIV] %s %s → %s", etf_id, start_str, end_str)

    # 判斷幣別
    currency = _get_currency_from_region(region, etf_id)
    
    # 抓取配息資料
    try:
        dividends_series = yf.Ticker(etf_id).dividends
    except Exception as e:
        logger.error("[FETCH][DIV] %s 抓取 yfinance 資料失敗: %s", etf_id, e)
        return []

    if dividends_series is None or dividends_series.empty:
        logger.info("[FETCH][DIV] %s 無配息資料", etf_id)
        return []

    # 區間篩選 (使用第一版更精確的 Pandas 索引篩選)
    start_ts = pd.Timestamp(start_str)
    end_ts = pd.Timestamp(end_str)

    # 篩選指定區間內的資料 (注意 yfinance 的日期索引是 ex_date)
    filtered_series = dividends_series[(dividends_series.index >= start_ts) & (dividends_series.index <= end_ts)]

    if filtered_series.empty:
        logger.info("[FETCH][DIV] %s 指定區間 (%s → %s) 無配息資料", etf_id, start_str, end_str)
        return []

    # 資料轉換與格式化
    dividend_dataframe = dividends_series.reset_index()

    # 統一欄位名稱並格式化
    output = pd.DataFrame({
        "etf_id": etf_id,
        "ex_date": dividend_dataframe["Date"].dt.strftime("%Y-%m-%d"), # yfinance 的 Index 變為 'Date'
        "dividend": dividend_dataframe["Dividends"].astype(float),     # yfinance 的配息欄位是 'Dividends'
        "currency": currency,
    })

    # 轉換成列表供寫入 DB
    rows: List[Dict[str, Any]] = output.to_dict(orient="records")
    new_records_count = len(rows) # 取得新增筆數

    # 寫入 DB
    if rows:
        write_etf_dividend_to_db(rows)
        logger.info("✅ %s 配息資料已寫入 DB（%d 筆）", etf_id, len(rows))

        # 取得最後一筆資料的 'ex_date'
        latest_date = output["ex_date"].iloc[-1]
        
        # 回傳包含 etf_id、最新日期和筆數的字典
        return {
            "etf_id": etf_id, 
            "dividend_latest_date": latest_date,
            "dividend_new_records_count": new_records_count
        }    
    # 如果 rows 為空，則回傳 None
    return None
