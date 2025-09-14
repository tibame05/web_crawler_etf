# crawler/tasks_etf_list_tw.py
import requests
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timezone
from bs4 import BeautifulSoup as bs
import numpy as np
import time

from database.main import write_etfs_to_db
from crawler.worker import app
from crawler import logger


@app.task()
def fetch_tw_etf_list():
    """
    從 Yahoo 財經抓取台灣 ETF 清單，並用 yfinance 補充：
      - expense_ratio（小數；0.005 代表 0.5%）
      - inception_date（YYYY-MM-DD）
    若判斷下市（近一個月 history 為空），直接排除，不寫入 DB。   
    """
    logger.info("開始爬取台灣 ETF 名單...")

    crawler_url = "https://tw.stock.yahoo.com/tw-etf"
    response = requests.get(crawler_url)
    soup = bs.BeautifulSoup(response.text, "html.parser")

    etf_records = []
    etf_card_divs = soup.find_all("div", {"class": "Bdbc($bd-primary-divider)"})

    for etf_card in etf_card_divs:
        etf_name_div = etf_card.find("div", {"class": "Lh(20px)"})
        etf_id_span = etf_card.find("span", {"class": "Fz(14px)"})

        etf_name_text = etf_name_div.text.strip() if etf_name_div else None
        etf_id_text = etf_id_span.text.strip() if etf_id_span else None

        if not etf_id_text:
            continue

        expense_ratio = None
        inception_date = None

        try:
            # etf_id 格式防呆處理
            yf_symbol = f"{etf_id_text}.TW" if "." not in etf_id_text else etf_id_text
            ticker = yf.Ticker(yf_symbol)

            # 取得基本資訊
            try:
                info = ticker.get_info()
            except Exception:
                info = getattr(ticker, "info", {}) or {}

            # 📌 費用率優先順序
            exp = (
                info.get("netExpenseRatio") # 投資人實際支付的淨費用率
                or info.get("annualReportExpenseRatio") # 年報披露的費用率
                or info.get("expenseRatio") # 泛用欄位（最後 fallback）
            )
            if exp is not None:
                try:
                    exp = float(exp)
                    if exp > 1.0:
                        exp /= 100.0
                    if exp >= 0:
                        expense_ratio = round(exp, 6)
                except Exception:
                    expense_ratio = None

            # 成立日
            inc_raw = info.get("fundInceptionDate", None) or info.get("firstTradeDateEpochUtc", None)
            if inc_raw is not None and not (isinstance(inc_raw, float) and np.isnan(inc_raw)):
                try:
                    ts = float(inc_raw)
                    inception_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                except Exception:
                    inception_date = str(inc_raw)

            # 📌 判斷是否下市
            hist = ticker.history(period="1mo", interval="1d")
            if hist.empty:
                logger.warning(f"台股 ETF {etf_id_text} 判斷為下市，已跳過")
                continue

        except Exception as e:
            logger.error(f"[TW] yfinance 查詢 {etf_id_text} 失敗：{e}")
            continue
    
        etf_records.append({
            "etf_id": etf_id_text,
            "etf_name": etf_name_text,
            "region": "TW",
            "currency": "TWD",
            "expense_ratio": expense_ratio,
            "inception_date": inception_date,
        })

        time.sleep(0.3)  # 避免被 yfinance 擋掉

    etf_list_dataframe = pd.DataFrame(etf_records)
    write_etfs_to_db(etf_list_dataframe)
    logger.info("✅ 台股 ETF 已寫入資料庫（下市已過濾）")

    return etf_list_dataframe.to_dict(orient="records")