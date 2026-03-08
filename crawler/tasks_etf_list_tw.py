# crawler/tasks_etf_list_tw.py
import requests
import os
from bs4 import BeautifulSoup
from typing import List

from database.main import write_etfs_to_db
from crawler.worker import app
from crawler import logger
from database import SessionLocal

def _get_currency_from_region(region: str, etf_id: str) -> str:
    """
    依 ETF 交易地區判斷幣別。

    參數：
        region (str): ETF 交易地區，例如 'TW' 或 'US'
        etf_id (str): ETF 代號，用於 log 訊息

    回傳：
        str: 幣別代碼
    """
    if region == 'TW':
        return "TWD"
    elif region == 'US':
        return "USD"
    else:
        # 預設值或錯誤處理
        currency = "UNKNOWN"
        logger.warning("[CURRENCY] %s 地區 %s 無法判定幣別，設為 %s", etf_id, region, currency)
        return currency

@app.task(name="crawler.tasks_etf_list_tw.fetch_tw_etf_list")
def fetch_tw_etf_list(crawler_url: str = "https://tw.stock.yahoo.com/tw-etf", region: str = "TW") -> List[dict]:
    """
    從 Yahoo 財經抓取台灣 ETF 清單，並整理為 list of dict：
      - etf_id:   ETF 代號（大寫，結尾為 .TW 或 .TWO）
      - etf_name: ETF 名稱
      - region:   固定 "TW"
      - currency: 固定 "TWD"
    若 etf_id 不符合格式，則略過。
    回傳:
      list of dict，可直接給 align_step0 使用。
    """
    try:
        user_agent = os.environ["USER_AGENT"]
    except KeyError:
        logger.error("❌ 找不到環境變數 USER_AGENT。請檢查 .env 檔案是否正確設定。")
        raise  # 重新拋出例外，確保程式停止    headers = {"User-Agent": user_agent}

    headers = {"User-Agent": user_agent}

    with SessionLocal.begin() as session:
        logger.info("開始爬取台灣 ETF 名單...")
        try:
            response = requests.get(crawler_url, headers=headers, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"連線失敗: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        etf_records = []

        # 改用 CSS Selector 定位所有列表項目
        items = soup.select('li.List\(n\)')

        for item in items:
            name_div = item.select_one('div.Lh\(20px\)')
            id_span = item.select_one('span.Fz\(14px\)')

            if name_div and id_span:
                # 取得名稱並去掉代號部分的文字 (如果有的話)
                etf_name_text = name_div.get_text(strip=True).replace(id_span.get_text(strip=True), "").strip()
                etf_id_text = id_span.get_text(strip=True)

                # --- 驗證與格式化 etf_id ---
                etf_id_text = etf_id_text.upper()
                if not (etf_id_text.endswith(".TW") or etf_id_text.endswith(".TWO")):
                    continue

                currency = _get_currency_from_region(region, etf_id_text)
                etf_records.append({
                    "etf_id": etf_id_text,
                    "etf_name": etf_name_text,
                    "region": region,
                    "currency": currency,
                })

        if etf_records:
            try:
                for r in etf_records:
                    r.setdefault("expense_ratio", None)
                    r.setdefault("inception_date", None)
                    r.setdefault("status", "active") # 或給空字串
                write_etfs_to_db(etf_records, session=session)
                logger.info("✅ 台股 ETF 已寫入資料庫（共 %d 筆）", len(etf_records))
            except Exception as e:
                logger.exception("❌ 台股 ETF 寫入資料庫失敗: %s", e)        
        else:
            logger.warning("⚠️ 未取得任何合法的台股 ETF 記錄。請檢查 Headers 或 Class 名稱。")

        return etf_records
    