# crawler/tasks_etf_list_tw.py
import requests
from bs4 import BeautifulSoup
from typing import List

from database.main import write_etfs_to_db
#from crawler.worker import app
from crawler import logger

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

#@app.task()
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
    logger.info("開始爬取台灣 ETF 名單...")

    response = requests.get(crawler_url)
    soup = BeautifulSoup(response.text, "html.parser")

    etf_records = []
    etf_card_divs = soup.find_all("div", {"class": "Bdbc($bd-primary-divider)"})

    for etf_card in etf_card_divs:
        etf_name_div = etf_card.find("div", {"class": "Lh(20px)"})
        etf_id_span = etf_card.find("span", {"class": "Fz(14px)"})

        etf_name_text = etf_name_div.text.strip() if etf_name_div else None
        etf_id_text = etf_id_span.text.strip() if etf_id_span else None

        # --- 驗證與格式化 etf_id ---
        if not etf_id_text:
            continue
    
        etf_id_text = str(etf_id_text).upper().strip()

        # 確認尾綴必須是 ".TW" 或 ".TWO"
        if not (etf_id_text.endswith(".TW") or etf_id_text.endswith(".TWO")):
            logger.warning("忽略不符合格式的 ETF 代號: %s", etf_id_text)
            continue

        # 前綴為數字與英文字母組合，例如 0050、00715B
        prefix = etf_id_text.rsplit(".", 1)[0]
        if not prefix.isalnum():  # 前綴必須是數字或字母
            logger.warning("忽略前綴不是數字和字母的 ETF 代號: %s", etf_id_text)
            continue

        # --- 判斷 region 與 currency ---
        currency = _get_currency_from_region(region, etf_id_text)

        etf_records.append({
            "etf_id": etf_id_text,
            "etf_name": etf_name_text,
            "region": region,
            "currency": currency,
        })

    # --- 直接用 list of dict 寫入 DB ---
    if etf_records:
        try:
            write_etfs_to_db(etf_records)
            logger.info("✅ 台股 ETF 已寫入資料庫（共 %d 筆）", len(etf_records))
        except Exception as e:
            logger.exception("❌ 台股 ETF 寫入資料庫失敗: %s", e)
    else:
        logger.warning("⚠️ 未取得任何合法的台股 ETF 記錄")

    # --- 回傳 list of dict ---
    return etf_records