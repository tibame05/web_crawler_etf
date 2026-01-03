# crawler/tasks_etf_list_us.py
import requests
from bs4 import BeautifulSoup

from database.main import write_etfs_to_db
#from crawler.worker import app
from crawler import logger
from crawler.tasks_etf_list_tw import _get_currency_from_region

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
#@app.task()
def fetch_us_etf_list(crawler_url: str = "https://tw.tradingview.com/markets/etfs/funds-usa/", region: str = "US"):
    """
    從指定 crawler_url 抓取美國 ETF 清單，並回傳 list of dict：
      - etf_id:   ETF 代號（必須為大寫英文字母）
      - etf_name: ETF 名稱
      - region:   固定 "US"
      - currency: 固定 "USD"
    回傳:
        list of dict，可直接給 align_step0 使用。    
      """
    logger.info("開始爬取美股 ETF 名單...")

    # 發送 HTTP 請求獲取網站內容
    response = requests.get(crawler_url)
    response.encoding = 'utf-8'  # 確保中文編碼正確

    # 解析 HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    etf_records = []
    # 解析表格數據
    rows = soup.find_all("span", {"class": "tickerCell-GrtoTeat"})

    for row in rows:
        code_tag = row.find("a", {"class": "tickerNameBox-GrtoTeat"})
        name_tag = row.find("a", {"class": "tickerDescription-GrtoTeat"})
        
        etf_id_text = code_tag.get_text(strip=True) if code_tag else None
        etf_name_text = name_tag.get_text(strip=True) if name_tag else None

        # --- 驗證與格式化 etf_id ---
        if not etf_id_text:
            continue

        etf_id_text = str(etf_id_text).upper().strip()

        # 必須全為數字和字母
        if not etf_id_text.isalnum():
            logger.warning("忽略不符合格式的 ETF 代號: %s", etf_id_text)
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
            logger.info("✅ 美股 ETF 已寫入資料庫（共 %d 筆）", len(etf_records))
        except Exception as e:
            logger.exception("❌ 美股 ETF 寫入資料庫失敗: %s", e)
    else:
        logger.warning("⚠️ 未取得任何合法的美股 ETF 記錄")

    return etf_records
