import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
from datetime import datetime

from database.main import write_etfs_to_db
from crawler.worker import app

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def etf_list_us(crawler_url):
    # 發送 HTTP 請求獲取網站內容
    response = requests.get(crawler_url)
    response.encoding = 'utf-8'  # 確保中文編碼正確

    # 解析 HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    etf_codes = []
    # 解析表格數據
    rows = soup.select("table tbody tr")
    for row in rows:
        code_tag = row.select_one('a[href^="/symbols/"]')
        name_tag = row.select_one("sup")
        
        if code_tag and name_tag:
            code = code_tag.get_text(strip=True)
            name = name_tag.get_text(strip=True)
            region = "US"  # 手動補上國別
            currency = "USD"  # 手動補上幣別
            try:
                etf = yf.Ticker(code)
                info = etf.info
                expense_ratio = info.get("netExpenseRatio", None)
                inception_ts = info.get("fundInceptionDate", None)
                inception_date = (
                datetime.fromtimestamp(inception_ts).strftime("%Y-%m-%d") if inception_ts else None
                        )
            except Exception as e:
                        print(f"查詢 {code} 時發生錯誤: {e}")
                        expense_ratio = None
                        inception_date = None

            etf_codes.append((code, name, region, currency, expense_ratio, inception_date))
            
    # 將資料放入 DataFrame
    etf_list_df = pd.DataFrame(etf_codes, columns=["etf_id", "etf_name", "region", "currency",
                                               "expense_ratio","inception_date"])
    write_etfs_to_db(etf_list_df)
    return etf_list_df.to_dict(orient="records")
