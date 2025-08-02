import pandas as pd
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


from crawler.worker import app
from database.main import write_etf_dividend_to_db

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def crawler_etf_dps_us(url):


    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)

    driver.get(url)

    # 等待表格載入
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    etf_data = []

    # 逐列抓取
    rows = soup.select("table tbody tr")
    for row in rows:
        code_tag = row.select_one('a[href^="/symbols/"]')
        name_tag = row.select_one("sup")
        
        if code_tag and name_tag:
            code = code_tag.get_text(strip=True)
            name = name_tag.get_text(strip=True)
            #currency = "USD"  # 固定幣別
            etf_data.append((code, name))

    driver.quit()
    #df = pd.DataFrame(etf_data, columns=['id', 'name','region','currency'])
    #etf_df = pd.DataFrame(etf_data, columns=['id', 'name', 'currency'])
    etf_codes = [code for code, _ in etf_data]

    for ticker in etf_codes:
        # 抓取配息資料
        dividends = yf.Ticker(ticker).dividends
        if not dividends.empty:
            dividends_df = dividends.reset_index()
            dividends_df.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
            dividends_df["date"] = dividends_df["date"].dt.date  # 只保留年月日
            dividends_df.insert(0, "etf_id", ticker)  # 新增股票代碼欄位，放第一欄
            dividends_df.insert(3, "currency", "USD")  # 新增欄位，放第一欄

        else:
            print(f"{ticker} 沒有配息資料")

    write_etf_dividend_to_db(dividends_df)

    # return dividends_df