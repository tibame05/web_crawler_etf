from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import pandas as pd
from database.main import write_etfs_to_db

from crawler.worker import app

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def etf_list_us(url):
    print("開始爬取美國 ETF 名單...")

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
            region = "US"  # 手動補上國別
            currency = "USD"  # 手動補上幣別
            etf_data.append((code, name,region,currency))

    driver.quit()

    df = pd.DataFrame(etf_data, columns=["etf_id", "etf_name", "region", "currency"])

    print(f"etf_List_us: {df.head()}")

    write_etfs_to_db(df)

    # return df