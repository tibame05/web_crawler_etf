from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import os
import csv
import pandas as pd

from crawler.worker import app

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def etf_list_us(url):
    os.makedirs("Output", exist_ok=True)

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

    df = pd.DataFrame(etf_data, columns=['id', 'name','region','currency'])

    for row in rows[:3]:  # 測試前3列
        cols = row.find_all("td")
        print([col.text.strip() for col in cols])

    print("抓到的美股ETF代碼與名稱：")
    for code, name, region, currency in etf_data:
        print(f"{code} - {name}- {region}- {currency}")

    with open("Output/us_etf_list.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(['etf_id', 'etf_name','region','currency'])
        writer.writerows(etf_data)
    print("已成功寫入 us_etf_list.csv")
    return df