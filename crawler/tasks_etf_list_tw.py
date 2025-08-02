# crawler/tasks_etf_list_tw.py
import bs4 as bs
import requests
import pandas as pd
import os

from crawler.worker import app
from database.main import write_etfs_to_db


@app.task()
def scrape_etf_list(
    output_path="crawler/output/output_etf_number/etf_list.csv", save_csv: bool = False
):
    """
    從 Yahoo 財經抓取台灣 ETF 名稱與代碼，並儲存成 TSV 檔案。

    參數：
        output_path (str): 儲存檔案的路徑，預設為 output/output_etf_number/etf_list.csv
    """

    print("開始爬取台灣 ETF 名單...")

    url = "https://tw.stock.yahoo.com/tw-etf"
    resp = requests.get(url)
    html = bs.BeautifulSoup(resp.text, "html.parser")

    etf_list = []
    headers = html.find_all("div", {"class": "Bdbc($bd-primary-divider)"})

    for block in headers:
        ETF_name = block.find("div", {"class": "Lh(20px)"})
        ETF_number = block.find("span", {"class": "Fz(14px)"})

        ETF_name_text = ETF_name.text.strip() if ETF_name else "N/A"
        ETF_number_text = ETF_number.text.strip() if ETF_number else "N/A"

        etf_list.append({
            "etf_id": ETF_number_text,
            "etf_name": ETF_name_text,
            "region": "TW",
            "currency": "TWD",
        })

    df = pd.DataFrame(etf_list)

    # 移除 etf_id 為 "N/A" 的行
    df = df[~df["etf_id"].isin(["N/A", None, ""])]

    if save_csv:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, sep="\t", encoding="utf-8", index=False)
        print(f"已儲存 ETF 名單至：{output_path}")

    write_etfs_to_db(df)
    print("✅ ETF 清單已儲存到資料庫")

    return df.to_dict(orient="records")