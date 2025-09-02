# crawler/tasks_etf_list_tw.py
import bs4 as bs
import requests
import pandas as pd
import os

from crawler.worker import app
from database.main import write_etfs_to_db


@app.task()
def fetch_tw_etf_list(
    output_path="crawler/output/output_etf_number/etf_list.csv", save_csv: bool = False
):
    """
    從 Yahoo 財經抓取台灣 ETF 名稱與代碼，並儲存成 TSV 檔案。

    參數：
        output_path (str): 儲存檔案的路徑，預設為 output/output_etf_number/etf_list.csv
    """

    print("開始爬取台灣 ETF 名單...")

    crawler_url = "https://tw.stock.yahoo.com/tw-etf"
    response = requests.get(crawler_url)
    soup = bs.BeautifulSoup(response.text, "html.parser")

    etf_records = []
    etf_card_divs = soup.find_all("div", {"class": "Bdbc($bd-primary-divider)"})

    for etf_card in etf_card_divs:
        etf_name_div = etf_card.find("div", {"class": "Lh(20px)"})
        etf_id_span = etf_card.find("span", {"class": "Fz(14px)"})

        etf_name_text = etf_name_div.text.strip() if etf_name_div else "N/A"
        etf_id_text = etf_id_span.text.strip() if etf_id_span else "N/A"

        etf_records.append({
            "etf_id": etf_id_text,
            "etf_name": etf_name_text,
            "region": "TW",
            "currency": "TWD",
        })

    etf_list_dataframe = pd.DataFrame(etf_records)

    # 移除 etf_id 為 "N/A" 的行
    etf_list_dataframe = etf_list_dataframe[~etf_list_dataframe["etf_id"].isin(["N/A", None, ""])]

    if save_csv:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        etf_list_dataframe.to_csv(output_path, sep="\t", encoding="utf-8", index=False)
        print(f"已儲存 ETF 名單至：{output_path}")

    write_etfs_to_db(etf_list_dataframe)
    print("✅ ETF 清單已儲存到資料庫")

    return etf_list_dataframe.to_dict(orient="records")