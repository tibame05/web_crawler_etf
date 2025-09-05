# crawler/producer_main_tw.py
import pandas as pd
from crawler.tasks_etf_list_tw import fetch_tw_etf_list  # 匯入爬 ETF 清單的函式
from crawler.tasks_crawler_etf_tw import (
    fetch_tw_etf_daily_price,
    fetch_tw_etf_dividends,
)  # 匯入爬取 ETF 歷史價格與配息的函式
from crawler.tasks_backtest_utils_tw import (
    calculate_technical_indicators,
    evaluate_backtest_performance,
)  # 匯入技術指標與績效分析的函式
from crawler import logger  # 使用自訂的 logger

if __name__ == "__main__":
    # 控制是否要存 CSV
    SAVE_CSV = False


    # 0️⃣ 先爬 ETF 清單（名稱與代號），並儲存成 etf_list.csv
    print("開始 0️⃣ 爬 ETF 清單")
    etf_records = fetch_tw_etf_list.apply_async(
        kwargs={"save_csv": SAVE_CSV}, queue="crawler_tw"
    ).get()
    etf_list_dataframe = pd.DataFrame(etf_records)
    print(f"爬取到 {len(etf_list_dataframe)} 筆 ETF 資料")
    print("✅ 爬取到所有 ETF list")


    # 1️⃣~3️⃣ 每一支 ETF 都要做的事情
    print("開始 🧩 處理所有 ETF 資料")
    etf_id_list = etf_list_dataframe["etf_id"].dropna().tolist()

    for ticker in etf_id_list:
        print(f"\n🎯 處理：{ticker}")

        try:
            # === 1️⃣ 抓歷史價格 + 技術指標計算 ===
            # price_dataframe = fetch_tw_etf_daily_price.apply_async(args=[ticker], queue="crawler_tw").get()
            daily_price_records = fetch_tw_etf_daily_price.apply_async(
                args=[ticker], queue="crawler_tw"
            ).get()
            price_dataframe = pd.DataFrame(daily_price_records)
            print(price_dataframe.head())   # ← 檢查歷史價格資料
            #print(price_dataframe.columns)  # ← 確認欄位名稱對不對
            if price_dataframe.empty:
                print(f"⚠️ 無歷史價格資料：{ticker}")
                continue

            # 檢查日期欄位是否為 datetime 格式，再加上技術指標欄位
            price_dataframe["date"] = pd.to_datetime(price_dataframe["date"])
            # enriched_price_dataframe = calculate_technical_indicators.apply_async(args=[price_dataframe], queue="crawler_tw").get()
            enriched_records = calculate_technical_indicators.apply_async(
                args=[price_dataframe], queue="crawler_tw"
            ).get()
            enriched_price_dataframe = pd.DataFrame(enriched_records)
            print(f"✅ {ticker} 歷史價格與技術指標已儲存")

            # === 2️⃣ 抓配息資料 ===
            # df_dividend = fetch_tw_etf_dividends.apply_async(args=[ticker], queue="crawler_tw").get()
            fetch_tw_etf_dividends.apply_async(args=[ticker], queue="crawler_tw")

            # === 3️⃣ 績效分析 ===
            # metrics = evaluate_backtest_performance.apply_async(args=[enriched_price_dataframe]).get()
            evaluate_backtest_performance.apply_async(
                args=[enriched_price_dataframe, ticker], queue="crawler_tw"
            )

        except Exception as e:
            print(f"❌ 發生錯誤：{ticker} - {e}")

    print("✅ 全部 ETF 資料處理完成")


