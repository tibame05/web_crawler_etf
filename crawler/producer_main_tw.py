# crawler/producer_main_tw.py
import pandas as pd
from crawler.tasks_etf_list_tw import scrape_etf_list  # 匯入爬 ETF 清單的函式
from crawler.tasks_crawler_etf_tw import (
    crawler_etf_daily_price,
    crawler_etf_dividend,
)  # 匯入爬取 ETF 歷史價格與配息的函式
from crawler.tasks_backtest_utils_tw import (
    calculate_indicators,
    evaluate_performance,
)  # 匯入技術指標與績效分析的函式


if __name__ == "__main__":
    # 控制是否要存 CSV
    SAVE_CSV = False


    # 0️⃣ 先爬 ETF 清單（名稱與代號），並儲存成 etf_list.csv
    print("開始 0️⃣ 爬 ETF 清單")
    etfs = scrape_etf_list.apply_async(
        kwargs={"save_csv": SAVE_CSV}, queue="crawler_tw"
    ).get()
    etfs_df = pd.DataFrame(etfs)
    print(f"爬取到 {len(etfs_df)} 筆 ETF 資料")
    print("✅ 爬取到所有 ETF list")


    # 1️⃣~3️⃣ 每一支 ETF 都要做的事情
    print("開始 🧩 處理所有 ETF 資料")
    ticker_list = etfs_df["etf_id"].dropna().tolist()

    for ticker in ticker_list:
        print(f"\n🎯 處理：{ticker}")

        try:
            # === 1️⃣ 抓歷史價格 + 技術指標計算 ===
            # df_price = crawler_etf_daily_price.apply_async(args=[ticker], queue="crawler_tw").get()
            daily_price = crawler_etf_daily_price.apply_async(
                args=[ticker], queue="crawler_tw"
            ).get()
            df_price = pd.DataFrame(daily_price)
            print(df_price.head())   # ← 檢查歷史價格資料
            #print(df_price.columns)  # ← 確認欄位名稱對不對
            if df_price.empty:
                print(f"⚠️ 無歷史價格資料：{ticker}")
                continue

            # 檢查日期欄位是否為 datetime 格式，再加上技術指標欄位
            df_price["date"] = pd.to_datetime(df_price["date"])
            # df_combined = calculate_indicators.apply_async(args=[df_price], queue="crawler_tw").get()
            combined = calculate_indicators.apply_async(
                args=[df_price], queue="crawler_tw"
            ).get()
            df_combined = pd.DataFrame(combined)
            print(f"✅ {ticker} 歷史價格與技術指標已儲存")

            # === 2️⃣ 抓配息資料 ===
            # df_dividend = crawler_etf_dividend.apply_async(args=[ticker], queue="crawler_tw").get()
            crawler_etf_dividend.apply_async(args=[ticker], queue="crawler_tw")

            # === 3️⃣ 績效分析 ===
            # metrics = evaluate_performance.apply_async(args=[df_combined]).get()
            evaluate_performance.apply_async(
                args=[df_combined, ticker], queue="crawler_tw"
            )

        except Exception as e:
            print(f"❌ 發生錯誤：{ticker} - {e}")

    print("✅ 全部 ETF 資料處理完成")


