# crawler/producer_main_tw.py
import os
import pandas as pd
import shutil
from crawler.tasks_etf_list_tw import scrape_etf_list         # 匯入爬 ETF 清單的函式
from crawler.tasks_crawler_etf_tw import crawler_etf_daily_price, crawler_etf_dividend       # 匯入爬取 ETF 歷史價格與配息的函式
from crawler.tasks_backtest_utils_tw import calculate_indicators, evaluate_performance      # 匯入技術指標與績效分析的函式

from database.main import (
    write_etfs_to_db,   # 寫入 ETF 清單到資料庫
    write_etf_daily_price_to_db,    # 寫入 ETF 歷史價格與技術指標到資料庫
    write_etf_dividend_to_db,   # 寫入 ETF 配息到資料庫
    write_etf_backtest_results_to_db,   # 寫入 ETF 績效分析到資料庫
)


if __name__ == "__main__":
    # 控制是否要存 CSV
    SAVE_CSV = False


    # 0️⃣ 先爬 ETF 清單（名稱與代號），並儲存成 etf_list.csv
    print("開始 0️⃣ 爬 ETF 清單")
    etfs_df = scrape_etf_list.apply_async(kwargs={"save_csv": SAVE_CSV}).get()
    print(f"✅ 爬取到所有 ETF list")
    write_etfs_to_db(etfs_df)
    print("✅ ETF 清單已儲存到資料庫")


    # 1️⃣~3️⃣ 每一支 ETF 都要做的事情
    print("開始 🧩 處理所有 ETF 資料")
    ticker_list = etfs_df["etf_id"].dropna().tolist()

    for ticker in ticker_list:
        print(f"\n🎯 處理：{ticker}")

        try:
            # === 1️⃣ 抓歷史價格 + 技術指標計算 ===
            df_price = crawler_etf_daily_price.apply_async(args=[ticker]).get()
            print(df_price.head())   # ← 檢查歷史價格資料
            #print(df_price.columns)  # ← 確認欄位名稱對不對
            if df_price.empty:
                print(f"⚠️ 無歷史價格資料：{ticker}")
                continue

            # 檢查日期欄位是否為 datetime 格式，再加上技術指標欄位
            df_price["date"] = pd.to_datetime(df_price["date"])
            df_combined = calculate_indicators.apply_async(args=[df_price]).get()
            df_combined["date"] = pd.to_datetime(df_combined["date"])
            print(df_combined[["adj_close", "daily_return", "cumulative_return"]].tail())

            # 刪除價格欄為 NaN 的資料（避免寫入全空 row）
            key_cols = ["adj_close", "close", "high", "low", "open", "volume", "daily_return", "cumulative_return"]
            df_combined.dropna(subset=key_cols, how="all", inplace=True)

            # 若清完為空，就不要寫入
            if df_combined.empty:
                print(f"⚠️ 清除 NaN 後無有效價格資料：{ticker}")
                continue
            write_etf_daily_price_to_db(df_combined)
            print(f"✅ {ticker} 歷史價格與技術指標已儲存")

            # === 2️⃣ 抓配息資料 ===
            df_dividend = crawler_etf_dividend.apply_async(args=[ticker]).get()
            if not df_dividend.empty:
                write_etf_dividend_to_db(df_dividend)
                print(f"✅ {ticker} 配息資料已儲存")
            else:
                print(f"⚠️ 無配息資料：{ticker}")

            # === 3️⃣ 績效分析 ===
            metrics = evaluate_performance.apply_async(args=[df_combined]).get() 
            if metrics is not None:
                metrics["etf_id"] = ticker

                desired_order = [
                    "etf_id", "backtest_start", "backtest_end",
                    "total_return", "cagr", "max_drawdown", "sharpe_ratio"
                ]
                df_metrics = pd.DataFrame([metrics])  
                df_metrics = df_metrics[[col for col in desired_order if col in df_metrics]]
                write_etf_backtest_results_to_db(df_metrics)
                print(f"✅ {ticker} 績效分析已儲存")
            else:
                print(f"⚠️ 無法進行績效分析：{ticker}")

        except Exception as e:
            print(f"❌ 發生錯誤：{ticker} - {e}")

    print("✅ 全部 ETF 資料處理完成")

