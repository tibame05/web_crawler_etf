# crawler/producer_main_tw.py
import os
import pandas as pd
import shutil
from crawler.tasks_etf_list_tw import scrape_etf_list         # 匯入爬 ETF 清單的函式
from crawler.tasks_crawler_etf_tw import crawler_etf_data, crawler_etf_dividend_data       # 匯入爬取 ETF 歷史價格與配息的函式
from crawler.tasks_backtest_utils_tw import calculate_indicators, evaluate_performance      # 匯入技術指標與績效分析的函式

from database.main import (
    write_etfs_to_db,   # 寫入 ETF 清單到資料庫
    write_etf_daily_price_to_db,    # 寫入 ETF 日常價格到資料庫
    write_etf_dividend_to_db,   # 寫入 ETF 配息到資料庫
    write_etf_indicators_to_db,   # 寫入 ETF 技術指標到資料庫
    write_etf_backtest_performance_to_db,   # 寫入 ETF 績效分析到資料庫
    read_etf_price_from_db,   # 從資料庫讀取 ETF 歷史價格
)


if __name__ == "__main__":
    # 控制是否要存 CSV
    SAVE_CSV = False


    # 0️⃣ 先爬 ETF 清單（名稱與代號），並儲存成 etf_list.csv
    print("開始 0️⃣ 爬 ETF 清單")
    csv_path = "crawler/output/output_etf_number/etf_list.csv"
    etfs_df = scrape_etf_list.apply_async(kwargs={
        "output_path": csv_path,
        "save_csv": SAVE_CSV
    }).get()
    write_etfs_to_db(etfs_df)


    # 1️⃣ 根據 ETF 清單下載歷史價格與配息資料
    print("開始 1️⃣ 下載歷史價格與配息資料")
    # 歷史價格
    etf_daily_price_df = crawler_etf_data.apply_async(args=[etfs_df], kwargs={
        "save_csv": SAVE_CSV
    }).get()
    write_etf_daily_price_to_db(etf_daily_price_df)

    # 配息資料
    etf_dividend_df = crawler_etf_dividend_data.apply_async(args=[etfs_df], kwargs={
        "save_csv": SAVE_CSV
    }).get()
    write_etf_dividend_to_db(etf_dividend_df)


    # 2️⃣ 進行技術指標計算與績效分析
    print("開始 2️⃣ 進行技術指標計算與績效分析")
    if SAVE_CSV:
        output_dir = "crawler/output/output_with_indicators"              # 存儲含技術指標的結果
        performance_dir = "crawler/output/output_backtesting_metrics"     # 儲存績效評估報表
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(performance_dir, exist_ok=True)

    summary_list = []   # 儲存每支 ETF 的績效指標結果

    # 針對每一個 ETF 歷史資料檔做分析
    ticker_list = etfs_df["etf_id"].dropna().tolist()

    for ticker in ticker_list:
        try:
            # 讀取歷史價格資料
            df = read_etf_price_from_db(ticker)

            if df.empty:
                print(f"❌ 沒有資料：{ticker}")
                continue

            # 把日期轉為 datetime 格式
            df['date'] = pd.to_datetime(df['date'])

            # 計算技術指標
            etf_indicators_df = calculate_indicators.apply_async(args=[df]).get()
            write_etf_indicators_to_db(etf_indicators_df)

            # 儲存技術指標結果CSV
            if SAVE_CSV:
                indicator_path = os.path.join(output_dir, f"{ticker}_with_indicators.csv")
                etf_indicators_df.to_csv(indicator_path, index=False)

            # 計算績效指標
            metrics = evaluate_performance.apply_async(args=[etf_indicators_df]).get()
            if metrics is None:
                print(f"❌ Error processing {ticker}: invalid data")
                continue
            metrics["etf_id"] = ticker
            summary_list.append(metrics)
            print(f"✅ 已完成：{ticker}的技術指標計算與績效分析")

        except Exception as e:
            print(f"❌ Error processing {ticker}: {e}")

    # === 匯出回測績效指標 ===
    etf_backtest_df = pd.DataFrame(summary_list)

    # 指定欄位輸出順序
    desired_order = ["etf_id", "backtest_start", "backtest_end", "total_return", "cagr", "max_drawdown", "sharpe_ratio"]
    etf_backtest_df = etf_backtest_df[
        [col for col in desired_order if col in etf_backtest_df.columns]
    ]

    # 匯出 summary
    if SAVE_CSV:
        summary_csv_path = os.path.join(performance_dir, "backtesting_performance_summary.csv")
        etf_backtest_df.to_csv(summary_csv_path, index=False)

    write_etf_backtest_performance_to_db(etf_backtest_df)

    # ✅ 任務完成後清除 output 資料夾
    if not SAVE_CSV:
        shutil.rmtree("crawler/output", ignore_errors=True)

    print("✅ 技術指標與績效分析完成")
