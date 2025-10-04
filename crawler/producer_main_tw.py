# -*- coding: utf-8 -*-
"""
台股整體管線（TW only）
流程：
  0) 啟動與參數記錄
  A) 名單對齊（資料來源：Yahoo TW → DB 對照）
  B) STEP1 規劃與抓取（價格、股利）
  C) STEP2 建 TRI（含前置查詢）
  D) 回測（輸入 TRI、輸出績效）
回傳：
{
  "summary": {...},
  "per_etf": {
     "0050.TW": {"plan":{...}, "fetch":{...}, "sync":{...}, "tri":{...}, "backtests":{...}},
     ...
  }
}
"""

from __future__ import annotations
from typing import Dict, Any, List, Set
from datetime import datetime

from crawler import logger
from crawler.config import DEFAULT_START_DATE, REGION_TW
from crawler.worker import app  # 僅初始化；實際呼叫在程式內

# --- 爬蟲／計算任務（僅引用，不在此實作） ---
from crawler.tasks_etf_list_tw import fetch_tw_etf_list
from crawler.tasks_align import align_step0
from crawler.tasks_sync import plan_price_fetch, plan_dividend_fetch
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends
from crawler.tasks_tri import build_tri
from crawler.tasks_backtests import compute_backtests

# --- DB 介面（database/main.py 提供；此處只呼叫，不實作細節） ---
from database.main import (
    write_etl_sync_status_to_db,
    read_etl_sync_status,
    upsert_etl_sync_status, replace_etl_sync_status_bulk,
    # C 組：STEP2 建 TRI 前置
    read_last_tri_point, read_prices_range, read_dividends_range,
    get_latest_tri_date, count_etf_tris, read_etf_region_currency, write_etf_tris_to_db,
    # D 組：回測
    read_tris_range, replace_backtests_for_etf, write_etf_backtest_results_to_db,
)


def main_tw() -> Dict[str, Any]:
    """
    【台股 ETF 同步主流程】

    用途：
    作為台股資料同步的總入口點，負責調度整個 ETL (Extract-Transform-Load) 流程。
    此函式專注於流程控制、日誌記錄和結果彙整，不涉及具體的抓取或計算邏輯。

    步驟概覽：
    1.  初始化：記錄開始時間，準備結果回傳的字典結構。
    2.  步驟 A (名單對齊)：從外部來源 (Yahoo股市) 獲取最新的 ETF 名單，
        與資料庫中的現有名單進行比對，確保我們處理的是最活躍、正確的 ETF。
        同時，將新發現的 ETF 加入到同步狀態追蹤表中。
    3.  步驟 B (規劃與抓取)：遍歷所有活躍的 ETF，逐一規劃並執行價格和股利資料的增量更新。
        - 規劃 (Plan)：檢查每檔 ETF 的資料庫紀錄，決定需要從哪個日期開始補齊資料。
        - 抓取 (Fetch)：執行爬蟲或 API 呼叫，獲取缺失的資料。
        - 同步 (Sync)：將新抓取的資料寫入資料庫，並更新同步狀態追蹤表。
        ...
    4.  完成：記錄結束時間與總耗時，回傳詳細的執行結果。
    """
    t0 = datetime.now()
    logger.info("【台股 ETF 資訊同步】主流程啟動...")
    result: Dict[str, Any] = {
        "summary": {
            "region": REGION_TW,
            "start_date": DEFAULT_START_DATE,
            "started_at": t0.isoformat(timespec="seconds"),
            "n_etf": 0,
            "errors": [],
        },
        "per_etf": {}
    }

    try:
        # A) STEP0：ETF 名單對齊  (Align ETF List)
        #  目標：確保我們的 ETF 清單與市場上最新的保持一致，並初始化新加入的 ETF。
        logger.info("===== 步驟 A：同步 ETF 名單開始 =====")

        # 1. 從 Yahoo 股市抓取完整的 ETF 名單
        crawler_url = "https://tw.stock.yahoo.com/tw-etf"
        src_rows = fetch_tw_etf_list(crawler_url, REGION_TW)
        logger.info("步驟 A.1：自 Yahoo 股市成功爬取 %d 筆原始 ETF 名單。", len(src_rows))

        # 2. 進行名單對齊、資料清洗與補充 (例如：透過 yfinance 補齊發行日等資訊)
        etfs_data_list = align_step0(region=REGION_TW, src_rows=src_rows, use_yfinance=True)

        # 3. 取得最終處理的活躍 ETF 清單
        final_active_ids: Set[str] = {data['etf_id'] for data in etfs_data_list}
        result["summary"]["n_etf"] = len(final_active_ids)
        logger.info("步驟 A.2：經過濾與對齊後，最終需處理的活躍 ETF 名單共 %d 筆。", len(final_active_ids))
        
        # 4. 處理同步狀態表 (etl_sync_status)，將新發現的 ETF 加入追蹤
        try:
            # 讀取資料庫中已在追蹤的 ETF
            existing_sync_rows = read_etl_sync_status(region=REGION_TW) or []
            existing_sync_ids: Set[str] = {row['etf_id'] for row in existing_sync_rows}
            logger.info("步驟 A.3：資料庫 `etl_sync_status` 中已存在 %d 筆 ETF 追蹤紀錄。", len(existing_sync_ids))

            # 計算哪些 ETF 是新加入的
            new_sync_ids_to_add = sorted(final_active_ids - existing_sync_ids)

            if new_sync_ids_to_add:
                logger.info("步驟 A.4：發現 %d 筆新 ETF 需加入追蹤：%s", len(new_sync_ids_to_add), new_sync_ids_to_add)
                rows_to_insert = [
                    {
                        "etf_id": eid,
                        "last_price_date": None,
                        "price_count": 0,
                        "last_dividend_ex_date": None,
                        "dividend_count": 0,
                        "last_tri_date": None,
                        "tri_count": 0,
                        "updated_at": None,
                    }
                    for eid in new_sync_ids_to_add
                ]
                write_etl_sync_status_to_db(rows_to_insert)
                logger.info("步驟 A.5：已成功將 %d 筆新 ETF 的初始狀態寫入資料庫。", len(rows_to_insert))
            else:
                logger.info("步驟 A.4：無新發現的 ETF，不需更新追蹤表。")

        except Exception as e:
            error_msg = f"處理 `etl_sync_status` 新增 ETF 追蹤時發生錯誤: {e}"
            logger.exception("【錯誤】%s", error_msg)
            # 此為關鍵步驟，若出錯仍記錄錯誤，但流程繼續，避免影響後續資料抓取
            result["summary"]["errors"].append({"etf_id": "N/A", "stage": "SYNC_INIT", "error": error_msg})
        
        logger.info("===== 步驟 A：同步 ETF 名單完成 =====")

        # ------------------------------------------------------------
        # B) STEP1 規劃與抓取 (Plan & Fetch)
        #  目標：逐一處理每檔 ETF，為其規劃並抓取價格與股利資料。
        logger.info("===== 步驟 B：規劃與抓取 ETF 詳細資料開始 =====")
        per_etf = result["per_etf"]

        for etf_info in etfs_data_list:
            eid = etf_info["etf_id"]
            inception_date = etf_info["inception_date"]  # 從已對齊的資料中取得成立日期
            
            logger.info("--- 開始處理 ETF: %s ---", eid)

            # 為每檔 ETF 初始化結果記錄結構
            per_etf[eid] = {
                "plan": {"price": {}, "dividend": {}},
                "fetch": {"price": {}, "dividend": {}},
                "sync": {}, "tri": {}, "backtests": {},
            }
            
            # 初始化同步狀態變數，用於後續更新
            current_last_price_date = None
            current_price_count = 0
            current_last_dividend_ex_date = None
            current_dividend_count = 0

            try:
                # --- B.1 價格資料處理 ---
                logger.info("[%s] 步驟 B.1.1：規劃價格 (Price) 資料抓取區間...", eid)
                plan_p_result = plan_price_fetch(eid, inception_date=inception_date)
                per_etf[eid]["plan"]["price"] = plan_p_result or {}
                
                initial_price_count = int(plan_p_result.get("price_count", 0)) if plan_p_result else 0

                if plan_p_result:
                    plan_p = {"start": plan_p_result["start"]}
                    logger.info("[%s] 步驟 B.1.2：執行價格資料抓取 (計畫=%s)...", eid, plan_p)
                    got_p_result = fetch_daily_prices(eid, plan_p)
                    per_etf[eid]["fetch"]["price"] = got_p_result or {}
                    
                    if got_p_result:
                        new_records_p = got_p_result.get("price_new_records_count", 0)
                        current_last_price_date = got_p_result.get("price_latest_date")
                        current_price_count = initial_price_count + new_records_p # 總數 = 舊 + 新
                else:
                    # 若無需規劃，表示資料已最新，數量維持不變
                    current_price_count = initial_price_count

                # --- B.2 股利資料處理 ---
                logger.info("[%s] 步驟 B.2.1：規劃股利 (Dividend) 資料抓取區間...", eid)
                plan_d_result = plan_dividend_fetch(eid, inception_date=inception_date)
                per_etf[eid]["plan"]["dividend"] = plan_d_result or {}
                
                initial_dividend_count = int(plan_d_result.get("dividend_count", 0)) if plan_d_result else 0
                
                if plan_d_result:
                    plan_d = {"start": plan_d_result["start"]}
                    logger.info("[%s] 步驟 B.2.2：執行股利資料抓取 (計畫=%s)...", eid, plan_d)
                    got_d_result = fetch_dividends(eid, plan_d, region=REGION_TW)
                    per_etf[eid]["fetch"]["dividend"] = got_d_result or {}

                    if got_d_result:
                        new_records_d = got_d_result.get("dividend_new_records_count", 0)
                        current_last_dividend_ex_date = got_d_result.get("dividend_latest_date")
                        current_dividend_count = initial_dividend_count + new_records_d # 總數 = 舊 + 新
                else:
                    # 若無需規劃，表示資料已最新，數量維持不變
                    current_dividend_count = initial_dividend_count
                
                # --- B.3 更新同步狀態 ---
                logger.info("[%s] 步驟 B.3：更新 `etl_sync_status` 追蹤紀錄...", eid)
                write_etl_sync_status_to_db(
                    etf_id=eid,
                    last_price_date=current_last_price_date,
                    price_count=current_price_count,
                    last_dividend_ex_date=current_last_dividend_ex_date,
                    dividend_count=current_dividend_count,
                )
                per_etf[eid]["sync"]["status"] = "ok"
                logger.info("[%s] 處理完成。", eid)

            except Exception as e:
                error_msg = f"處理 ETF {eid} 的資料規劃與抓取時發生錯誤: {e}"
                logger.exception("【錯誤】%s", error_msg)
                per_etf[eid]["sync"]["status"] = "error"
                result["summary"]["errors"].append({"etf_id": eid, "stage": "B", "error": error_msg})

        logger.info("===== 步驟 B：規劃與抓取 ETF 詳細資料完成 =====")

        # ------------------------------------------------------------
        # C) STEP2 建 TRI（Transform: Total Return Index）
        #  流程（每檔）：
        logger.info("===== 步驟 C：建立 TRI（總報酬指數）開始 =====")

        # 抽出實際要處理的 etf_id 清單
        tri_etf_ids = [d["etf_id"] for d in etfs_data_list]

        for eid in tri_etf_ids:
            try:
                logger.info("[C][%s] 準備計算 TRI（region=TW）...", eid)

                # C1) 計算 TRI 並落庫；build_tri 僅回傳最小欄位（etf_id, last_tri_date, tri_count_new）
                info = build_tri(eid, region=REGION_TW)
                last_tri_date_new = info.get("last_tri_date")
                tri_count_new = int(info.get("tri_count_new") or 0)

                # C2) 更新 etl_sync_status（只更新 TRI 相關兩欄）
                write_etl_sync_status_to_db(
                    etf_id=eid,
                    last_tri_date=last_tri_date_new,
                    tri_count=tri_count_new,
                )

                # 記錄本檔在 result/per_etf 中的摘要（與前段格式保持一致）
                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({
                    "status": "ok",
                    "last_tri_date": last_tri_date_new,
                    "tri_count": tri_count_new,
                })

                logger.info("[C][%s] TRI 完成：last_tri_date=%s, tri_count=%s", eid, last_tri_date_new, tri_count_new)

            except Exception as e:
                logger.exception("[C][%s] 建立 TRI 發生錯誤：%s", eid, e)
                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({"status": "error"})
                result["summary"]["errors"].append({"etf_id": eid, "stage": "C", "error": str(e)})

        logger.info("===== 步驟 C：建立 TRI（總報酬指數）完成 =====")

        # 以下還沒改好
        # ------------------------------------------------------------
        # D) 回測
        # 流程（每檔）：
        #   d1) 從 TRI 讀取日期區間（read_tris_range）
        #   d2) compute_backtests(eid, ...) → 產生回測結果
        #   d3) replace_backtests_for_etf / write_etf_backtest_results_to_db → 寫回
        logger.info("[TW][D] Running backtests ...")
        for eid in etfs_data_list:
            try:
                logger.info("[TW][D] Compute backtests for %s ...", eid)
                bt = compute_backtests(eid)
                per_etf[eid]["backtests"] = bt or {"status": "ok"}

            except Exception as e:
                logger.exception("[TW][D] Error while backtesting for %s: %s", eid, e)
                per_etf[eid]["backtests"] = {"status": "error"}
                result["summary"]["errors"].append({"etf_id": eid, "stage": "D", "error": str(e)})

    except Exception as e:
        logger.exception("[TW] Top-level pipeline error: %s", e)
        result["summary"]["errors"].append({"etf_id": None, "stage": "TOP", "error": str(e)})

    finally:
        t1 = datetime.now()
        elapsed = (t1 - t0).total_seconds()
        result["summary"]["finished_at"] = t1.isoformat(timespec="seconds")
        result["summary"]["elapsed_sec"] = elapsed
        logger.info("[TW] Pipeline finished in %.2f sec | n_etf=%s | errors=%d",
                    elapsed, result["summary"]["n_etf"], len(result["summary"]["errors"]))

    return result


if __name__ == "__main__":
    # 注意：主程式僅示範執行入口；實務可由 CLI 參數傳入 start_date 等
    out = main_tw()
    # 這裡不強制 print 詳細內容，保留給上層呼叫者；需要時可自行 print(json.dumps(out, ensure_ascii=False, indent=2))
