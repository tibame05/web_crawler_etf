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
from crawler.config import DEFAULT_START_DATE, REGION_TW, BACKTEST_WINDOWS_YEARS
from crawler.worker import app  # 僅初始化；實際呼叫在程式內

# --- 爬蟲／計算任務（僅引用，不在此實作） ---
from crawler.tasks_etf_list_tw import fetch_tw_etf_list
from crawler.tasks_align import align_step0
from crawler.tasks_plan import plan_price_fetch, plan_dividend_fetch
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends
from crawler.tasks_tri import build_tri
from crawler.tasks_backtests import backtest_windows_from_tri

# --- DB 介面（database/main.py 提供；此處只呼叫，不實作細節） ---
from database import SessionLocal
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

DATE_FMT = "%Y-%m-%d"

def main_tw() -> Dict[str, Any]:
    """
    台股整體管線（TW only）
    流程：
    A) STEP0：ETF 名單對齊
        - 從 Yahoo 股市抓取最新 ETF 名單，與資料庫比對。
        - 將新發現的 ETF 加入 `etl_sync_status` 追蹤表。
    B) STEP1：規劃與抓取（價格、股利）
        - 逐檔 ETF 檢查需補齊的資料區間（價格與股利）。
        - 執行爬蟲抓取歷史資料。
        - 將抓取結果寫入資料庫並更新 `etl_sync_status` 的進度。
    C) STEP2：建立總報酬指數（TRI）
        - 根據已有的價格與股利，計算每檔 ETF 的 TRI。
        - 將計算結果存入資料庫，並更新 `etl_sync_status` 中的 TRI 進度。
    D) STEP3：執行回測
        - 根據最新的 TRI 資料，計算不同時間窗口（如 1/3/10 年）的績效指標。
        - 將回測結果寫入資料庫。
    E) STEP4：收尾
        - 更新今日所有已處理的 ETF 在 `etl_sync_status` 中的 `updated_at` 時間戳。
    回傳：
    {
    "summary": {...},
    "per_etf": {
        "0050.TW": {"plan":{...}, "fetch":{...}, "sync":{...}, "tri":{...}, "backtests":{...}},
        ...
    }
    }

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
    per_etf = result["per_etf"]
    today_str = datetime.today().strftime(DATE_FMT)

    # ------------------------------------------------------------
    # A) STEP0：ETF 名單對齊  (Align ETF List)
    #  目標：確保我們的 ETF 清單與市場上最新的保持一致，並初始化新加入的 ETF。
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 A：同步 ETF 名單開始 =====")

        # 1. 從 Yahoo 股市抓取完整的 ETF 名單
        crawler_url = "https://tw.stock.yahoo.com/tw-etf"
        src_rows = fetch_tw_etf_list(crawler_url, REGION_TW)
        logger.info("步驟 A.1：自 Yahoo 股市成功爬取 %d 筆原始 ETF 名單。", len(src_rows))

        # 2. 進行名單對齊、資料清洗與補充 (例如：透過 yfinance 補齊發行日等資訊)
        etfs_data_list = align_step0(region=REGION_TW, src_rows=src_rows, use_yfinance=True, session=session)

        # 3. 取得最終處理的活躍 ETF 清單
        final_active_ids: Set[str] = {data['etf_id'] for data in etfs_data_list}
        result["summary"]["n_etf"] = len(final_active_ids)
        logger.info("步驟 A.2：經過濾與對齊後，最終需處理的活躍 ETF 名單共 %d 筆。", len(final_active_ids))
        
        # 4. 處理同步狀態表 (etl_sync_status)，將新發現的 ETF 加入追蹤
        try:
            # 讀取資料庫中已在追蹤的 ETF
            existing_sync_rows = read_etl_sync_status(region=REGION_TW, session=session) or []
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
                write_etl_sync_status_to_db(rows_to_insert, session=session)
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
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 B：規劃與抓取 ETF 詳細資料開始  =====")

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
            

            try:
                # --- B.1 價格股利區間 ---
                logger.info("[%s] 步驟 B.1.1：規劃價格 (Price) 資料抓取區間...", eid)
                plan_p_result = plan_price_fetch(eid, inception_date=inception_date, session=session)
                per_etf[eid]["plan"]["price"] = plan_p_result or {}

                logger.info("[%s] 步驟 B.1.2：規劃股利 (Dividend) 資料抓取區間...", eid)
                plan_d_result = plan_dividend_fetch(eid, inception_date=inception_date, session=session)
                per_etf[eid]["plan"]["dividend"] = plan_d_result or {}
                
                current_last_price_date = None
                current_price_count = int(plan_p_result.get("price_count", 0)) if plan_p_result else 0
                current_last_dividend_ex_date = None
                current_dividend_count = int(plan_d_result.get("dividend_count", 0)) if plan_d_result else 0

                # --- B.2 價格股利抓取資料 ---
                if plan_p_result:
                    plan_p = {"start": plan_p_result["start"]}
                    logger.info("[%s] 步驟 B.2.1：執行價格資料抓取 (計畫=%s)...", eid, plan_p)
                    got_p_result = fetch_daily_prices(eid, plan_p, session=session)
                    per_etf[eid]["fetch"]["price"] = got_p_result or {}
                    
                    if got_p_result:
                        new_records_p = got_p_result.get("price_new_records_count", 0)
                        current_last_price_date = got_p_result.get("price_latest_date")
                        current_price_count = current_price_count + new_records_p # 總數 = 舊 + 新
                else:
                    # 若無需規劃，表示資料已最新，數量維持不變
                    current_price_count = current_price_count

                if plan_d_result:
                    plan_d = {"start": plan_d_result["start"]}
                    logger.info("[%s] 步驟 B.2.2：執行股利資料抓取 (計畫=%s)...", eid, plan_d)
                    got_d_result = fetch_dividends(eid, plan_d, region=REGION_TW, session=session)
                    per_etf[eid]["fetch"]["dividend"] = got_d_result or {}

                    if got_d_result:
                        new_records_d = got_d_result.get("dividend_new_records_count", 0)
                        current_last_dividend_ex_date = got_d_result.get("dividend_latest_date")
                        current_dividend_count = current_dividend_count + new_records_d # 總數 = 舊 + 新
                else:
                    # 若無需規劃，表示資料已最新，數量維持不變
                    current_dividend_count = current_dividend_count
                
                # --- B.3 更新同步狀態 ---
                logger.info("[%s] 步驟 B.3：更新 `etl_sync_status` 追蹤紀錄...", eid)
                write_etl_sync_status_to_db(
                    [{"etf_id": eid,
                      "last_price_date": current_last_price_date,
                      "price_count": current_price_count,
                      "last_dividend_ex_date": current_last_dividend_ex_date,
                      "dividend_count": current_dividend_count}],
                    session=session
                )
                per_etf[eid]["sync"]["status"] = "ok"
                logger.info("[%s] 價格/股利抓取與同步完成。", eid)

            except Exception as e:
                error_msg = f"處理 ETF {eid} 的資料規劃與抓取時發生錯誤: {e}"
                logger.exception("【錯誤】%s", error_msg)
                per_etf[eid]["sync"]["status"] = "error"
                result["summary"]["errors"].append({"etf_id": eid, "stage": "B", "error": error_msg})

        logger.info("===== 步驟 B：規劃與抓取 ETF 詳細資料完成 =====")

    # ------------------------------------------------------------
    # C+D) STEP2 建 TRI 並條件性回測（Backtests）
    #   - 先對每檔 ETF 建/增量 TRI，落庫與同步
    #   - 若本日有新 TRI（last_tri_date == today），再執行回測，並寫入 DB
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 C+D：建立 TRI 並（必要時）回測 開始 =====")

        tri_etf_ids = [d["etf_id"] for d in etfs_data_list]
        result["summary"]["backtests_written"] = 0

        for eid in tri_etf_ids:
            try:
                logger.info("[C+D][%s] 準備計算 TRI（region=TW）...", eid)

                # 1) 計算 TRI 並落庫；build_tri 僅回傳最小欄位（etf_id, last_tri_date, tri_count_new）
                info = build_tri(eid, region=REGION_TW, session=session)
                last_tri_date_new = info.get("last_tri_date")
                tri_count_new = int(info.get("tri_count_new") or 0)

                # 2) 更新 etl_sync_status（只更新 TRI 相關兩欄；可用 upsert 單筆或 write 批次）
                # 若 DB 不支援 kwargs 單筆，請改為 write_etl_sync_status_to_db([ {...} ])
                write_etl_sync_status_to_db(
                    [{"etf_id":eid,
                      "last_tri_date":last_tri_date_new,
                      "tri_count":tri_count_new}],
                    session=session
                )

                # 紀錄在 per_etf
                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({
                    "status": "ok",
                    "last_tri_date": last_tri_date_new,
                    "tri_count": tri_count_new,
                })
                logger.info("[C+D][%s] TRI 完成：last_tri_date=%s, tri_count=%s", eid, last_tri_date_new, tri_count_new)

                # 3) 決定是否回測：
                #    規則：只有「有新 TRI」（last_tri_date == today）才回測；否則跳過（等到 E）
                do_backtest = (last_tri_date_new == today_str)
                if not do_backtest:
                    per_etf.setdefault(eid, {}).setdefault("backtests", {})
                    per_etf[eid]["backtests"].update({
                        "status": "skipped_no_today_update",
                        "end_date": last_tri_date_new,
                    })
                    logger.info("[C+D][%s] 今日無新 TRI（last_tri_date=%s），跳過回測。", eid, last_tri_date_new)

                else:
                    logger.info("[C+D][%s] 以 end_date=%s 執行回測（1y/3y/10y 嚴格年窗）...", eid, last_tri_date_new)
                    bt_res = backtest_windows_from_tri(
                        etf_id=eid,
                        end_date=last_tri_date_new,
                        windows_years=BACKTEST_WINDOWS_YEARS,
                        session=session
                    )
                    written = int(bt_res.get("written", 0))
                    result["summary"]["backtests_written"] += written

                    per_etf.setdefault(eid, {}).setdefault("backtests", {})
                    per_etf[eid]["backtests"].update({
                        "status": "ok",
                        "end_date": last_tri_date_new,
                        "windows_done": bt_res.get("windows_done", []),
                        "windows_skipped": bt_res.get("windows_skipped", []),
                        "written": written,
                    })
                    logger.info("[C+D][%s] 回測完成：寫入 %d 筆；完成=%s；跳過=%s",
                                eid, written,
                                bt_res.get("windows_done", []),
                                bt_res.get("windows_skipped", []))

            except Exception as e:
                logger.exception("[C+D][%s] TRI/回測流程發生錯誤：%s", eid, e)
                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({"status": "error"})
                per_etf.setdefault(eid, {}).setdefault("backtests", {})
                per_etf[eid]["backtests"].update({"status": "error"})
                result["summary"]["errors"].append({"etf_id": eid, "stage": "C+D", "error": str(e)})

        logger.info("===== 步驟 C+D：建立 TRI 並（必要時）回測 完成；本批回測寫入 %d 筆 =====",
                    result["summary"]["backtests_written"])

    # ------------------------------------------------------------
    # E) STEP4 收尾：更新同步時間與當日狀態
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 E：更新同步時間與日誌 =====")

        # 判斷今天是否有更新（任一檔 ETF 的 last_tri_date == today）
        updated_today_ids = []
        for eid in per_etf.keys():
            last_tri_date = per_etf.get(eid, {}).get("tri", {}).get("last_tri_date")
            if last_tri_date == today_str:
                updated_today_ids.append(eid)

        if updated_today_ids:
            logger.info("【總結】今日有更新 TRI 資料的 ETF：共 %d 檔 → %s",
                        len(updated_today_ids), updated_today_ids)
        else:
            logger.info("【總結】今日無新的 TRI 更新資料。")

        # 更新所有活躍 ETF 的 updated_at 欄位為今日日期
        try:
            rows_to_update = [{"etf_id": eid, "updated_at": today_str} for eid in per_etf.keys()]
            write_etl_sync_status_to_db(rows_to_update, session=session)
            logger.info("已更新所有 %d 檔 ETF 的 `updated_at=%s`。", len(rows_to_update), today_str)
        except Exception as e:
            logger.exception("更新 `etl_sync_status.updated_at` 時發生錯誤：%s", e)
            result["summary"]["errors"].append({"etf_id": "ALL", "stage": "E", "error": str(e)})

        logger.info("===== 步驟 E：同步收尾完成 =====")

    return result


if __name__ == "__main__":
    # 注意：主程式僅示範執行入口；實務可由 CLI 參數傳入 start_date 等
    out = main_tw()
    # 這裡不強制 print 詳細內容，保留給上層呼叫者；需要時可自行 print(json.dumps(out, ensure_ascii=False, indent=2))
