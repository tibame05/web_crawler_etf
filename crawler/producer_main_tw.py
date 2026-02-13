# crawler/producer_main_tw.py
"""
台股整體管線（TW only）
核心流程：
1. 名單對齊：同步 Yahoo TW 與 DB 資訊。
2. 任務派發：規劃並抓取價格與股利。
3. 數據整合：建置 TRI (總報酬指數) 並執行回測。
4. 狀態更新：紀錄 ETL 同步進度於資料庫。
"""

from __future__ import annotations
from typing import Dict, Any
from datetime import datetime

from crawler import logger
from crawler.config import DEFAULT_START_DATE, REGION_TW, BACKTEST_WINDOWS_YEARS
from database import SessionLocal

# --- 原始爬蟲／計算任務 ---
from crawler.rpc_tasks import (
    rpc_fetch_tw_etf_list,
    rpc_align_step0_tw,
    rpc_plan_price_fetch_tw,
    rpc_plan_dividend_fetch_tw,
    rpc_async_fetch_daily_prices_tw,
    rpc_async_fetch_dividends_tw,
    rpc_async_build_tri_tw,
    rpc_async_backtest_windows_tw
)
from crawler.sync_service import (
    merge_update_sync_status, 
    init_missing_sync_status, 
    update_all_timestamp
)


DATE_FMT = "%Y-%m-%d"

# =================================================================
# 主流程 (內容邏輯完全不變，僅抽換呼叫方式)
# =================================================================

def main_tw() -> Dict[str, Any]:
    t0 = datetime.now()
    logger.info("【台股 ETF 資訊同步】主流程啟動...")
    result: Dict[str, Any] = {
        "summary": {
            "region": REGION_TW, "start_date": DEFAULT_START_DATE,
            "started_at": t0.isoformat(timespec="seconds"),
            "n_etf": 0, "errors": [],
        },
        "per_etf": {}
    }
    per_etf = result["per_etf"]
    
    try:
        # A) STEP0：ETF 名單對齊
        with SessionLocal.begin() as session:
            logger.info("===== 步驟 A：同步 ETF 名單開始 =====")
            
            # 使用 RPC 封裝
            src_rows = rpc_fetch_tw_etf_list(crawler_url="https://tw.stock.yahoo.com/tw-etf")
            logger.info("步驟 A.1：自 Yahoo 股市成功爬取 %d 筆原始 ETF 名單。", len(src_rows))

            etfs_data_list = rpc_align_step0_tw(src_rows)

            id2info = {d['etf_id']: d for d in etfs_data_list}
            active_ids = sorted(set(id2info.keys()))
            result["summary"]["n_etf"] = len(active_ids)
            logger.info("步驟 A.2：經過濾與對齊後，最終需處理的活躍 ETF 名單共 %d 筆。", len(active_ids))

            # 4) etl_sync_status 逐檔補建
            try:
                # 呼叫封裝後的函式
                n_new = init_missing_sync_status(active_ids, REGION_TW, session)
                
                if n_new > 0:
                    logger.info(f"步驟 A.5：已成功將 {n_new} 筆新 ETF 的初始狀態寫入資料庫。")
                else:
                    logger.info("步驟 A.4：無新發現的 ETF，不需更新追蹤表。")

            except Exception as e:
                # 保留原本的錯誤紀錄邏輯
                error_msg = f"處理 `etl_sync_status` 新增 ETF 追蹤時發生錯誤: {e}"
                logger.exception("【錯誤】%s", error_msg)
                result["summary"]["errors"].append({
                    "etf_id": "N/A", 
                    "stage": "SYNC_INIT", 
                    "error": error_msg
                })

        # B） 規劃與抓取
        with SessionLocal.begin() as session:
            logger.info("===== 步驟 B：規劃與派發並行抓取任務開始 =====")
            result["summary"]["backtests_written"] = 0
            price_jobs, div_jobs, plans = {}, {}, {}

            for eid in active_ids:  
                per_etf[eid] = {
                    "plan": {"price": {}, "dividend": {}},
                    "fetch": {"price": {}, "dividend": {}},
                    "sync": {}, "tri": {}, "backtests": {},
                }
                etf_info = id2info[eid]
                inception_date = etf_info.get("inception_date") or DEFAULT_START_DATE

                logger.info("[%s] 步驟 B.1：規劃價格 (Price) 與股利 (Dividend) 資料抓取區間...", eid)
                plans[eid] = {
                    "p": rpc_plan_price_fetch_tw(eid, inception_date),
                    "d": rpc_plan_dividend_fetch_tw(eid, inception_date)
                }
                per_etf[eid]["plan"]["price"] = plans[eid]["p"] or {}
                per_etf[eid]["plan"]["dividend"] = plans[eid]["d"] or {}
            
            for eid in active_ids:
                if plans[eid]["p"]:
                    plan_p = {"start": plans[eid]["p"]["start"]}
                    logger.info("[%s] 步驟 B.2.1：執行價格資料抓取 (計畫=%s)...", eid, plan_p)
                    price_jobs[eid] = rpc_async_fetch_daily_prices_tw(eid, plans[eid]["p"])
                
                if plans[eid]["d"]:
                    plan_d = {"start": plans[eid]["d"]["start"]}
                    logger.info("[%s] 步驟 B.2.2：執行股利資料抓取 (計畫=%s)...", eid, plan_d)
                    div_jobs[eid] = rpc_async_fetch_dividends_tw(eid, plans[eid]["d"])

            # C+D：處理
            logger.info("===== 步驟 C+D：1.收集價格股利結果與執行計算 =====")
            tri_jobs, bt_jobs = {}, {}
            for eid in active_ids:
                try:
                    p_res = price_jobs[eid].get(timeout=120) if eid in price_jobs else None
                    d_res = div_jobs[eid].get(timeout=120) if eid in div_jobs else None
                    
                    per_etf[eid]["fetch"]["price"] = p_res or {}
                    per_etf[eid]["fetch"]["dividend"] = d_res or {}

                    new_records_p = int(p_res.get("price_new_records_count", 0) or 0) if p_res else 0
                    
                    with SessionLocal.begin() as session:
                        merge_update_sync_status({
                            "etf_id": eid,
                            "last_price_date": p_res.get("price_latest_date") if p_res else None,
                            "price_count": (int(plans[eid]["p"].get("price_count", 0)) + new_records_p) if plans[eid]["p"] else 0,
                            "last_dividend_ex_date": d_res.get("dividend_latest_date") if d_res else None,
                            "dividend_count": (int(plans[eid]["d"].get("dividend_count", 0)) + int(d_res.get("dividend_new_records_count", 0) if d_res else 0)) if plans[eid]["d"] else 0
                        }, session=session)

                    if new_records_p > 0:
                        tri_jobs[eid] = rpc_async_build_tri_tw(eid)
                    else:
                        logger.info(f"[{eid}] B 無新增價格，跳過 C 與 D。")

                except Exception as e:
                    logger.error(f"[{eid}] 任務處理失敗: {e}")
            
            # 收集 TRI 並派發 Backtest
            logger.info("===== 步驟 C+D：2.收集「TRI」並派發回測任務 =====")
            for eid, job in tri_jobs.items():
                try:
                    tri_res = job.get(timeout=60)
                    tri_added = int(tri_res.get("tri_added", 0) or 0)
                    with SessionLocal.begin() as session:
                        merge_update_sync_status({
                            "etf_id": eid, 
                            "last_tri_date": tri_res.get("last_tri_date"), 
                            "tri_count": int(tri_res.get("tri_count_new") or 0)
                        }, session=session)
                    
                    per_etf[eid]["tri"].update(tri_res)
                    logger.info(f"[{eid}] TRI 計算完成，新增 {tri_added} 筆，最新日期 {tri_res.get('last_tri_date')}.")
                    
                    if tri_added > 0:
                        bt_jobs[eid] = rpc_async_backtest_windows_tw(eid, tri_res.get("last_tri_date"), BACKTEST_WINDOWS_YEARS)
                    else:
                        logger.info(f"[{eid}] TRI 無新增資料，跳過回測。")

                except Exception as e:
                    logger.error(f"[{eid}] TRI 計算或派發回測失敗: {e}")

            # 收集最後結果
            logger.info("===== 步驟 C+D：3.收集「回測」結果 =====")
            for eid, job in bt_jobs.items():
                try:
                    bt_res = job.get(timeout=60)
                    written = int(bt_res.get("written", 0) or 0)
                    result["summary"]["backtests_written"] += written
                    per_etf[eid]["backtests"].update({"status": "ok", "written": written})
                    logger.info(f"[{eid}] 回測並行完成，寫入 {written} 筆。")
                except Exception as e:
                    logger.error(f"[{eid}] 收集回測結果失敗: {e}")

        # E) 收尾
        with SessionLocal.begin() as session:
            logger.info("===== 步驟 E：更新同步時間與日誌 =====")

            # 修改後的判斷邏輯：檢查本次執行的「新增筆數」
            updated_this_run_ids = []
            for eid in per_etf.keys():
                # 取得任務回傳的新增筆數 (tri_added)
                tri_added = per_etf.get(eid, {}).get("tri", {}).get("tri_added", 0)
                if tri_added > 0:
                    updated_this_run_ids.append(eid)

            if updated_this_run_ids:
                logger.info("【總結】本次執行有更新 TRI 資料的 ETF：共 %d 檔 → %s",
                            len(updated_this_run_ids), updated_this_run_ids)
            else:
                logger.info("【總結】本次執行中，所有 ETF 均無新的 TRI 資料需要更新。")

            try:
                n_updated = update_all_timestamp(list(per_etf.keys()), session)
                logger.info("已更新所有 %d 檔 ETF 的 updated_at。", n_updated)            
            except Exception as e:
                logger.exception("更新 `etl_sync_status.updated_at` 時發生錯誤：%s", e)
                result["summary"]["errors"].append({"etf_id": "ALL", "stage": "E", "error": str(e)})

            logger.info("===== 步驟 E：同步收尾完成 =====")
            return result

    except Exception as e:
        logger.exception("【重大錯誤】台股 ETF 資訊同步主流程發生未預期錯誤：%s", e)
        result["summary"]["errors"].append({"etf_id": "N/A", "stage": "MAIN", "error": str(e)})
        return result

if __name__ == "__main__":
    main_tw()