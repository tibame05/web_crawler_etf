# crawler/producer_main_tw.py
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
from typing import Dict, Any, Set
from datetime import datetime
from sqlalchemy import text

from crawler import logger
from crawler.config import DEFAULT_START_DATE, REGION_TW, BACKTEST_WINDOWS_YEARS

# --- 爬蟲／計算任務（僅引用，不在此實作） ---
from crawler.tasks_etf_list_tw import fetch_tw_etf_list_task
from crawler.tasks_align import align_step0_task
from crawler.tasks_plan import plan_price_fetch_task, plan_dividend_fetch_task
from crawler.tasks_fetch import fetch_daily_prices_task, fetch_dividends_task
from crawler.tasks_tri import build_tri_task
from crawler.tasks_backtests import backtest_windows_from_tri_task

# --- DB 介面（database/main.py 提供；此處只呼叫，不實作細節） ---
from database import SessionLocal
from database.main import (
    write_etl_sync_status_to_db,
    read_etl_sync_status,
)

DATE_FMT = "%Y-%m-%d"

_ALLOWED_SYNC_COLS = [
    "region",
    "last_price_date", "price_count",
    "last_dividend_ex_date", "dividend_count",
    "last_tri_date", "tri_count",
    "updated_at",
]

def _merge_update_sync_status(row: Dict[str, Any], session) -> None:
    """不改 database 模組：在 main 裡完成 insert-or-update。
    - 若不存在 → 呼叫 write_etl_sync_status_to_db 插入
    - 若已存在 → 只 UPDATE 本次有提供且非 None 的欄位（避免把舊值蓋成 NULL）
    """
    eid = row.get("etf_id")
    if not eid:
        logger.warning("_merge_update_sync_status: etf_id 為空,跳過")
        return

    # 讀現況
    cur = read_etl_sync_status(etf_id=eid, session=session)
    if isinstance(cur, list):
        cur = cur[0] if cur else None

    if not cur:
        # 第一次:直接插入
        logger.debug("_merge_update_sync_status: %s 不存在,執行插入", eid)
        write_etl_sync_status_to_db([row], session=session)
        return

    # 準備只更新「有提供且非 None」的欄位
    set_parts, params = [], {"eid": eid}
    for col in _ALLOWED_SYNC_COLS:
        if col in row and row[col] is not None:
            set_parts.append(f"{col} = :{col}")
            params[col] = row[col]

    if not set_parts:
        logger.debug("_merge_update_sync_status: %s 無欄位需更新", eid)
        return

    sql = text(f"""
        UPDATE etl_sync_status
        SET {", ".join(set_parts)}
        WHERE etf_id = :eid
    """)
    result = session.execute(sql, params)
    
    # ✅ 檢查是否真的有更新到
    if result.rowcount == 0:
        logger.warning("_merge_update_sync_status: %s UPDATE 影響 0 筆,可能記錄不存在", eid)
    else:
        logger.debug("_merge_update_sync_status: %s UPDATE 影響 %d 筆", eid, result.rowcount)

def main_tw() -> Dict[str, Any]:
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
    # A) STEP0：ETF 名單對齊
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 A：同步 ETF 名單開始 =====")

        # 1) 抓台股 ETF 名單
        crawler_url = "https://tw.stock.yahoo.com/tw-etf"
        src_rows = fetch_tw_etf_list_task.apply_async(
            kwargs={"crawler_url": crawler_url, "region": REGION_TW},
            queue="tw_crawler",
        ).get()
        logger.info("步驟 A.1：自 Yahoo 股市成功爬取 %d 筆原始 ETF 名單。", len(src_rows))

        # 2) 名單對齊與補值
        etfs_data_list = align_step0_task.apply_async(
            kwargs={"region": REGION_TW, "src_rows": src_rows, "use_yfinance": True},
            queue="tw_align",
        ).get()

        # 3) 整備活躍清單（不再過濾，全部處理）
        id2info = {d['etf_id']: d for d in etfs_data_list}
        final_all_ids: Set[str] = set(id2info.keys())

        active_ids = sorted(final_all_ids)

        result["summary"]["n_etf"] = len(active_ids)
        logger.info("步驟 A.2：經過濾與對齊後，最終需處理的活躍 ETF 名單共 %d 筆。", len(active_ids))


        # 4) etl_sync_status 逐檔補建
        try:
            existing_sync_ids: Set[str] = set()
            missing_sync_ids: list[str] = []

            for eid in active_ids:
                row = read_etl_sync_status(etf_id=eid, session=session)
                if isinstance(row, list):
                    row = row[0] if row else None

                if row and row.get("etf_id"):
                    existing_sync_ids.add(row["etf_id"])
                else:
                    missing_sync_ids.append(eid)

            logger.info("步驟 A.3：資料庫 `etl_sync_status` 中已存在 %d 筆 ETF 追蹤紀錄。", len(existing_sync_ids))

            if missing_sync_ids:
                logger.info("步驟 A.4：發現 %d 筆新 ETF 需加入追蹤：%s",
                            len(missing_sync_ids), missing_sync_ids)
                rows_to_insert = [
                    {
                        "etf_id": eid,
                        "region": REGION_TW,  # 若表無 region 欄位可移除
                        "last_price_date": None,
                        "price_count": 0,
                        "last_dividend_ex_date": None,
                        "dividend_count": 0,
                        "last_tri_date": None,
                        "tri_count": 0,
                        "updated_at": None,
                    }
                    for eid in missing_sync_ids
                ]
                write_etl_sync_status_to_db(rows_to_insert, session=session)
                session.flush()
                logger.info("步驟 A.5：已成功將 %d 筆新 ETF 的初始狀態寫入資料庫。", len(rows_to_insert))
            else:
                logger.info("步驟 A.4：無新發現的 ETF，不需更新追蹤表。")
        except Exception as e:
            error_msg = f"處理 `etl_sync_status` 新增 ETF 追蹤時發生錯誤: {e}"
            logger.exception("【錯誤】%s", error_msg)
            result["summary"]["errors"].append({"etf_id": "N/A", "stage": "SYNC_INIT", "error": error_msg})

        logger.info("===== 步驟 A：同步 ETF 名單完成 =====")

    # ------------------------------------------------------------
    # B~D) 規劃與抓取 → 建 TRI →（必要時）回測（單一迴圈逐檔執行）
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 B~D：規劃/抓取 → 建立 TRI →（必要時）回測 開始 =====")
        result["summary"]["backtests_written"] = 0

        for eid in active_ids:  
            etf_info = id2info[eid]
            inception_date = etf_info.get("inception_date")
            logger.info("--- 開始處理 ETF: %s ---", eid)

            # 保留原本 per_etf 結構
            per_etf[eid] = {
                "plan": {"price": {}, "dividend": {}},
                "fetch": {"price": {}, "dividend": {}},
                "sync": {}, "tri": {}, "backtests": {},
            }

            try:
                # -----------------------------
                # --- B.1 規劃（保留原 log）---
                logger.info("[%s] 步驟 B.1.1：規劃價格 (Price) 資料抓取區間...", eid)
                logger.info("[DBG] (%s) B.1.1 即將送出 plan_price 任務", eid)
                plan_p_result = plan_price_fetch_task.apply_async(
                    kwargs={"etf_id": eid, "inception_date": inception_date},
                    queue="tw_plan",
                ).get()
                logger.info("[DBG] (%s) B.1.1 收到 plan_price 結果", eid)
                per_etf[eid]["plan"]["price"] = plan_p_result or {}

                logger.info("[%s] 步驟 B.1.2：規劃股利 (Dividend) 資料抓取區間...", eid)
                plan_d_result = plan_dividend_fetch_task.apply_async(
                    kwargs={"etf_id": eid, "inception_date": inception_date},
                    queue="tw_plan",
                ).get()
                per_etf[eid]["plan"]["dividend"] = plan_d_result or {}

                current_last_price_date = None
                current_price_count = int(plan_p_result.get("price_count", 0)) if plan_p_result else 0
                current_last_dividend_ex_date = None
                current_dividend_count = int(plan_d_result.get("dividend_count", 0)) if plan_d_result else 0

                # -----------------------------
                # --- B.2 抓取（保留原 log）---
                got_p_result = {}
                got_d_result = {}

                if plan_p_result:
                    plan_p = {"start": plan_p_result["start"]}
                    logger.info("[%s] 步驟 B.2.1：執行價格資料抓取 (計畫=%s)...", eid, plan_p)
                    got_p_result = fetch_daily_prices_task.apply_async(
                        kwargs={"etf_id": eid, "plan": plan_p},
                        queue="tw_fetch",
                    ).get() or {}
                    per_etf[eid]["fetch"]["price"] = got_p_result
                    if got_p_result:
                        new_records_p = int(got_p_result.get("price_new_records_count", 0) or 0)
                        current_last_price_date = got_p_result.get("price_latest_date")
                        current_price_count += new_records_p
                    else:
                        new_records_p = 0
                else:
                    new_records_p = 0

                if plan_d_result:
                    plan_d = {"start": plan_d_result["start"]}
                    logger.info("[%s] 步驟 B.2.2：執行股利資料抓取 (計畫=%s)...", eid, plan_d)
                    got_d_result = fetch_dividends_task.apply_async(
                        kwargs={"etf_id": eid, "plan": plan_d, "region": REGION_TW},
                        queue="tw_fetch",
                    ).get() or {}
                    per_etf[eid]["fetch"]["dividend"] = got_d_result
                    if got_d_result:
                        new_records_d = int(got_d_result.get("dividend_new_records_count", 0) or 0)
                        current_last_dividend_ex_date = got_d_result.get("dividend_latest_date")
                        current_dividend_count += new_records_d
                    else:
                        new_records_d = 0
                else:
                    new_records_d = 0

                # -----------------------------
                # --- B.3 更新同步狀態（保留原 log）---
                logger.info("[%s] 步驟 B.3：更新 `etl_sync_status` 追蹤紀錄...", eid)
                _merge_update_sync_status(
                    {
                        "etf_id": eid,
                        "last_price_date": current_last_price_date,
                        "price_count": current_price_count,
                        "last_dividend_ex_date": current_last_dividend_ex_date,
                        "dividend_count": current_dividend_count
                    }, session=session
                )
                session.flush()
                per_etf[eid]["sync"]["status"] = "ok"
                logger.info("[%s] 價格/股利抓取與同步完成。", eid)

                # -----------------------------
                # --- 介面：若 B 無新增價格 → 直接跳過 C、D（新增說明 log）---
                if new_records_p <= 0:
                    logger.info("[%s] B 無新增價格（%d），跳過 C 與 D。", eid, new_records_p)
                    continue
                
                # 關鍵：把同一 transaction 的 INSERT 送到 DB、讓同 session 的 SELECT 看得到
                session.flush()

                # -----------------------------
                # --- C：建 TRI（沿用原 C+D 首行 log）---
                logger.info("[C+D][%s] 準備計算 TRI（region=TW）...", eid)
                info = build_tri_task.apply_async(
                    kwargs={"etf_id": eid, "region": REGION_TW},
                    queue="tw_tri",
                ).get() or {}
                last_tri_date_new = info.get("last_tri_date")
                tri_count_new = int(info.get("tri_count_new") or 0)
                tri_added = int(info.get("tri_added", 0) or 0)  # 建議在 build_tri 回傳此欄位

                # 同步 TRI 欄位（保留你的寫法與 log）
                _merge_update_sync_status(
                    {"etf_id": eid, "last_tri_date": last_tri_date_new, "tri_count": tri_count_new},
                    session=session
                )
                session.flush()

                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({
                    "status": "ok",
                    "last_tri_date": last_tri_date_new,
                    "tri_count": tri_count_new,
                    "tri_added": tri_added,
                })
                logger.info("[C+D][%s] TRI 完成：last_tri_date=%s, tri_count=%s", eid, last_tri_date_new, tri_count_new)

                # 若 C 無新增 TRI → 跳過 D（新增說明 log）
                if tri_added <= 0:
                    per_etf.setdefault(eid, {}).setdefault("backtests", {})
                    per_etf[eid]["backtests"].update({
                        "status": "skipped_no_new_tri",
                        "end_date": last_tri_date_new,
                    })
                    logger.info("[C+D][%s] 本次無新增 TRI（tri_added=%d），跳過回測。", eid, tri_added)
                    continue

                # -----------------------------
                # --- D：回測（沿用原有回測 log 格式，但不再限制「必須是今日」）---
                logger.info("[C+D][%s] 以 end_date=%s 執行回測（1y/3y/10y 嚴格年窗）...", eid, last_tri_date_new)
                bt_res = backtest_windows_from_tri_task.apply_async(
                    kwargs={
                        "etf_id": eid,
                        "end_date": last_tri_date_new,
                        "windows_years": BACKTEST_WINDOWS_YEARS,
                    },
                    queue="tw_backtest",
                ).get() or {}
                written = int(bt_res.get("written", 0) or 0)
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
                # 保留你原本的錯誤紀錄格式
                logger.exception("[C+D][%s] TRI/回測流程發生錯誤：%s", eid, e)
                session.rollback() 
                per_etf.setdefault(eid, {}).setdefault("tri", {})
                per_etf[eid]["tri"].update({"status": "error"})
                per_etf.setdefault(eid, {}).setdefault("backtests", {})
                per_etf[eid]["backtests"].update({"status": "error"})
                result["summary"]["errors"].append({"etf_id": eid, "stage": "C+D", "error": str(e)})

        logger.info("===== 步驟 B~D：規劃/抓取 → 建立 TRI →（必要時）回測 完成；本批回測寫入 %d 筆 =====",
                    result["summary"]["backtests_written"])

    # ------------------------------------------------------------
    # E) 收尾
    with SessionLocal.begin() as session:
        logger.info("===== 步驟 E：更新同步時間與日誌 =====")

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

        try:
            count = 0
            now_dt = datetime.now() 
            for eid in per_etf.keys():
                _merge_update_sync_status({"etf_id": eid, "updated_at": now_dt}, session=session)
                session.flush()
                count += 1
            logger.info("已更新所有 %d 檔 ETF 的 `updated_at=%s`。", count, now_dt.isoformat(timespec="seconds"))
        
        except Exception as e:
            logger.exception("更新 `etl_sync_status.updated_at` 時發生錯誤：%s", e)
            result["summary"]["errors"].append({"etf_id": "ALL", "stage": "E", "error": str(e)})

        logger.info("===== 步驟 E：同步收尾完成 =====")

    return result


if __name__ == "__main__":
    # 注意：主程式僅示範執行入口；實務可由 CLI 參數傳入 start_date 等
    main_tw()
    # 這裡不強制 print 詳細內容，保留給上層呼叫者；需要時可自行 print(json.dumps(out, ensure_ascii=False, indent=2))
