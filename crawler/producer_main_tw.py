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

# --- 爬蟲／計算任務 ---
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
        return

    # 讀現況
    cur = read_etl_sync_status(etf_id=eid, session=session)
    if isinstance(cur, list):
        cur = cur[0] if cur else None

    if not cur:
        # 第一次：直接沿用你的插入函式
        write_etl_sync_status_to_db([row], session=session)
        return

    # 準備只更新「有提供且非 None」的欄位
    set_parts, params = [], {"eid": eid}
    for col in _ALLOWED_SYNC_COLS:
        if col in row and row[col] is not None:
            set_parts.append(f"{col} = :{col}")
            params[col] = row[col]

    if not set_parts:
        return  # 沒有要更新的欄位就跳過

    sql = text(f"""
        UPDATE etl_sync_status
        SET {", ".join(set_parts)}
        WHERE etf_id = :eid
    """)
    session.execute(sql, params)
    # 不在這裡 commit；外層 with SessionLocal.begin() 會處理

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
    try:
        # A) STEP0：ETF 名單對齊
        with SessionLocal.begin() as session:
            logger.info("===== 步驟 A：同步 ETF 名單開始 =====")

            # 1) 抓台股 ETF 名單
            crawler_url = "https://tw.stock.yahoo.com/tw-etf"
            src_rows = fetch_tw_etf_list.apply_async(
                kwargs={"crawler_url": crawler_url, "region": REGION_TW},
                queue="crawler_tw"
            ).get(timeout=60) # 等待結果，設定 60 秒超時保護
            logger.info("步驟 A.1：自 Yahoo 股市成功爬取 %d 筆原始 ETF 名單。", len(src_rows))

            # 2) 名單對齊與補值
            etfs_data_list = align_step0.apply_async(
                kwargs={"region": REGION_TW, "src_rows": src_rows, "use_yfinance": True},
                queue="crawler_tw"
            ).get(timeout=60) 

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
                    logger.info("步驟 A.5：已成功將 %d 筆新 ETF 的初始狀態寫入資料庫。", len(rows_to_insert))
                else:
                    logger.info("步驟 A.4：無新發現的 ETF，不需更新追蹤表。")
            except Exception as e:
                error_msg = f"處理 `etl_sync_status` 新增 ETF 追蹤時發生錯誤: {e}"
                logger.exception("【錯誤】%s", error_msg)
                result["summary"]["errors"].append({"etf_id": "N/A", "stage": "SYNC_INIT", "error": error_msg})

            logger.info("===== 步驟 A：同步 ETF 名單完成 =====")

        # ------------------------------------------------------------
        # B） 規劃與抓取
        with SessionLocal.begin() as session:
            logger.info("===== 步驟 B：規劃與派發並行抓取任務開始 =====")
            result["summary"]["backtests_written"] = 0

            # 使用 Dictionary 存放任務物件，實現真正並行
            price_jobs = {}
            div_jobs = {}
            plans = {}

            # --- B.1 規劃 ---
            for eid in active_ids:  
                # 保留原本 per_etf 結構
                per_etf[eid] = {
                    "plan": {"price": {}, "dividend": {}},
                    "fetch": {"price": {}, "dividend": {}},
                    "sync": {}, "tri": {}, "backtests": {},
                }
                etf_info = id2info[eid]
                inception_date = etf_info.get("inception_date") or DEFAULT_START_DATE

                logger.info("[%s] 步驟 B.1：規劃價格 (Price) 與股利 (Dividend) 資料抓取區間...", eid)
                plans[eid] = {
                    "p": plan_price_fetch.apply_async(kwargs={"etf_id": eid, "inception_date": inception_date}, queue="crawler_tw").get(),
                    "d": plan_dividend_fetch.apply_async(kwargs={"etf_id": eid, "inception_date": inception_date}, queue="crawler_tw").get()
                }
                per_etf[eid]["plan"]["price"] = plans[eid]["p"] or {}
                per_etf[eid]["plan"]["dividend"] = plans[eid]["d"] or {}

                current_last_price_date = None
                current_price_count = int(plans[eid]["p"].get("price_count", 0)) if plans[eid]["p"] else 0
                current_last_dividend_ex_date = None
                current_dividend_count = int(plans[eid]["d"].get("dividend_count", 0)) if plans[eid]["d"] else 0
            
            # --- B.2 抓取 ---
            for eid in active_ids:
                if plans[eid]["p"]:
                    plan_p = {"start": plans[eid]["p"]["start"]}
                    logger.info("[%s] 步驟 B.2.1：執行價格資料抓取 (計畫=%s)...", eid, plan_p)
                    price_jobs[eid] = fetch_daily_prices.apply_async(
                        kwargs={"etf_id": eid, "plan": plans[eid]["p"]},
                        queue="crawler_tw"
                    )
                if plans[eid]["d"]:
                    plan_d = {"start": plans[eid]["d"]["start"]}
                    logger.info("[%s] 步驟 B.2.2：執行股利資料抓取 (計畫=%s)...", eid, plan_d)
                    div_jobs[eid] = fetch_dividends.apply_async(
                        kwargs={"etf_id": eid, "plan": plans[eid]["d"], "region": REGION_TW},
                        queue="crawler_tw"
                    )

            # -----------------------------
            # --- C+D：收集結果、更新同步狀態、執行 TRI 與 回測 ---
            logger.info("===== 步驟 C+D：收集結果與執行計算 =====")

            # 用來存放計算任務的物件
            tri_jobs = {}
            bt_jobs = {}

            # 第一階段：快速獲取 Fetch 結果並「立刻派發」計算任務
            for eid in active_ids:
                try:
                    new_records_p = 0
                    current_last_price_date = None
                    
                    # 收集抓取結果 (阻塞等待個別任務完成)
                    p_res = price_jobs[eid].get(timeout=120) if eid in price_jobs else None
                    d_res = div_jobs[eid].get(timeout=120) if eid in div_jobs else None
                    
                    per_etf[eid]["fetch"]["price"] = p_res or {}
                    per_etf[eid]["fetch"]["dividend"] = d_res or {}

                    if p_res:
                        new_records_p = int(p_res.get("price_new_records_count", 0) or 0) if p_res else 0
                        current_last_price_date = p_res.get("price_latest_date")

                    # 更新 price and dividend 同步狀態
                    with SessionLocal.begin() as session:
                        _merge_update_sync_status({
                            "etf_id": eid,
                            "last_price_date": current_last_price_date,
                            "price_count": (int(plans[eid]["p"].get("price_count", 0)) + new_records_p) if plans[eid]["p"] else 0,
                            "last_dividend_ex_date": d_res.get("dividend_latest_date") if d_res else None,
                            "dividend_count": (int(plans[eid]["d"].get("dividend_count", 0)) + int(d_res.get("dividend_new_records_count", 0) if d_res else 0)) if plans[eid]["d"] else 0
                        }, session=session)

                    # 只有在有新價格時才進行 TRI 與回測
                    if new_records_p > 0:
                        tri_jobs[eid] = build_tri.apply_async(kwargs={"etf_id": eid, "region": REGION_TW}, queue="crawler_tw")
                    else:
                        logger.info(f"[{eid}] B 無新增價格，跳過 C 與 D。")

                except Exception as e:
                    logger.error(f"[{eid}] 派發 TRI 任務失敗: {e}")
            
            # 第二階段：統一收集 TRI 結果並派發 Backtest 任務
            logger.info("===== 步驟 C+D：收集 TRI 並派發回測任務 =====")
            for eid, job in tri_jobs.items():
                try:
                    tri_res = job.get(timeout=60)
                    last_tri_date_new = tri_res.get("last_tri_date")
                    tri_count_new = int(tri_res.get("tri_count_new") or 0)
                    tri_added = int(tri_res.get("tri_added", 0) or 0)

                    # 更新 TRI 同步狀態
                    with SessionLocal.begin() as session:
                        _merge_update_sync_status({"etf_id": eid, "last_tri_date": last_tri_date_new, "tri_count": tri_count_new}, session=session)
                    
                    per_etf[eid]["tri"].update(tri_res)
                    per_etf[eid]["tri"].update({"status": "ok"})
                    logger.info(f"[{eid}] TRI 計算完成，新增 {tri_added} 筆，最新日期 {last_tri_date_new}.")

                    # 如果有新 TRI，「派發」回測任務
                    if tri_added > 0:
                        bt_res = backtest_windows_from_tri.apply_async(
                            kwargs={"etf_id": eid, "end_date": last_tri_date_new, "windows_years": BACKTEST_WINDOWS_YEARS}, # 修正參數名
                            queue="crawler_tw"
                        )
                    else:
                        logger.info(f"[{eid}] TRI 無新增資料，跳過回測。")
                
                except Exception as e:
                    logger.error(f"[{eid}] TRI 計算或派發回測失敗: {e}")

            # 第三階段：最後統一收集回測結果
            logger.info("===== 步驟 C+D：收集最終回測結果 =====")
            for eid, job in bt_jobs.items():
                try:
                    bt_res = job.get(timeout=60)
                    written = int(bt_res.get("written", 0) or 0)
                    result["summary"]["backtests_written"] += written
                    per_etf[eid]["backtests"].update({
                        "status": "ok", "end_date": last_tri_date_new, "windows_done": bt_res.get("windows_done", []),
                        "windows_skipped": bt_res.get("windows_skipped", []), "written": written
                    })
                    logger.info(f"[{eid}] 回測並行完成，寫入 {written} 筆。")

                except Exception as e:
                    logger.error(f"[{eid}] 收集回測結果失敗: {e}")

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
                    count += 1
                logger.info("已更新所有 %d 檔 ETF 的 `updated_at=%s`。", count, now_dt.isoformat(timespec="seconds"))
            
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
