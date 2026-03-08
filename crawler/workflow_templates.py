# crawler/producer_main_tw.py
from __future__ import annotations
from typing import Dict, Any, List
from datetime import datetime
from sqlalchemy import text
from celery import shared_task

from crawler import logger
from crawler.config import DEFAULT_START_DATE, BACKTEST_WINDOWS_YEARS
from crawler.tasks_plan import plan_price_fetch, plan_dividend_fetch
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends
from crawler.tasks_tri import build_tri
from crawler.tasks_backtests import backtest_windows_from_tri

from database import SessionLocal
from database.main import (
    write_etl_sync_status_to_db,
    read_etl_sync_status,
)

_ALLOWED_SYNC_COLS = ["region", "last_price_date", "price_count", "last_dividend_ex_date", "dividend_count", "last_tri_date", "tri_count", "updated_at"]


def _merge_update_sync_status(row: Dict[str, Any], session) -> None:
    """
    [輔助函式]
    根據 etf_id 判斷並執行 etl_sync_status 表的資料新增 (Insert) 或動態更新 (Update)。
    """
    eid = row.get("etf_id")
    if not eid: return
    cur = read_etl_sync_status(etf_id=eid, session=session)
    if isinstance(cur, list): cur = cur[0] if cur else None
    if not cur:
        write_etl_sync_status_to_db([row], session=session)
        return
    set_parts, params = [], {"eid": eid}
    for col in _ALLOWED_SYNC_COLS:
        if col in row and row[col] is not None:
            set_parts.append(f"{col} = :{col}")
            params[col] = row[col]
    if not set_parts: return
    sql = text(f"UPDATE etl_sync_status SET {', '.join(set_parts)} WHERE etf_id = :eid")
    session.execute(sql, params)


@shared_task(name="workflow.generic_single_etf")
def process_single_etf_task(eid, etf_info, region):
    """
    步驟 B, C, D：單檔 ETF 的詳細處理邏輯
    回傳字典供步驟 E 統計使用
    """
    inception_date = etf_info.get("inception_date") or DEFAULT_START_DATE
    tri_added = 0
    
    # B.1 規劃
    plan_p = plan_price_fetch(etf_id=eid, inception_date=inception_date)
    plan_d = plan_dividend_fetch(etf_id=eid, inception_date=inception_date)
    
    # B.2 抓取
    p_res = fetch_daily_prices(etf_id=eid, plan=plan_p) if plan_p else None
    d_res = fetch_dividends(etf_id=eid, plan=plan_d, region=region) if plan_d else None
    
    new_records_p = int(p_res.get("price_new_records_count", 0) or 0) if p_res else 0
    
    # 寫入價格/股利同步狀態
    with SessionLocal.begin() as session:
        _merge_update_sync_status({
            "etf_id": eid,
            "last_price_date": p_res.get("price_latest_date") if p_res else None,
            "price_count": (int(plan_p.get("price_count", 0)) + new_records_p) if plan_p else 0,
            "last_dividend_ex_date": d_res.get("dividend_latest_date") if d_res else None,
            "dividend_count": (int(plan_d.get("dividend_count", 0)) + int(d_res.get("dividend_new_records_count", 0) if d_res else 0)) if plan_d else 0,
            "updated_at": datetime.now()
        }, session=session)

    # C & D：TRI 與回測
    if new_records_p > 0:
        tri_res = build_tri(etf_id=eid, region=region)
        tri_added = int(tri_res.get("tri_added", 0) or 0)
        last_tri_date = tri_res.get("last_tri_date")
        
        with SessionLocal.begin() as session:
            _merge_update_sync_status({
                "etf_id": eid, 
                "last_tri_date": last_tri_date, 
                "tri_count": int(tri_res.get("tri_count_new") or 0)
            }, session=session)
            
        if tri_added > 0:
            backtest_windows_from_tri(etf_id=eid, end_date=last_tri_date, windows_years=BACKTEST_WINDOWS_YEARS)
            logger.info(f"[{eid}] 非同步回測完成。")
    else:
        logger.info(f"[{eid}] 無新增價格，跳過 TRI 與回測。")

    # 回傳結果給收尾任務 (Stage E)
    return {"etf_id": eid, "tri_added": tri_added}

@shared_task(name="workflow.generic_summary")
def stage_e_summary_task(results: List[Dict[str, Any]], region):
    """
    步驟 E：同步收尾總結日誌
    此任務會在所有 process_single_etf_task 完成後觸發
    """
    logger.info(f"===== 步驟 E：開始同步收尾總結[地區：{region}] =====")
    
    updated_this_run_ids = [
        r["etf_id"] for r in results 
        if isinstance(r, dict) and r.get("tri_added", 0) > 0
    ]    
    all_processed_ids = [r["etf_id"] for r in results]

    if updated_this_run_ids:
        logger.info("【總結】本次執行有更新 TRI 資料的 ETF：共 %d 檔 → %s",
                    len(updated_this_run_ids), updated_this_run_ids)
    else:
        logger.info("【總結】本次執行中，所有 ETF 均無新的 TRI 資料需要更新。")

    # 更新所有相關 ETF 的 updated_at (保留原邏輯)
    try:
        now_dt = datetime.now()
        with SessionLocal.begin() as session:
            for eid in all_processed_ids:
                _merge_update_sync_status({"etf_id": eid, "updated_at": now_dt}, session=session)
        logger.info("已更新所有 %d 檔 ETF 的 `updated_at=%s`。", len(all_processed_ids), now_dt.isoformat(timespec="seconds"))
    except Exception as e:
        logger.exception("更新 `etl_sync_status.updated_at` 時發生錯誤：%s", e)

    logger.info("===== 步驟 E：同步收尾完成 =====")

