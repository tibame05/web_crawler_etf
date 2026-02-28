# crawler/producer_main_tw.py
from __future__ import annotations
from celery import chord, shared_task

from crawler import logger
from crawler.config import REGION_TW
from crawler.tasks_etf_list_tw import fetch_tw_etf_list
from crawler.tasks_align import align_step0
from crawler.workflow_templates import (
    _merge_update_sync_status, 
    process_single_etf_task, 
    stage_e_summary_task
)

from database import SessionLocal
from database.main import (
    read_etl_sync_status,
)

DATE_FMT = "%Y-%m-%d"

@shared_task(name="workflow.stage_a_align_tw", queue="crawler_tw")
def stage_a_align_task_tw():
    """步驟 A：名單對齊與初始補建"""
    logger.info("【台股 ETF 資訊同步】非同步主流程啟動...")
    crawler_url = "https://tw.stock.yahoo.com/tw-etf"
    
    # 1. 抓取原始名單與對齊
    src_rows = fetch_tw_etf_list(crawler_url=crawler_url, region=REGION_TW)
    etfs_data_list = align_step0(region=REGION_TW, src_rows=src_rows, use_yfinance=True)
    
    id2info = {d['etf_id']: d for d in etfs_data_list}
    active_ids = sorted(id2info.keys())
    
    # 2. 初始檢查與補建追蹤表 (etl_sync_status)
    with SessionLocal.begin() as session:
        new_count = 0
        for eid in active_ids:
            row = read_etl_sync_status(etf_id=eid, session=session)
            if not row:
                _merge_update_sync_status({
                    "etf_id": eid, 
                    "region": REGION_TW, 
                    "price_count": 0,
                    "dividend_count": 0,
                    "tri_count": 0
                }, session=session)
                new_count += 1
        logger.info("步驟 A.5：已成功寫入 %d 筆新 ETF 狀態，總計處理 %d 檔。", new_count, len(active_ids))

    # 3. 使用 Celery Chord 派發並行任務
    # header: 每一檔 ETF 獨立執行規劃、抓取、計算 TRI 與回測
    # callback: 當所有 ETF 處理完後，執行總結報告
    header = [
        process_single_etf_task.s(eid, id2info[eid], REGION_TW).set(queue="crawler_tw")
        for eid in active_ids
    ]
    callback = stage_e_summary_task.s(REGION_TW).set(queue="crawler_tw")
    
    chord(header)(callback)
    logger.info("【台股 ETF】已成功派發並行任務，等待所有任務完成後將執行總結任務。")


def main_tw():
    """台股非同步啟動入口"""
    stage_a_align_task_tw.delay()
    return {"status": "workflow_started"}

if __name__ == "__main__":
    main_tw()