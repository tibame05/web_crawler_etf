# crawler/tasks_sync.py
from typing import Dict, Any, List, Set
from datetime import datetime
from sqlalchemy import text
from database.main import read_etl_sync_status, write_etl_sync_status_to_db
from crawler import logger

_ALLOWED_SYNC_COLS = [
    "region", "last_price_date", "price_count",
    "last_dividend_ex_date", "dividend_count",
    "last_tri_date", "tri_count", "updated_at",
]

def merge_update_sync_status(row: Dict[str, Any], session) -> None:
    """更新或插入單筆 ETF 同步狀態（僅更新非 None 欄位）"""
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

def init_missing_sync_status(active_ids: List[str], region: str, session) -> None:
    """自動檢查並初始化資料庫中缺少的 ETF 追蹤紀錄"""
    existing_sync_ids: Set[str] = set()
    missing_sync_ids: List[str] = []

    for eid in active_ids:
        row = read_etl_sync_status(etf_id=eid, session=session)
        if isinstance(row, list): row = row[0] if row else None
        
        if row and row.get("etf_id"):
            existing_sync_ids.add(row["etf_id"])
        else:
            missing_sync_ids.append(eid)

    logger.info("步驟 A.3：資料庫 `etl_sync_status` 中已存在 %d 筆 ETF 追蹤紀錄。", len(existing_sync_ids))

    if missing_sync_ids:
        logger.info("步驟 A.4：發現 %d 筆新 ETF 需加入追蹤：%s", len(missing_sync_ids), missing_sync_ids)
        rows_to_insert = [
            {
                "etf_id": eid,
                "region": region,
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
        return len(rows_to_insert)
    return 0

def update_all_timestamp(etf_ids: List[str], session) -> int:
    """統一更新指定 ETF 清單的 updated_at 時間戳記"""
    now_dt = datetime.now()
    for eid in etf_ids:
        merge_update_sync_status({"etf_id": eid, "updated_at": now_dt}, session=session)
    return len(etf_ids)