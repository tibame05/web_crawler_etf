# crawler/tasks_plan.py
import json
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional, List
from crawler import logger
from crawler.config import DEFAULT_START_DATE
from database.main import read_etl_sync_status
#from crawler.worker import app

_HARD_BASELINE = "2015-01-01"  # 當所有日期都錯誤時的最後防線

def _to_date(s: Optional[str]) -> Optional[date]:
    """[輔助函式] 將 YYYY-MM-DD 格式字串轉換為 date 物件。"""
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def _next_day(d: date) -> date:
    """[輔助函式] 取得隔天的日期。"""
    return d + timedelta(days=1)

def _today() -> date:
    """[輔助函式] 取得今天的日期。"""
    return datetime.today().date()

def _plan_from_sync(
    *,
    sync_row: Dict[str, Any],
    inception_date: Optional[str],
    today: date,
    anchor_field: str,     # ex: "last_price_date" / "last_dividend_ex_date"
    count_field: str,      # ex: "price_count" / "dividend_count"
) -> Dict[str, Any]:
    """
    通用規劃邏輯（價格/股利皆用；永遠回傳 dict）。
    起始來源（繁中，五擇一）：
      1) "sync表last_date有資料但錯誤：sync中的last_date >= today"
      2) "sync表last_date有資料：start為last_date + 1"
      3) "sync表無資料但日期錯誤：使用硬基礎日_HARD_BASELINE"
      4) "sync表無資料，且成立日較大：使用成立日inception_date"
      5) "sync表無資料，且預設起始日較大：使用預設起始日DEFAULT_START_DATE"
    """
    # 取 anchor（同步表中的最後日期）
    last_str = sync_row.get(anchor_field)
    anchor: Optional[date] = None
    try:
        anchor = _to_date(last_str)
    except Exception:
        # 同步表的日期壞掉就視為無 anchor（記錄在 meta）
        anchor = None
    
    # 取 count
    cnt = int(sync_row.get(count_field) or 0)

    # 解析 成立日 vs 預設起始日
    default_start_str = DEFAULT_START_DATE
    default_start = _to_date(DEFAULT_START_DATE)  # 這個若設錯會直接拋例外，讓你早發現

    inc_raw = inception_date
    try:
        inc_parsed = _to_date(inception_date) if inception_date else None
    except Exception:
        inc_parsed = None  # 成立日字串壞掉 → 視為「不可用」

    # log建立
    start_source = ""  # 五種情境之一
    meta: Dict[str, Any] = {
        "inception_raw": inc_raw,           # 成立日原始日期字串
        "default_start": default_start_str, # 預設起始日字串
    }

    # --- 決策 ---
    if anchor is not None:
        if anchor >= today:
            # 1) 同步表記錄錯誤/過新：直接 today
            start = today
            start_source = "sync表記錄錯誤last_date：sync中的last_date >= today"
            meta["reason"] = "anchor_ge_today"
        else:
            # 2) 正常：last_date + 1（但不超過 today）
            next_day = _next_day(anchor)
            if next_day >= today:
                start = today
                meta["reason"] = "next_day_ge_today"
            else:
                start = next_day
                meta["reason"] = "next_day_lt_today"
            start_source = "sync表last_date有資料：start為last_date + 1"
    else:
        # 無 anchor
        # 1) 先檢查「錯誤/未來」→ 情境三
        case3 = False
        reasons = []

        if inc_raw is not None and inc_parsed is None:
            case3 = True; reasons.append("inception_unparsable")
        if default_start and default_start > today:
            case3 = True; reasons.append("default_gt_today")
        if inc_parsed and inc_parsed > today:
            case3 = True; reasons.append("inception_gt_today")

        if case3:
            start = min(_to_date(_HARD_BASELINE), today)
            start_source = "sync表無資料但日期錯誤：使用硬基礎日_HARD_BASELINE"
            meta["reason"] = "+".join(reasons) or "date_error"
            meta["hard_baseline"] = _HARD_BASELINE

        else:
            # 2) 正常 → 情境四／五：取 max(成立日, DEFAULT_START_DATE)
            base_default = default_start or _to_date(_HARD_BASELINE)
            if inc_parsed and inc_parsed >= base_default:
                lower_bound, lb_src = inc_parsed, "inception_date"       # 情境四
            else:
                lower_bound, lb_src = base_default, "DEFAULT_START_DATE" # 情境五

            meta["lower_bound"] = lower_bound.isoformat()
            meta["lower_bound_source"] = lb_src

            if lower_bound > today:
                # 理論上不會發生（應該被 case3 擋住），保險起見再回退
                start = _to_date(_HARD_BASELINE)
                start_source = "sync表無資料但日期錯誤：使用硬基礎日_HARD_BASELINE"
                meta["notes"].append("lower_bound_gt_today_but_rerouted_to_case3")
                meta["hard_baseline"] = _HARD_BASELINE
            else:
                start = lower_bound
                start_source = (
                    "sync表無資料：使用成立日inception_date"
                    if lb_src == "inception_date"
                    else "sync表無資料：使用預設起始日DEFAULT_START_DATE"
                )


    return {
        "start": start.isoformat(),
        "count": str(cnt),
        "start_source": start_source,
        "start_source_meta": meta,
        "anchor_value": last_str,
    }

#@app.task()
def plan_price_fetch(
    etf_id: str,
    inception_date: Optional[str] = None,
    session=None,   # "YYYY-MM-DD"
) -> Optional[Dict[str, str]]:
    """
    規劃『價格』抓取區間。
    只回傳必要的抓取起始日期和現有價格數量（均為字串），詳細計畫輸出至 Log。

    回傳：
      - None: 無需補資料 (start > end)
      - {"start": "YYYY-MM-DD", "price_count": "N"}
    """
    rows: List[Dict[str, Any]] = read_etl_sync_status(etf_id=etf_id, session=session) or []
    sync_row: Dict[str, Any] = (rows[0] if rows else {})
    today = _today()

    try:
        common = _plan_from_sync(
            sync_row=sync_row,
            inception_date=inception_date,
            today=today,
            anchor_field="last_price_date",
            count_field="price_count",
        )
        start_d = _to_date(common["start"])
        days_span = max(0, (_to_date(today.isoformat()) - start_d).days + 1)

        payload = {
            "etf_id": etf_id,
            "fetch": "price",
            "start": common["start"],
            "end": today.isoformat(),
            "days": days_span,                          # 涵蓋天數
            "price_count": common["count"],
            "last_price_date": common["anchor_value"],
            "start_source": common["start_source"],       # 起始來源
            "start_source_meta": common["start_source_meta"],     # 起始來源詳細資訊
        }
        payload_str = json.dumps(payload, indent=4, ensure_ascii=False)
        logger.info("[PLAN][PRICE] %s → \n%s", etf_id, payload_str)

        return {
            "start": common["start"],
            "price_count": common["count"],
        }

    except Exception as e:
        logger.exception("[PLAN][PRICE] %s 產生規劃訊息時發生錯誤：%s", etf_id, e)
        return {
            "start": today.isoformat(),
            "price_count": "0",
        }

#@app.task()
def plan_dividend_fetch(
    etf_id: str,
    inception_date: Optional[str] = None,
    session=None,   # "YYYY-MM-DD"
) -> Optional[Dict[str, str]]:
    """
    規劃『股利』抓取區間。
    只回傳必要的抓取起始日期和現有股利數量（均為字串），詳細計畫輸出至 Log。

    回傳：
      - None: 無需補資料 (start > end)
      - {"start": "YYYY-MM-DD", "dividend_count": "N"}
    """
    rows: List[Dict[str, Any]] = read_etl_sync_status(etf_id=etf_id, session=session) or []
    sync_row: Dict[str, Any] = (rows[0] if rows else {})
    today = _today()

    try:
        common = _plan_from_sync(
            sync_row=sync_row,
            inception_date=inception_date,
            today=today,
            anchor_field="last_dividend_ex_date",
            count_field="dividend_count",
        )
        start_d = _to_date(common["start"])
        days_span = max(0, (_to_date(today.isoformat()) - start_d).days + 1)

        payload = {
            "etf_id": etf_id,
            "fetch": "dividend",
            "start": common["start"],
            "end": today.isoformat(),
            "days": days_span,                          # 涵蓋天數
            "dividend_count": common["count"],
            "last_dividend_ex_date": common["anchor_value"],
            "start_source": common["start_source"],     # 起始來源
            "start_source_meta": common["start_source_meta"],   # 起始來源詳細資訊
        }
        payload_str = json.dumps(payload, indent=4, ensure_ascii=False)
        logger.info("[PLAN][DIV] %s → \n%s", etf_id, payload_str)

        return {
            "start": common["start"],
            "dividend_count": common["count"],
        }

    except Exception as e:
        logger.exception("[PLAN][DIV] %s 產生規劃訊息時發生錯誤：%s", etf_id, e)
        return {
            "start": today.isoformat(),
            "dividend_count": "0",
        }