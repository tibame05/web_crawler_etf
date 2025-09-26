# crawler/tasks_sync.py
from datetime import datetime, timedelta, date
from typing import Dict, Any, Optional
from crawler import logger
from crawler.config import DEFAULT_START_DATE
from database.main import read_etl_sync_status
from crawler.worker import app

def _to_date(s: Optional[str]) -> Optional[date]:
    """[輔助函式] 將 YYYY-MM-DD 格式字串轉換為 date 物件。"""
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()

def _next_day(d: date) -> date:
    """[輔助函式] 取得隔天的日期。"""
    return d + timedelta(days=1)

def _pick_lower_bound(inception_date: Optional[str]) -> date:
    """[輔助函式] 決定資料抓取的最小起始日期（以 ETF 成立日或預設起始日為準）。"""
    lower_default = _to_date(DEFAULT_START_DATE)  # e.g. "2015-01-01"
    inc = _to_date(inception_date) if inception_date else None
    # 如果成立日存在且晚於預設起始日，則使用成立日作為起點
    return inc if (inc and inc >= lower_default) else lower_default

def _today() -> date:
    """[輔助函式] 取得今天的日期。"""
    return datetime.today().date()

@app.task()
def plan_price_fetch(
    etf_id: str,
    inception_date: Optional[str] = None,   # "YYYY-MM-DD"
) -> Optional[Dict[str, str]]:
    """
    規劃『價格』抓取區間。
    只回傳必要的抓取起始日期和現有價格數量（均為字串），詳細計畫輸出至 Log。

    回傳：
      - None: 無需補資料 (start > end)
      - {"start": "YYYY-MM-DD", "price_count": "N"}
    """
    # 1. 讀取現有的同步狀態
    sync = read_etl_sync_status(etf_id=etf_id) or {}
    last_price_date_str = sync.get("last_price_date")
    price_cnt = int(sync.get("price_count") or 0)

    # 2. 決定新的抓取起始日期 (start)
    anchor = _to_date(last_price_date_str)
    if anchor:
        # 如果有上次成功抓取的日期 (anchor)，則從「隔天」開始抓取
        start = _next_day(anchor)
        anchor_src = "last_price_date"
    else:
        # 如果是第一次抓取 (無 anchor)，則從 ETF 成立日或預設起始日開始抓取
        start = _pick_lower_bound(inception_date)
        anchor_src = "inception_or_default"

    end = _today()

    # 3. 檢查是否需要補資料
    if start > end:
        # 如果起始日晚於今天，表示資料已經是最新，無需補資料
        logger.info("[PLAN][PRICE] %s 無需補資料（start=%s > end=%s）", etf_id, start, end)
        return None

    # 4. 輸出完整的 Plan 到 Log 中 (供查閱使用)
    plan = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": (end - start).days + 1,
        "price_count": price_cnt,
        "anchor": anchor_src,
        "last_price_date": last_price_date_str,
    }
    logger.info("[PLAN][PRICE] %s → %s", etf_id, plan)

    # 5. 回傳精簡的結果給呼叫方
    return {
        "start": start.isoformat(),
        "price_count": str(price_cnt),
    }


def plan_dividend_fetch(
    etf_id: str,
    inception_date: Optional[str] = None,   # "YYYY-MM-DD"
) -> Optional[Dict[str, str]]:
    """
    規劃『股利』抓取區間。
    只回傳必要的抓取起始日期和現有股利數量（均為字串），詳細計畫輸出至 Log。

    回傳：
      - None: 無需補資料 (start > end)
      - {"start": "YYYY-MM-DD", "dividend_count": "N"}
    """
    # 1. 讀取現有的同步狀態
    sync = read_etl_sync_status(etf_id=etf_id) or {}
    last_ex_date_str = sync.get("last_dividend_ex_date")
    div_cnt = int(sync.get("dividend_count") or 0)

    # 2. 決定新的抓取起始日期 (start)
    anchor = _to_date(last_ex_date_str)
    if anchor:
        # 如果有上次成功抓取的日期 (anchor)，則從「隔天」開始抓取
        start = _next_day(anchor)
        anchor_src = "last_dividend_ex_date"
    else:
        # 如果是第一次抓取 (無 anchor)，則從 ETF 成立日或預設起始日開始抓取
        start = _pick_lower_bound(inception_date)
        anchor_src = "inception_or_default"

    end = _today()

    # 3. 檢查是否需要補資料
    if start > end:
        # 如果起始日晚於今天，表示資料已經是最新，無需補資料
        logger.info("[PLAN][DIV] %s 無需補資料（start=%s > end=%s）", etf_id, start, end)
        return None

    # 4. 輸出完整的 Plan 到 Log 中 (供查閱使用)
    plan = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "dividend_count": div_cnt,
        "anchor": anchor_src,
        "last_dividend_ex_date": last_ex_date_str,
    }
    logger.info("[PLAN][DIV] %s → %s", etf_id, plan)

    # 5. 回傳精簡的結果給呼叫方
    return {
        "start": start.isoformat(),
        "dividend_count": str(div_cnt),
    }
