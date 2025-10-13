# debug/step1_tasks_plan.py✅
# 目的：
# - 使用「假的 DB」測試 crawler/tasks_plan.py（保留你的 DB 函式簽名）
# - read_etl_sync_status(etf_id) -> List[Dict[str, Any]]
# - 測試情境：
#   0050.TW、VTI：last_price_date = 2025-11-09（未來，應回 None）
#   006204.TW、VOO：last_price_date = None（首次抓，應從 inception 或 DEFAULT_START_DATE）

import json
from typing import List

import crawler.tasks_plan as plan

# ---- 測試輸入（你指定的）----
TW = [
  {"etf_id": "0050.TW",   "inception_date": "2003-06-25"},
  {"etf_id": "006204.TW", "inception_date": "2011-09-06"},
]
US = [
  {"etf_id": "VOO", "inception_date": "2020-09-07"}, # inception_date為假資料，實際為2010-09-07
  {"etf_id": "VTI", "inception_date": "2001-05-24"},
]

# ---- 假資料庫（符合你的狀況）----
FAKE_SYNC = {
    # 未來 anchor（應該回 None）
    "0050.TW": {
        "etf_id": "0050.TW",
        "last_price_date": "2025-10-08",  # 注意補 0：YYYY-MM-DD
        "price_count": 123,
        "last_dividend_ex_date": "2025-10-01",
        "dividend_count": 50,
        "last_tri_date": None,
        "tri_count": 0,
        "updated_at": None,
    },
    # 首次抓（沒有 anchor）
    "006204.TW": {
        "etf_id": "006204.TW",
        "last_price_date": None,
        "price_count": 0,
        "last_dividend_ex_date": None,
        "dividend_count": 0,
        "last_tri_date": None,
        "tri_count": 0,
        "updated_at": None,
    },
    # 首次抓（沒有 anchor）
    "VOO": {
        "etf_id": "VOO",
        "last_price_date": None,
        "price_count": 0,
        "last_dividend_ex_date": None,
        "dividend_count": 0,
        "last_tri_date": None,
        "tri_count": 0,
        "updated_at": None,
    },
    # 未來 anchor（應該回 None）
    "VTI": {
        "etf_id": "VTI",
        "last_price_date": "2025-10-09",
        "price_count": 456,
        "last_dividend_ex_date": "2025-09-01",
        "dividend_count": 100,
        "last_tri_date": None,
        "tri_count": 0,
        "updated_at": None,
    },
}

# ---- 用假的 DB 函式覆蓋你的 read_etl_sync_status（維持簽名與回傳 List[Dict]）----
def fake_read_etl_sync_status(etf_id: str, session=None):
    row = FAKE_SYNC.get(etf_id)
    return [row] if row else []

plan.read_etl_sync_status = fake_read_etl_sync_status  # monkeypatch

def run_batch(title: str, rows: List[dict]):
    print("\n" + "="*80)
    print(title)
    print("="*80)

    out_price = {}
    out_div   = {}

    for r in rows:
        eid = r["etf_id"]
        inc = r["inception_date"]

        p = plan.plan_price_fetch(eid, inception_date=inc, session=None)
        d = plan.plan_dividend_fetch(eid, inception_date=inc, session=None)

        out_price[eid] = p
        out_div[eid]   = d

    print("\n[Price plans]")
    print(json.dumps(out_price, ensure_ascii=False, indent=2))
    print("\n[Dividend plans]")
    print(json.dumps(out_div, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    run_batch("TW 測試（假 DB）", TW)
    run_batch("US 測試（假 DB）", US)
