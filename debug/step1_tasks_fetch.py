# debug/step1_tasks_fetch.py
# 目的：
# - 用「真 yfinance」測 crawler/tasks_fetch.py
# - 但把 DB 寫入換成「假寫入」（不落 DB，只印出準備要寫入的內容）
# - plan 以字串提供，這裡會自動轉成 {"start": plan_str}

from __future__ import annotations
import json
from typing import Any, Dict, List

import crawler.tasks_fetch as tf  # 測試目標：含 fetch_daily_prices / fetch_dividends

# -------------------------------
# 0) 可選：固定今天（若想跑可重現，解除註解）
# -------------------------------
# tf._today_str = lambda: "2025-10-12"

# -------------------------------
# 1) 假寫入函式（取代真正 DB 寫入）
# -------------------------------
PRINT_ROWS_LIMIT = 3  # 預覽前幾筆

def fake_write_etf_daily_price_to_db(rows: List[Dict[str, Any]], session=None):
    print(f"  [FAKE DB][PRICE] 將寫入 {len(rows)} 筆；session={session}")
    if rows:
        preview = rows[:PRINT_ROWS_LIMIT]
        print("    預覽前幾筆：", json.dumps(preview, ensure_ascii=False, indent=2))

def fake_write_etf_dividend_to_db(rows: List[Dict[str, Any]], session=None):
    print(f"  [FAKE DB][DIV]   將寫入 {len(rows)} 筆；session={session}")
    if rows:
        preview = rows[:PRINT_ROWS_LIMIT]
        print("    預覽前幾筆：", json.dumps(preview, ensure_ascii=False, indent=2))

# 用假寫入覆蓋掉 crawler.tasks_fetch 內匯入的 DB 寫入函式
tf.write_etf_daily_price_to_db = fake_write_etf_daily_price_to_db
tf.write_etf_dividend_to_db    = fake_write_etf_dividend_to_db

# -------------------------------
# 2) 測試輸入（你的需求）
# -------------------------------
PRICE_TW = [
  {"etf_id": "0050.TW",   "plan": "2025-10-08"},
  {"etf_id": "006204.TW", "plan": "2015-01-05"},
]
PRICE_US = [
  {"etf_id": "VOO",  "plan": "2025-10-23"},
  {"etf_id": "VTI",  "plan": "2025-10-10"},
  {"etf_id": "IGSB", "plan": "2025-07-01"},
]

DIV_TW = [
  {"etf_id": "0050.TW",   "plan": "2015-10-26"},
  {"etf_id": "006204.TW", "plan": "2020-10-23"},
]
DIV_US = [
  {"etf_id": "VOO",  "plan": "2024-09-07"},
  {"etf_id": "VTI",  "plan": "2024-05-24"},
  {"etf_id": "IGSB", "plan": "2024-12-18"},
]

# -------------------------------
# 3) 小工具：把字串 plan 轉 dict
# -------------------------------
def normalize_plan(v: Any) -> Dict[str, str]:
    """允許 plan 是 'YYYY-MM-DD' 或 {'start': 'YYYY-MM-DD'}；都轉成 dict 格式。"""
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        return {"start": v}
    raise TypeError(f"不支援的 plan 型別：{type(v)}")

# -------------------------------
# 4) 執行批次
# -------------------------------
def run_price_batch(title: str, region: str, items: List[Dict[str, Any]], session=None):
    print("\n" + "=" * 88)
    print(f"{title}（region={region}）")
    print("=" * 88)

    results: Dict[str, Any] = {}
    for it in items:
        etf_id = it["etf_id"]
        plan   = normalize_plan(it["plan"])
        print(f"\n[PRICE] etf_id={etf_id}, plan={plan}")
        try:
            ret = tf.fetch_daily_prices(etf_id, plan, session=session)
        except Exception as e:
            ret = {"error": str(e)}
        results[etf_id] = ret

    print("\n[PRICE 回傳彙整]")
    print(json.dumps(results, ensure_ascii=False, indent=2))


def run_div_batch(title: str, region: str, items: List[Dict[str, Any]], session=None):
    print("\n" + "=" * 88)
    print(f"{title}（region={region}）")
    print("=" * 88)

    results: Dict[str, Any] = {}
    for it in items:
        etf_id = it["etf_id"]
        plan   = normalize_plan(it["plan"])
        print(f"\n[DIV]   etf_id={etf_id}, plan={plan}, region={region}")
        try:
            ret = tf.fetch_dividends(etf_id, plan, region=region, session=session)
        except Exception as e:
            ret = {"error": str(e)}
        results[etf_id] = ret

    print("\n[DIV 回傳彙整]")
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    # 若你想傳真的 DB session（但我們已改為假寫入，不會動 DB），可以在這裡建立並傳入：
    # from database.session import SessionLocal
    # session = SessionLocal()
    session = None

    # 日價格
    run_price_batch("TW 日價格測試（真 yfinance／假寫入）", "TW", PRICE_TW, session=session)
    run_price_batch("US 日價格測試（真 yfinance／假寫入）", "US", PRICE_US, session=session)

    # 配息
    run_div_batch("TW 配息測試（真 yfinance／假寫入）", "TW", DIV_TW, session=session)
    run_div_batch("US 配息測試（真 yfinance／假寫入）", "US", DIV_US, session=session)
