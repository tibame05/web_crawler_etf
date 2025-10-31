# debug/step2_tasks_tri.py ✅
import json
from datetime import datetime

from crawler.tasks_tri import build_tri as build_tri_fn

# ======= 假資料定義（與測試一致） =======
FAKE_PRICES = {
    "0050.TW": [
        {"date": "2025-10-08", "close": 61.15, "adj_close": 61.15},
        {"date": "2025-10-09", "close": 61.60, "adj_close": 61.60},
        {"date": "2025-10-13", "close": 61.00, "adj_close": 61.00},
        {"date": "2025-10-14", "close": 60.80, "adj_close": 60.80},
        {"date": "2025-10-15", "close": 61.10, "adj_close": 61.10},
    ],
    "006204.TW": [
        {"date": "2015-01-05", "close": 46.10, "adj_close": 32.3577},
        {"date": "2015-01-06", "close": 45.16, "adj_close": 31.6979},
        {"date": "2015-01-07", "close": 45.27, "adj_close": 31.7751},
    ],
    "VOO": [
        {"date": "2025-10-23", "close": 617.44, "adj_close": 617.44},
        {"date": "2025-10-24", "close": 622.55, "adj_close": 622.55},
    ],
    "VTI": [
        {"date": "2025-10-10", "close": 321.80, "adj_close": 321.80},
        {"date": "2025-10-13", "close": 326.93, "adj_close": 326.93},
        {"date": "2025-10-14", "close": 326.83, "adj_close": 326.83},
    ],
    "IGSB": [
        {"date": "2025-07-01", "close": 52.510, "adj_close": 51.91848},
        {"date": "2025-07-02", "close": 52.530, "adj_close": 51.93826},
        {"date": "2025-07-03", "close": 52.480, "adj_close": 51.88882},
    ],
}

FAKE_DIVIDENDS = {
    "0050.TW": [
        {"date": "2015-10-26", "dividend_per_unit": 0.50},
        {"date": "2025-10-14", "dividend_per_unit": 0.10},
    ],
    "006204.TW": [
        {"date": "2020-10-23", "dividend_per_unit": 0.76},
        {"date": "2021-10-19", "dividend_per_unit": 5.30},
    ],
    "VOO": [],
    "VTI": [],
    "IGSB": [],
}

# ======= 猴補（直接覆寫 crawler.tasks_tri 命名空間） =======
import crawler.tasks_tri as tri_mod
import pandas as pd

WRITE_SINK = []

def _log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}")

def read_etl_sync_status(etf_id, session=None):
    return {"etf_id": etf_id, "last_tri_date": None, "tri_count": 0}

def read_tris_range(etf_id, start=None, end=None, limit=None, order="asc", session=None):
    return {"records": []}

def read_prices_range(etf_id, start=None, end=None, session=None):
    return {"records": FAKE_PRICES.get(etf_id, [])}

def read_dividends_range(etf_id, start=None, end=None, session=None):
    rows = FAKE_DIVIDENDS.get(etf_id, [])
    return {"records": [{"date": r["date"], "dividend_per_unit": r["dividend_per_unit"]} for r in rows]}

def write_etf_tris_to_db(df, session=None):
    WRITE_SINK.append(df.copy())
    print("  [FAKE DB][TRI] 將寫入 %d 筆；session=%s" % (len(df), session))
    prv = df.head(3).to_dict(orient="records")
    print("    預覽前幾筆：", json.dumps(prv, ensure_ascii=False, indent=2))

def _get_currency_from_region(region, etf_id):
    return "USD" if region == "US" else "TWD"

# 套用猴補
tri_mod.read_etl_sync_status = read_etl_sync_status
tri_mod.read_tris_range = read_tris_range
tri_mod.read_prices_range = read_prices_range
tri_mod.read_dividends_range = read_dividends_range
tri_mod.write_etf_tris_to_db = write_etf_tris_to_db
tri_mod._get_currency_from_region = _get_currency_from_region

# ======= 跑一輪（TW / US） =======
def run_case(etf_id, region):
    print(f"\n[TRI]  etf_id={etf_id}, region={region}")
    res = build_tri_fn(etf_id=etf_id, region=region, base=1000.0, session=None)
    print("[TRI 回傳]", json.dumps(res, ensure_ascii=False))

def main():
    print("="*80)
    print("TRI 測試（假讀寫）")
    print("="*80)

    # TW
    run_case("0050.TW", "TW")
    run_case("006204.TW", "TW")

    # US
    run_case("VOO", "US")
    run_case("VTI", "US")
    run_case("IGSB", "US")

    # 彙整
    print("\n[TRI 寫入總結]")
    total = sum(len(df) for df in WRITE_SINK)
    print(f"  批次數：{len(WRITE_SINK)}，總列數：{total}")
    for i, df in enumerate(WRITE_SINK, 1):
        print(f"  批次#{i}: {df['etf_id'].iloc[0]}  rows={len(df)}  last={df['tri_date'].iloc[-1]}")

if __name__ == "__main__":
    main()
