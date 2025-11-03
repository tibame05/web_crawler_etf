# debug/step0_tasks_align.py✅
# 目的：
# - 不連真 DB：fake read_etfs_id / write_etfs_to_db
# - 叫真 yfinance：不 mock
# - 同一支腳本依序測 TW 與 US 兩組輸入

import json
import time
from typing import List, Dict, Any
import crawler.tasks_align as mod

# ---- 模擬 DB（只會回 ACTIVE；正式 SQL 已限制）----
FAKE_DB_ROWS: List[Dict[str, Any]] = [
    {"etf_id": "0050.TW", "region": "TW"},
    {"etf_id": "VOO",     "region": "US"},
]

def fake_read_etfs_id(session=None, region=None) -> List[Dict[str, Any]]:
    if region:
        return [r for r in FAKE_DB_ROWS if r["region"] == region]
    return list(FAKE_DB_ROWS)

mod.read_etfs_id = fake_read_etfs_id  # 套用

# 攔截寫 DB：只記錄 payload，不入庫
def make_capture():
    box = {"rows": None}
    def fake_write(rows, session=None):
        box["rows"] = rows
    return box, fake_write

# ---- 兩組測試資料：TW 與 US 分開跑 ----
cases = [
    ("TW", [
        {"etf_id": "0050.TW",   "etf_name": "元大台灣50",     "region": "TW", "currency": "TWD"},
        {"etf_id": "006204.TW", "etf_name": "永豐臺灣加權",   "region": "TW", "currency": "TWD"},
    ]),
    ("US", [
        {"etf_id": "VOO", "etf_name": "Vanguard S&P 500 ETF",          "region": "US", "currency": "USD"},
        {"etf_id": "VTI", "etf_name": "Vanguard Total Stock Market ETF","region": "US", "currency": "USD"},
    ]),
]

for REGION, SRC_ROWS in cases:
    _CAPTURED, fake_write_etfs_to_db = make_capture()
    mod.write_etfs_to_db = fake_write_etfs_to_db  # 套用攔截器（每輪重置）

    print("\n" + "="*80)
    print(f"### RUN align_step0 | REGION={REGION} | n_src={len(SRC_ROWS)}")
    print("="*80)

    out = mod.align_step0(
        region=REGION,
        src_rows=SRC_ROWS,
        use_yfinance=True,  # 真 yfinance
        session=None,
    )

    print("\n--- align_step0 回傳（final_etfs_data）---")
    print(json.dumps(out, ensure_ascii=False, indent=2))

    print("\n--- write_etfs_to_db 準備寫入（攔截，不入庫）---")
    print(json.dumps(_CAPTURED.get("rows"), ensure_ascii=False, indent=2))

    # 友善：兩輪之間小睡一下，避免 yfinance 節流
    time.sleep(0.5)
