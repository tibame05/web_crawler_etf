# -*- coding: utf-8 -*-
"""
測試目的：
- 使用「真實 yfinance 抓取」的大量資料（不改原始碼）
- 攔截 DB 寫入 → 彙整 rows → 台美合併成單一 CSV
輸出：
  debug/files/<timestamp>/prices_all.csv
  debug/files/<timestamp>/dividends_all.csv
"""
import os
import json
import pandas as pd
import pytest

# 匯入原函式（不改原始檔）
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends

# ---------------------------- 初始化 ----------------------------
OUT_DIR = os.path.join("debug", "files")
os.makedirs(OUT_DIR, exist_ok=True)

ACC = {"prices": [], "dividends": []}  # 收集所有市場資料

# ---------------------------- 抓取清單 ----------------------------
ETF_JOBS = [
    ("0050.TW", "2015-01-01", "TW"),
    ("006204.TW", "2024-01-01", "TW"),
    ("VOO", "2015-01-01", "US"),
    ("VTI", "2022-01-01", "US"),
    ("IGSB", "2024-01-01", "US"),
]

# ---------------------------- 攔截 DB 寫入 ----------------------------
@pytest.fixture
def patch_db_writers(monkeypatch):
    """攔截 DB 寫入並收集成記憶體"""
    import crawler.tasks_fetch as mod

    def fake_write_prices(rows, session=None):
        if not rows:
            return
        for r in rows:
            ACC["prices"].append(dict(r))
        etf = rows[0].get("etf_id")
        print(f"[FAKE DB][PRICE] 收到 {etf} {len(rows)} 列")

    def fake_write_dividends(rows, session=None):
        if not rows:
            return
        for r in rows:
            ACC["dividends"].append(dict(r))
        etf = rows[0].get("etf_id")
        print(f"[FAKE DB][DIV] 收到 {etf} {len(rows)} 列")

    monkeypatch.setattr(mod, "write_etf_daily_price_to_db", fake_write_prices, raising=True)
    monkeypatch.setattr(mod, "write_etf_dividend_to_db", fake_write_dividends, raising=True)
    return True

# ---------------------------- 寫出合併 CSV ----------------------------
def _dump_csv_all():
    """合併台美資料輸出單一 CSV 檔"""
    # 歷史價格
    if ACC["prices"]:
        dfp = pd.DataFrame(ACC["prices"]).sort_values(["etf_id", "trade_date"])
        cols = ["etf_id", "trade_date", "high", "low", "open", "close", "adj_close", "volume"]
        for c in cols:
            if c not in dfp.columns:
                dfp[c] = pd.NA
        dfp = dfp[cols]
        path_p = os.path.join(OUT_DIR, "prices_all.csv")
        dfp.to_csv(path_p, index=False, encoding="utf-8")
        print(f"[WRITE] 合併價格 → {path_p}（{len(dfp):,} 列）")
    else:
        print("[WRITE] 合併價格：無資料。")

    # 配息
    if ACC["dividends"]:
        dfd = pd.DataFrame(ACC["dividends"]).sort_values(["etf_id", "ex_date"])
        cols = ["etf_id", "ex_date", "dividend_per_unit", "currency"]
        for c in cols:
            if c not in dfd.columns:
                dfd[c] = pd.NA
        dfd = dfd[cols]
        path_d = os.path.join(OUT_DIR, "dividends_all.csv")
        dfd.to_csv(path_d, index=False, encoding="utf-8")
        print(f"[WRITE] 合併配息 → {path_d}（{len(dfd):,} 列）")
    else:
        print("[WRITE] 合併配息：無資料。")

# ---------------------------- 主測試 ----------------------------
def test_fetch_all_to_csv(patch_db_writers):
    print("=" * 80)
    print("批次抓取台美 ETF 歷史資料與配息")
    print("=" * 80)

    # 抓歷史價格
    for etf_id, start, region in ETF_JOBS:
        plan = {"start": start}
        print(f"\n[PRICE] {etf_id} ({region}) {start} → 今天")
        res = fetch_daily_prices(etf_id=etf_id, plan=plan, session=None)
        print("[PRICE 回傳]", json.dumps(res, ensure_ascii=False))

    # 抓配息
    for etf_id, start, region in ETF_JOBS:
        plan = {"start": start}
        print(f"\n[DIV] {etf_id} ({region}) {start} → 今天")
        res = fetch_dividends(etf_id=etf_id, plan=plan, region=region, session=None)
        print("[DIV 回傳]", json.dumps(res, ensure_ascii=False))

    print("\n" + "=" * 80)
    print("寫出合併 CSV")
    print("=" * 80)
    _dump_csv_all()

    # 驗證是否有抓到資料
    total_rows = len(ACC["prices"]) + len(ACC["dividends"])
    assert total_rows > 0, "沒有任何資料被抓到，請檢查網路或 yfinance 限流。"
