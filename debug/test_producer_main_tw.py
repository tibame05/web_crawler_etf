# debug/test_producer_main_tw.py
import os
import json
import pandas as pd
import datetime as dt
import pytest

# 被測模組
import crawler.producer_main_tw as main_tw
from crawler.config import DEFAULT_START_DATE

# 這個 dict 用來記錄被呼叫次數（驗證流程有跑到）
CALLS = {
    "fetch_tw_etf_list": 0,
    "align_step0": 0,
    "plan_price_fetch": 0,
    "plan_dividend_fetch": 0,
    "fetch_daily_prices": 0,
    "fetch_dividends": 0,
    "read_etl_sync_status": 0,
    "write_etl_sync_status_to_db": 0,
    "build_tri": 0,
    "backtest_windows_from_tri": 0,
}

@pytest.fixture(autouse=True)
def _reset_calls():
    CALLS.update({k: 0 for k in CALLS})
    yield

def _today_str():
    return dt.datetime.today().strftime("%Y-%m-%d")

class DummySession:  # 產品程式不會用到方法，佔位即可
    pass

class DummySessionMaker:
    """模擬 SQLAlchemy sessionmaker().begin() 的行為"""
    def begin(self):
        class _CM:
            def __enter__(_self):
                return DummySession()
            def __exit__(_self, exc_type, exc, tb):
                return False
        return _CM()

# ---------------------------
# 基礎猴補：流程跑通（不連 DB/網路）
# ---------------------------
@pytest.fixture
def patch_everything(monkeypatch):
    # 1) SessionLocal → Dummy
    monkeypatch.setattr(main_tw, "SessionLocal", DummySessionMaker(), raising=False)

    # 2) 假名單：Yahoo 爬蟲
    def fake_fetch_tw_etf_list(url, region):
        CALLS["fetch_tw_etf_list"] += 1
        return [{"etf_id": "0050.TW", "etf_name": "元大台灣50", "region": "TW", "currency": "TWD"}]
    monkeypatch.setattr(main_tw, "fetch_tw_etf_list", fake_fetch_tw_etf_list, raising=True)

    # 3) 假對齊
    def fake_align_step0(*, region, src_rows, use_yfinance=True, session=None):
        CALLS["align_step0"] += 1
        return [{"etf_id": "0050.TW", "inception_date": "2003-06-30"}]
    monkeypatch.setattr(main_tw, "align_step0", fake_align_step0, raising=True)

    # 4) 假規劃
    def fake_plan_price_fetch(etf_id, inception_date=None, session=None):
        CALLS["plan_price_fetch"] += 1
        return {"start": DEFAULT_START_DATE, "price_count": "1000"}

    def fake_plan_dividend_fetch(etf_id, inception_date=None, session=None):
        CALLS["plan_dividend_fetch"] += 1
        return {"start": DEFAULT_START_DATE, "dividend_count": "50"}
    monkeypatch.setattr(main_tw, "plan_price_fetch", fake_plan_price_fetch, raising=True)
    monkeypatch.setattr(main_tw, "plan_dividend_fetch", fake_plan_dividend_fetch, raising=True)

    # 5) 假抓取（要吃 **kwargs 才能容忍 region/session）
    def fake_fetch_daily_prices(etf_id, plan, *args, **kwargs):
        CALLS["fetch_daily_prices"] += 1
        return {"inserted": 10, "start": plan["start"], "end": _today_str(), "last_price_date": _today_str()}
    def fake_fetch_dividends(etf_id, plan, *args, **kwargs):
        CALLS["fetch_dividends"] += 1
        return {"inserted": 1, "start": plan["start"], "end": _today_str(), "last_dividend_ex_date": _today_str()}
    monkeypatch.setattr(main_tw, "fetch_daily_prices", fake_fetch_daily_prices, raising=True)
    monkeypatch.setattr(main_tw, "fetch_dividends", fake_fetch_dividends, raising=True)

    # 6) 假 read/寫 sync（read 可能以 region= 或 etf_id= 呼叫，都要容忍）
    def fake_read_etl_sync_status(*args, **kwargs):
        CALLS["read_etl_sync_status"] += 1
        if "region" in kwargs and "etf_id" not in kwargs:
            return []  # A 步驟：查整體追蹤清單 → 回空表示不用新增
        etf_id = kwargs.get("etf_id") or (args[0] if args else "0050.TW")
        return {
            "etf_id": etf_id,
            "last_price_date": "2025-10-23",
            "price_count": 1000,
            "last_dividend_ex_date": "2025-10-01",
            "dividend_count": 40,
            "last_tri_date": "2025-10-23",
            "tri_count": 5000,
        }
    def fake_write_etl_sync_status_to_db(rows, *args, **kwargs):
        CALLS["write_etl_sync_status_to_db"] += 1
        return {"upserted": len(rows)}
    monkeypatch.setattr(main_tw, "read_etl_sync_status", fake_read_etl_sync_status, raising=True)
    monkeypatch.setattr(main_tw, "write_etl_sync_status_to_db", fake_write_etl_sync_status_to_db, raising=True)

    # 7) 假建 TRI（預設：今日）
    def _fake_build_tri_generic(last_tri):
        def _inner(etf_id, region, session=None, base=None):
            CALLS["build_tri"] += 1
            return {"etf_id": etf_id, "last_tri_date": last_tri, "tri_count_new": 5}
        return _inner
    monkeypatch.setattr(main_tw, "build_tri", _fake_build_tri_generic(_today_str()), raising=True)

    # 8) 假回測
    def fake_backtest(etf_id, end_date, windows_years=None, session=None, **kwargs):
        CALLS["backtest_windows_from_tri"] += 1
        return {"etf_id": etf_id, "end_date": end_date,
                "windows_done": ["1y","3y","10y"], "windows_skipped": [],
                "written": 3}
    monkeypatch.setattr(main_tw, "backtest_windows_from_tri", fake_backtest, raising=True)

    yield

# ---------------------------
# 單獨的「把 etl_sync_status 攔成 CSV」fixture
# ---------------------------
SYNC_ROWS = []

@pytest.fixture
def patch_sync_writer(monkeypatch):
    def fake_write(rows, *args, **kwargs):
        if hasattr(rows, "to_dict"):
            data = rows.to_dict(orient="records")
        else:
            data = rows
        SYNC_ROWS.extend(data)
        print("[FAKE-UPSERT][etl_sync_status] incoming rows:")
        for r in data:
            print("  -", json.dumps(r, ensure_ascii=False))
        return {"upserted": len(data)}
    monkeypatch.setattr(main_tw, "write_etl_sync_status_to_db", fake_write, raising=True)
    yield
    # teardown：統一輸出 CSV
    os.makedirs("debug/files", exist_ok=True)
    if SYNC_ROWS:
        out = pd.DataFrame(SYNC_ROWS)
        cols = [c for c in ["etf_id","region","last_price_date","price_count",
                            "last_dividend_ex_date","dividend_count",
                            "last_tri_date","tri_count",
                            "updated_at","created_at"] if c in out.columns]
        out = out[cols] if cols else out
        out_path = "debug/files/etl_sync_status.csv"
        out.to_csv(out_path, index=False, encoding="utf-8")
        print(f"[WRITE] etl_sync_status → {out_path}（{len(out):,} 列）")
    else:
        print("[WRITE] 本次沒有 etl_sync_status 寫入")

# ---------------------------
# 測試案例
# ---------------------------
def test_main_tw_golden_path_runs_and_backtests(patch_everything, patch_sync_writer):
    """黃金路徑：今日有新 TRI → 會進回測"""
    result = main_tw.main_tw()
    # 流程必須完整打到各步驟一次以上
    assert CALLS["fetch_tw_etf_list"] == 1
    assert CALLS["align_step0"] == 1
    assert CALLS["plan_price_fetch"] == 1
    assert CALLS["plan_dividend_fetch"] == 1
    assert CALLS["fetch_daily_prices"] == 1
    assert CALLS["fetch_dividends"] == 1
    assert CALLS["build_tri"] >= 1
    assert CALLS["backtest_windows_from_tri"] >= 1

    # 回傳結構檢查（最基本）
    assert "summary" in result and "per_etf" in result
    per = result["per_etf"].get("0050.TW", {})
    assert per.get("tri", {}).get("status") == "ok"
    assert per.get("backtests", {}).get("status") in {"ok", "skipped_no_today_update", "error"}
    assert per.get("backtests", {}).get("status") == "ok"  # 今天 → 應該有回測

def test_main_tw_skip_backtest_when_no_new_tri(patch_everything, monkeypatch):
    """邊界：若 build_tri 回傳的 last_tri_date 不是今天 → main 要跳過回測"""
    yesterday = (dt.datetime.today() - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    def fake_build_tri_yesterday(etf_id, region, session=None, base=None):
        CALLS["build_tri"] += 1
        return {"etf_id": etf_id, "last_tri_date": yesterday, "tri_count_new": 2}
    monkeypatch.setattr(main_tw, "build_tri", fake_build_tri_yesterday, raising=True)

    result = main_tw.main_tw()

    assert CALLS["fetch_tw_etf_list"] == 1
    assert CALLS["align_step0"] == 1
    # 會抓資料、寫 sync，但不會回測
    assert CALLS["backtest_windows_from_tri"] == 0

    per = result["per_etf"].get("0050.TW", {})
    assert per.get("tri", {}).get("status") == "ok"
    assert per.get("backtests", {}).get("status") == "skipped_no_today_update"
