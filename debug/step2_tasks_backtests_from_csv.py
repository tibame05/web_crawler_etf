# -*- coding: utf-8 -*-
"""
根據 debug/files/tri.csv 跑回測（不碰原始回測程式碼）：
- 猴補 database.main.read_tris_range：從 tri.csv 回傳 payload
- 猴補 database.main.write_etf_backtest_results_to_db：彙整 rows → debug/files/backtest.csv
"""
import os
import json
import pandas as pd
import pytest

# === 這裡請依你的實際模組位置調整 ===
# 你之前提供的 backtest_windows_from_tri 函式應在某個模組，例如 crawler.tasks_backtest
from crawler.tasks_backtests import backtest_windows_from_tri  # 如果路徑不同，請改成正確模組

# ---------------------------------------------------------------------
# 路徑與輸入 / 輸出
# ---------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # tests/
PROJ_DIR = os.path.dirname(BASE_DIR)                   # 專案根目錄
FILES_DIR = os.path.join(PROJ_DIR, "debug", "files")
TRI_CSV = os.path.join(FILES_DIR, "tri.csv")
BACKTEST_OUT = os.path.join(FILES_DIR, "backtest.csv")

# 收集所有回測 rows（原本應寫 DB）
_ROWS = []

# ---------------------------------------------------------------------
# 前置：載入 tri.csv
# ---------------------------------------------------------------------
def _load_tri_df() -> pd.DataFrame:
    if not os.path.isfile(TRI_CSV):
        raise FileNotFoundError(f"找不到 TRI 檔案：{TRI_CSV}，請先產生 debug/files/tri.csv")
    df = pd.read_csv(TRI_CSV, dtype={"etf_id": str})
    cols = [c.strip().lower() for c in df.columns]
    df.columns = cols
    need = {"etf_id", "tri_date", "tri"}
    if not need.issubset(set(cols)):
        raise ValueError(f"tri.csv 欄位不足；需要 {need}，實得 {set(cols)}")
    # 正規化
    df["tri_date"] = pd.to_datetime(df["tri_date"], errors="coerce")
    df["tri"] = pd.to_numeric(df["tri"], errors="coerce")
    df = df.dropna(subset=["etf_id", "tri_date", "tri"])
    return df

TRI_DF = _load_tri_df()  # 測試載入一次即可

# ---------------------------------------------------------------------
# 猴補：把 DB I/O 變成 CSV 讀寫
# ---------------------------------------------------------------------
@pytest.fixture
def patch_db_io(monkeypatch):
    import pandas as pd
    import os
    import crawler.tasks_backtests as target_mod  # ← 目標是被測模組
    from debug.step2_tasks_backtests_from_csv import TRI_DF  # 同一份 TRI_DF

    # 乾脆把舊輸出刪掉，避免「殘留舊檔」誤導
    if os.path.exists(BACKTEST_OUT):
        os.remove(BACKTEST_OUT)

    # 用同一份 _ROWS（這裡直接引用測試檔的全域）
    global _ROWS
    _ROWS = []  # 每次測試先清空

    def fake_read_tris_range(etf_id: str, *args, **kwargs):
        start = kwargs.get("start") or kwargs.get("start_date")
        end   = kwargs.get("end")   or kwargs.get("end_date")
        order = kwargs.get("order", "asc")

        df = TRI_DF[TRI_DF["etf_id"] == etf_id].copy()
        if start:
            df = df[df["tri_date"] >= pd.to_datetime(start)]
        if end:
            df = df[df["tri_date"] <= pd.to_datetime(end)]
        df = df.sort_values("tri_date", ascending=(order != "desc"))

        recs = df.assign(tri_date=df["tri_date"].dt.strftime("%Y-%m-%d"))[
            ["tri_date", "tri"]
        ].to_dict(orient="records")
        return {"records": recs}

    def fake_write_etf_backtest_results_to_db(df, session=None):
        global _ROWS
        print(f"[FAKE DB][BACKTEST] 收到 {len(df)} 列")
        _ROWS.extend(df.to_dict(orient="records"))

    monkeypatch.setattr(target_mod, "read_tris_range", fake_read_tris_range, raising=True)
    monkeypatch.setattr(target_mod, "write_etf_backtest_results_to_db",
                        fake_write_etf_backtest_results_to_db, raising=True)
    yield

    # teardown：這裡只在真的有資料時才寫出 CSV
    if _ROWS:
        out = pd.DataFrame(_ROWS).sort_values(["etf_id", "label", "start_date"])
        os.makedirs(FILES_DIR, exist_ok=True)
        out.to_csv(BACKTEST_OUT, index=False, encoding="utf-8")
        print(f"[WRITE] 回測彙整 → {BACKTEST_OUT}（{len(out):,} 列）")
    else:
        print("[WRITE] 本次回測沒有任何可寫出的結果（可能年資不足或篩選為空）。")

# ---------------------------------------------------------------------
# 主測試：對 tri.csv 中的每個 etf_id 執行回測
# ---------------------------------------------------------------------
def test_backtest_from_tri_csv(patch_db_io):
    os.environ["BACKTEST_PROBE"] = "1"  # 讓被測模組印出 windows_years / end_dt / max_year / earliest_needed

    by_etf = TRI_DF.groupby("etf_id", sort=True)["tri_date"].max().reset_index()
    etf_end_dates = [(r["etf_id"], r["tri_date"].strftime("%Y-%m-%d")) for _, r in by_etf.iterrows()]

    print("=" * 80)
    print("Backtest from tri.csv（嚴格年窗；結果輸出到 debug/files/backtest.csv）")
    print("=" * 80)

    for etf_id, end_date in etf_end_dates:
        print(f"[RUN] {etf_id}  end={end_date}")
        res = backtest_windows_from_tri(etf_id=etf_id, end_date=end_date, windows_years=None, session=None)
        print("[RET]", json.dumps(res, ensure_ascii=False))

    print(f"[ASSERT PROBE] _ROWS 收集筆數：{len(_ROWS)}")
    assert len(_ROWS) > 0, "沒有任何回測結果；可能 tri.csv 年資不足，或全被視窗條件跳過。"