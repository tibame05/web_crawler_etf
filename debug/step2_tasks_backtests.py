# debug/step2_tasks_backtests.py 
"""
可直接跑的印表腳本：用「相同假資料」→ 先建 TRI → 再做績效（嚴格年窗 + 全期間）
建議路徑：debug/run_backtest_from_tri_fake.py
"""
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import json
import pandas as pd
import numpy as np

# ===== 你專案內的函式（用現成 build_tri） =====
from crawler.tasks_tri import build_tri as build_tri_fn

# ------------------------------------------------------------
# 0) 相同假資料（與前一版 TRI 測試一致）
# ------------------------------------------------------------
FAKE_PRICES = {
    # --- TW ---
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
    # --- US ---
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

# ------------------------------------------------------------
# 1) 假資料庫（記憶體）：TRI 存放 / 讀取
# ------------------------------------------------------------
DATE_FMT = "%Y-%m-%d"
TRI_STORE = {}  # key: etf_id -> list of {"tri_date","tri","currency"}

def _fake_db_tri_write(df):
    etf = df["etf_id"].iloc[0]
    rows = df[["tri_date", "tri", "currency"]].to_dict(orient="records")
    TRI_STORE.setdefault(etf, [])
    TRI_STORE[etf].extend(rows)
    # 排序 + 去重（保留最後）
    tmp = pd.DataFrame(TRI_STORE[etf]).sort_values("tri_date")
    tmp = tmp.drop_duplicates(subset=["tri_date"], keep="last")
    TRI_STORE[etf] = tmp.to_dict(orient="records")

def _fake_db_tri_read(etf_id, start=None, end=None):
    rows = TRI_STORE.get(etf_id, [])
    if start:
        rows = [r for r in rows if r["tri_date"] >= start]
    if end:
        rows = [r for r in rows if r["tri_date"] <= end]
    # 統一 payload 介面
    return {"records": [{"tri_date": r["tri_date"], "tri": r["tri"]} for r in rows]}

# ------------------------------------------------------------
# 2) 猴補 crawler.tasks_tri：用相同假資料建 TRI
# ------------------------------------------------------------
import crawler.tasks_tri as tri_mod
def _fake_read_etl_sync_status(etf_id, session=None):
    return {"etf_id": etf_id, "last_tri_date": None, "tri_count": 0}

def _fake_read_tris_range(etf_id, start=None, end=None, limit=None, order="asc", session=None):
    # 給 build_tri 在需要 seed 的情況（這裡仍回空，代表無 seed）
    return {"records": []}

def _fake_read_prices_range(etf_id, start=None, end=None, session=None):
    return {"records": FAKE_PRICES.get(etf_id, [])}

def _fake_read_dividends_range(etf_id, start=None, end=None, session=None):
    rows = FAKE_DIVIDENDS.get(etf_id, [])
    return {"records": [{"date": r["date"], "dividend_per_unit": r["dividend_per_unit"]} for r in rows]}

def _fake_write_etf_tris_to_db(df, session=None):
    print("  [FAKE DB][TRI] 寫入 %d 筆；etf=%s" % (len(df), df['etf_id'].iloc[0]))
    print("    預覽：", json.dumps(df.head(3).to_dict(orient="records"), ensure_ascii=False, indent=2))
    _fake_db_tri_write(df)

def _fake_get_currency_from_region(region, etf_id):
    return "USD" if region == "US" else "TWD"

# 套用猴補
tri_mod.read_etl_sync_status = _fake_read_etl_sync_status
tri_mod.read_tris_range = _fake_read_tris_range
tri_mod.read_prices_range = _fake_read_prices_range
tri_mod.read_dividends_range = _fake_read_dividends_range
tri_mod.write_etf_tris_to_db = _fake_write_etf_tris_to_db
tri_mod._get_currency_from_region = _fake_get_currency_from_region

# ------------------------------------------------------------
# 3) 轉換與指標計算（沿用你提供的核心）
# ------------------------------------------------------------
def _records_to_tri_series(payload) -> pd.Series:
    recs = (payload or {}).get("records", [])
    if not recs:
        return pd.Series(dtype=float)
    df = pd.DataFrame.from_records(recs)
    if "tri_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "tri_date"})
    s = pd.Series(df["tri"].astype(float).values, index=pd.to_datetime(df["tri_date"]))
    return s.sort_index()

def _compute_metrics_from_tri(
    tri: pd.Series,
    *,
    risk_free_rate_annual: float = 0.0,
    annualization: int = 252,
    use_calendar_years: bool = True,
) -> dict:
    s = tri.dropna().astype(float)
    if s.size < 2 or (s <= 0).any():
        return {
            "total_return": np.nan,
            "cagr": np.nan,
            "volatility": np.nan,
            "sharpe_ratio": np.nan,
            "max_drawdown": np.nan,
        }
    total_return = s.iloc[-1] / s.iloc[0] - 1.0
    if use_calendar_years and isinstance(s.index, (pd.DatetimeIndex, pd.PeriodIndex)):
        days = (s.index[-1] - s.index[0]).days
        years = days / 365.25 if days > 0 else np.nan
        cagr = (s.iloc[-1] / s.iloc[0]) ** (1.0 / years) - 1.0 if years and years > 0 else np.nan
    else:
        n = s.size - 1
        cagr = (s.iloc[-1] / s.iloc[0]) ** (annualization / n) - 1.0 if n > 0 else np.nan
    r = s.pct_change().dropna()
    if r.empty or r.std(ddof=1) == 0:
        volatility_ann = 0.0
        sharpe_ratio = np.nan
    else:
        volatility_ann = r.std(ddof=1) * np.sqrt(annualization)
        rf_daily = risk_free_rate_annual / annualization
        excess = r - rf_daily
        sharpe_ratio = (excess.mean() * np.sqrt(annualization)) / r.std(ddof=1)
    peak = s.cummax()
    drawdown = (peak - s) / peak
    max_drawdown = float(drawdown.max()) if not drawdown.empty else np.nan
    return {
        "total_return": float(total_return),
        "cagr": float(cagr) if pd.notna(cagr) else np.nan,
        "volatility": float(volatility_ann),
        "sharpe_ratio": float(sharpe_ratio) if pd.notna(sharpe_ratio) else np.nan,
        "max_drawdown": max_drawdown,
    }

def backtest_windows_from_tri(
    etf_id: str,
    end_date: str,
    windows_years=None,
    *,
    risk_free_rate_annual: float = 0.0,
    annualization: int = 252,
    session=None,
) -> dict:
    BACKTEST_WINDOWS_YEARS = list(windows_years or [1, 3, 10])
    end_dt: date = datetime.strptime(end_date, DATE_FMT).date()
    rows = []
    windows_done, windows_skipped = [], []

    payload_all = _fake_db_tri_read(etf_id, end=end_date)
    tri_all = _records_to_tri_series(payload_all)
    if tri_all.empty:
        print(f"[BACKTEST][{etf_id}] end={end_date} 無 TRI，全部跳過。")
        return {"etf_id": etf_id, "end_date": end_date, "inserted": 0,
                "windows_done": [], "windows_skipped": [f"{y}y" for y in BACKTEST_WINDOWS_YEARS]}

    actual_first = tri_all.index[0].date()
    actual_last = tri_all.index[-1].date()
    if actual_last < end_dt:
        print(f"[BACKTEST][{etf_id}] end={end_date} 但資料最後日為 {actual_last.strftime(DATE_FMT)}，以資料最後日計算。")
        end_dt = actual_last

    for y in BACKTEST_WINDOWS_YEARS:
        label = f"{y}y"
        target_start_dt = end_dt - relativedelta(years=y)
        if actual_first > target_start_dt:
            print(f"[BACKTEST][{etf_id}][{label}] 年資不足（first={actual_first} > target_start={target_start_dt}），跳過。")
            windows_skipped.append(label)
            continue
        tri = tri_all[tri_all.index.date >= target_start_dt]
        if tri.empty:
            print(f"[BACKTEST][{etf_id}][{label}] 視窗內無 TRI，跳過。")
            windows_skipped.append(label)
            continue

        metrics = _compute_metrics_from_tri(tri, risk_free_rate_annual=risk_free_rate_annual,
                                            annualization=annualization, use_calendar_years=True)
        win_start = tri.index[0].date()
        win_end = tri.index[-1].date()
        row = {
            "etf_id": etf_id, "label": label,
            "start_date": win_start.strftime(DATE_FMT),
            "end_date": win_end.strftime(DATE_FMT),
            "total_return": metrics["total_return"],
            "cagr": metrics["cagr"],
            "volatility": metrics["volatility"],
            "sharpe_ratio": metrics["sharpe_ratio"],
            "max_drawdown": metrics["max_drawdown"],
        }
        rows.append(row)
        windows_done.append(label)
        print(f"[BACKTEST][{etf_id}][{label}] start={row['start_date']} end={row['end_date']} "
              f"TR={row['total_return']:.6f} CAGR={row['cagr'] if pd.notna(row['cagr']) else float('nan'):.6f} "
              f"VOL={row['volatility']:.6f} SR={row['sharpe_ratio'] if pd.notna(row['sharpe_ratio']) else float('nan'):.6f} "
              f"MDD={row['max_drawdown']:.6f}")

    # 模擬寫 DB（印表）
    if rows:
        df_out = pd.DataFrame(rows).sort_values(["start_date"])
        print("  [FAKE DB][BACKTEST] 將寫入 %d 筆；預覽：" % len(df_out))
        print(json.dumps(df_out.head(5).to_dict(orient="records"), ensure_ascii=False, indent=2))
        inserted = len(df_out)
    else:
        inserted = 0

    print(f"[BACKTEST][{etf_id}] end={end_date} 已寫入 {inserted} 筆；完成: {windows_done}；跳過: {windows_skipped}")
    return {"etf_id": etf_id, "end_date": end_date}

# 方便觀察：補一個全期間（full period）績效
def backtest_full_period(etf_id: str):
    payload = _fake_db_tri_read(etf_id)
    tri = _records_to_tri_series(payload)
    if tri.empty or len(tri) < 2:
        print(f"[FULL][{etf_id}] TRI 點數不足，略過。")
        return
    m = _compute_metrics_from_tri(tri)
    s = tri.index[0].strftime(DATE_FMT)
    e = tri.index[-1].strftime(DATE_FMT)
    print(f"[FULL][{etf_id}] start={s} end={e} "
          f"TR={m['total_return']:.6f} CAGR={m['cagr'] if pd.notna(m['cagr']) else float('nan'):.6f} "
          f"VOL={m['volatility']:.6f} SR={m['sharpe_ratio'] if pd.notna(m['sharpe_ratio']) else float('nan'):.6f} "
          f"MDD={m['max_drawdown']:.6f}")

# ------------------------------------------------------------
# 4) 主程式：建 TRI → 回測（年窗 + 全期間）
# ------------------------------------------------------------
def _run_build_tri_one(etf_id, region):
    print(f"\n[TRI]  etf_id={etf_id}, region={region}")
    res = build_tri_fn(etf_id=etf_id, region=region, base=1000.0, session=None)
    print("[TRI 回傳]", json.dumps(res, ensure_ascii=False))

def main():
    print("="*80)
    print("TRI → 績效 測試（相同假資料；假讀寫）")
    print("="*80)

    # 先「建 TRI」
    _run_build_tri_one("0050.TW", "TW")
    _run_build_tri_one("006204.TW", "TW")
    _run_build_tri_one("VOO", "US")
    _run_build_tri_one("VTI", "US")
    _run_build_tri_one("IGSB", "US")

    # 設定回測截止日：取各自 TRI 的最後一天
    def _get_end(etf):
        payload = _fake_db_tri_read(etf)
        s = _records_to_tri_series(payload)
        return s.index[-1].strftime(DATE_FMT) if not s.empty else datetime.today().strftime(DATE_FMT)

    print("\n" + "="*80)
    print("回測（嚴格年窗：1y / 3y / 10y；資料不足者會跳過）")
    print("="*80)
    for etf in ["0050.TW", "006204.TW", "VOO", "VTI", "IGSB"]:
        end_date = _get_end(etf)
        backtest_windows_from_tri(etf, end_date=end_date, windows_years=[1, 3, 10])

    print("\n" + "="*80)
    print("全期間（Full Period）績效（補足年資不足的樣本）")
    print("="*80)
    for etf in ["0050.TW", "006204.TW", "VOO", "VTI", "IGSB"]:
        backtest_full_period(etf)

    print("\n[TRI 寫入總結]")
    total = sum(len(v) for v in TRI_STORE.values())
    print(f"  批次數：{len(TRI_STORE)}，總列數：{total}")
    for etf, rows in TRI_STORE.items():
        last = rows[-1]["tri_date"] if rows else None
        print(f"  {etf}: rows={len(rows)} last={last}")

if __name__ == "__main__":
    main()
