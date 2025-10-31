# -*- coding: utf-8 -*-
"""
讀取 debug/files/prices_all.csv 與 dividends_all.csv，
在不修改 crawler/tasks_tri.py 的前提下，計算各 ETF 的 TRI，
並將合併結果輸出到 debug/files/tri.csv。

使用方式：
    pipenv run python debug/run_tri_from_csv.py
"""
import os
import sys
import json
from datetime import datetime
import pandas as pd

# === 路徑設定 ===
FILES_DIR = os.path.join("debug", "files")
PRICES_CSV = os.path.join(FILES_DIR, "prices_all.csv")
DIVS_CSV   = os.path.join(FILES_DIR, "dividends_all.csv")
TRI_CSV    = os.path.join(FILES_DIR, "tri.csv")

# === 匯入原三件套（不動 tasks_tri 原始碼） ===
from crawler.tasks_tri import build_tri as build_tri_fn  # 主函式
import crawler.tasks_tri as tri_mod                      # 目標命名空間（猴補會補在這裡）
from crawler.config import REGION_US, TRI_BASE           # 只需 REGION_US，TW 走 else 分支

DATE_FMT = "%Y-%m-%d"

def _ensure_files():
    if not os.path.isfile(PRICES_CSV):
        print(f"[ERR] 找不到 {PRICES_CSV}，請先產生 prices_all.csv", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(DIVS_CSV):
        print(f"[WARN] 找不到 {DIVS_CSV}，將以無配息處理。")

def _load_csvs():
    prices = pd.read_csv(PRICES_CSV, dtype={"etf_id": str})
    # 正規欄名（保險起見）
    prices.columns = [c.strip().lower() for c in prices.columns]
    # 需要欄位：etf_id, trade_date, close, adj_close
    need_p = ["etf_id", "trade_date", "close", "adj_close"]
    for c in need_p:
        if c not in prices.columns:
            raise ValueError(f"[prices_all.csv] 缺少必要欄位：{c}")
    # 轉日期
    prices["trade_date"] = pd.to_datetime(prices["trade_date"], errors="coerce").dt.strftime(DATE_FMT)

    if os.path.isfile(DIVS_CSV):
        divs = pd.read_csv(DIVS_CSV, dtype={"etf_id": str})
        divs.columns = [c.strip().lower() for c in divs.columns]
        # 需要欄位：etf_id, ex_date, dividend_per_unit
        need_d = ["etf_id", "ex_date", "dividend_per_unit"]
        for c in need_d:
            if c not in divs.columns:
                raise ValueError(f"[dividends_all.csv] 缺少必要欄位：{c}")
        divs["ex_date"] = pd.to_datetime(divs["ex_date"], errors="coerce").dt.strftime(DATE_FMT)
        # 確保數值
        divs["dividend_per_unit"] = pd.to_numeric(divs["dividend_per_unit"], errors="coerce").fillna(0.0)
    else:
        # 沒有配息檔時，給空 DataFrame
        divs = pd.DataFrame(columns=["etf_id", "ex_date", "dividend_per_unit"])

    return prices, divs

# === 假 DB：由 CSV 提供 read_*，把 write_* 收集起來並最後寫 tri.csv ===
class _TriSink:
    def __init__(self):
        self.frames = []  # list[pd.DataFrame]，每次 build_tri 寫入一批

    def write(self, df: pd.DataFrame):
        # 保持與 tasks_tri 寫入欄位一致
        df = df[["etf_id", "tri_date", "tri", "currency"]].copy()
        self.frames.append(df)

    def dump_to_csv(self, path: str):
        if not self.frames:
            print("[WARN] 本次未產生任何 TRI。")
            return 0
        out = pd.concat(self.frames, ignore_index=True)
        out = out.sort_values(["etf_id", "tri_date"]).drop_duplicates(subset=["etf_id", "tri_date"], keep="last")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        out.to_csv(path, index=False, encoding="utf-8")
        print(f"[WRITE] TRI 合併輸出 → {path}（{len(out):,} 列）")
        return len(out)

def main():
    print("=" * 80)
    print("從 CSV 建 TRI（不改 tasks_tri.py）")
    print("=" * 80)

    _ensure_files()
    prices_all, divs_all = _load_csvs()

    # 依 etf_id 建索引，方便 read_* 篩選
    prices_all.set_index("etf_id", inplace=True)
    if not divs_all.empty:
        divs_all.set_index("etf_id", inplace=True)

    # 建立假的 DB 介面（猴補到 crawler.tasks_tri 命名空間）
    sink = _TriSink()

    def _fake_read_etl_sync_status(etf_id, session=None):
        # 不帶 seed：讓第一天 TRI = TRI_BASE
        return {"etf_id": etf_id, "last_tri_date": None, "tri_count": 0}

    def _fake_read_tris_range(etf_id, start=None, end=None, limit=None, order="asc", session=None):
        # 這裡不提供 seed（保持空）
        return {"records": []}

    def _fake_read_prices_range(etf_id, start=None, end=None, session=None):
        try:
            df = prices_all.loc[[etf_id]].reset_index()  # 單一 etf
        except KeyError:
            return {"records": []}
        # 篩日期
        if start:
            df = df[df["trade_date"] >= start]
        if end:
            df = df[df["trade_date"] <= end]
        # 只傳 tasks_tri 會用到的欄位名稱
        out = df[["trade_date", "close", "adj_close"]].to_dict(orient="records")
        return {"records": out}

    def _fake_read_dividends_range(etf_id, start=None, end=None, session=None):
        if divs_all.empty or etf_id not in divs_all.index:
            return {"records": []}
        df = divs_all.loc[[etf_id]].reset_index()
        if start:
            df = df[df["ex_date"] >= start]
        if end:
            df = df[df["ex_date"] <= end]
        out = df[["ex_date", "dividend_per_unit"]].rename(columns={"ex_date": "date"}).to_dict(orient="records")
        # 注意：tasks_tri._df_divs 會把 "date" 轉成 "ex_date"
        return {"records": out}

    def _fake_write_etf_tris_to_db(df, session=None):
        print(f"  [FAKE DB][TRI] 寫入 {len(df)} 列；etf={df['etf_id'].iloc[0]}")
        preview = df.head(3).to_dict(orient="records")
        print("    預覽：", json.dumps(preview, ensure_ascii=False))
        sink.write(df)

    # === 套用猴補（只覆蓋 crawler.tasks_tri 命名空間內的符號） ===
    tri_mod.read_etl_sync_status = _fake_read_etl_sync_status
    tri_mod.read_tris_range = _fake_read_tris_range
    tri_mod.read_prices_range = _fake_read_prices_range
    tri_mod.read_dividends_range = _fake_read_dividends_range
    tri_mod.write_etf_tris_to_db = _fake_write_etf_tris_to_db
    # _get_currency_from_region 沿用原本的（會自動判斷 TWD / USD），無須猴補

    # === 逐檔跑 TRI ===
    etf_ids = sorted(prices_all.index.unique().tolist())
    print(f"[INFO] 將計算 TRI 的 ETF：{etf_ids}")

    # 以資料中的最後一天作為 today（對齊 build_tri 的行為）
    # 但我們只需把 region 帶正確即可
    for etf_id in etf_ids:
        # 判斷 region：US 用 REGION_US，否則一率當 TW（因 tasks_tri 只跟 REGION_US 比較）
        region = REGION_US if not etf_id.endswith(".TW") else "TW"
        print(f"\n[TRI] etf_id={etf_id}  region={region}")
        res = build_tri_fn(etf_id=etf_id, region=region, base=TRI_BASE, session=None)
        print("[TRI 回傳]", json.dumps(res, ensure_ascii=False))

    # === 合併寫檔 ===
    os.makedirs(FILES_DIR, exist_ok=True)
    sink.dump_to_csv(TRI_CSV)

    print("\n完成。")

if __name__ == "__main__":
    main()
