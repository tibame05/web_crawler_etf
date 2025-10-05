# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from crawler import logger
from crawler.config import REGION_US, TRI_BASE, DEFAULT_START_DATE
from database.main import (
    read_etl_sync_status,
    read_tris_range,
    read_prices_range,
    read_dividends_range,
    write_etf_tris_to_db,
)
from crawler.tasks_etf_list_tw import _get_currency_from_region
from crawler.worker import app

DATE_FMT = "%Y-%m-%d"

def _df_prices(payload)->pd.DataFrame:
    '''[輔助函式] 把 DB 回傳的 records 轉成 DataFrame、正規欄位名與型別、排序去重，股利同日彙總。'''
    recs = (payload or {}).get("records", [])
    if not recs:
        return pd.DataFrame(columns=["trade_date","close","adj_close"])
    df = pd.DataFrame.from_records(recs).rename(columns={"date":"trade_date"})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for c in ("close","adj_close"):
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
        else: df[c] = np.nan
    df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    return df

def _df_divs(payload)->pd.DataFrame:
    '''[輔助函式] 把 DB 回傳的 records 轉成 DataFrame、正規欄位名與型別、排序去重，股利同日彙總。'''
    recs = (payload or {}).get("records", [])
    if not recs:
        return pd.DataFrame(columns=["ex_date","dividend_per_unit"])
    df = pd.DataFrame.from_records(recs).rename(columns={"date":"ex_date"})
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    df["dividend_per_unit"] = pd.to_numeric(df["dividend_per_unit"], errors="coerce").fillna(0.0)
    return df.groupby("ex_date", as_index=False)["dividend_per_unit"].sum()

def _compute_us(df_prices: pd.DataFrame, seed_date: Optional[pd.Timestamp], seed_tri: float, base: float)->pd.DataFrame:
    '''[輔助函式] 計算美股 ETF 含息累積指數（TRI）。
    用 adj_close / adj_close.shift(1) 當日增益；若沒有種子（seed_date），第一天 TRI=base，之後連乘。'''
    if df_prices.empty: 
        return pd.DataFrame(columns=["tri_date","tri"])
    df = df_prices[["trade_date","adj_close"]].dropna()
    if df.empty: 
        return pd.DataFrame(columns=["tri_date","tri"])
    df["factor"] = df["adj_close"] / df["adj_close"].shift(1)

    out_dates, out_vals = [], []
    if seed_date is None:
        # 第一筆直接 base，從第二筆才用 factor
        out_dates.append(df.iloc[0]["trade_date"]); out_vals.append(base)
        cur = base
        itr = df.iloc[1:]
    else:
        cur = seed_tri
        itr = df[df["trade_date"] > seed_date]

    for _, r in itr.iterrows():
        f = r["factor"]
        if pd.isna(f) or f == 0: 
            continue
        cur *= float(f)
        out_dates.append(r["trade_date"])
        out_vals.append(cur)

    return pd.DataFrame({"tri_date": out_dates, "tri": out_vals})

def _compute_tw(df_prices: pd.DataFrame, df_divs: pd.DataFrame,
                seed_date: Optional[pd.Timestamp], seed_tri: float, base: float)->pd.DataFrame:
    '''[輔助函式] 計算台股 ETF 含息累積指數（TRI）。
    用 (close + dividend_per_unit) / prev_close 當日增益；若沒有種子（seed_date），第一天 TRI=base，之後連乘。'''
    if df_prices.empty:
        return pd.DataFrame(columns=["tri_date","tri"])
    df = df_prices[["trade_date","close"]].copy().sort_values("trade_date")
    if not df_divs.empty:
        df = df.merge(df_divs.rename(columns={"ex_date":"trade_date"}), on="trade_date", how="left")
    else:
        df["dividend_per_unit"] = 0.0
    df["dividend_per_unit"] = df["dividend_per_unit"].fillna(0.0)
    df["prev_close"] = df["close"].shift(1)
    df["factor"] = (df["close"] + df["dividend_per_unit"]) / df["prev_close"]

    out_dates, out_vals = [], []
    if seed_date is None:
        out_dates.append(df.iloc[0]["trade_date"]); out_vals.append(base)
        cur = base
        itr = df.iloc[1:]
    else:
        cur = seed_tri
        itr = df[df["trade_date"] > seed_date]

    for _, r in itr.iterrows():
        f = r["factor"]
        if pd.isna(f) or f == 0:
            continue
        cur *= float(f)
        out_dates.append(r["trade_date"])
        out_vals.append(cur)

    return pd.DataFrame({"tri_date": out_dates, "tri": out_vals})

@app.task()
def build_tri(etf_id: str, region: str, base: float = TRI_BASE) -> Dict:
    """
    回傳僅：
      { "etf_id": str, "last_tri_date": str|None, "tri_count_new": int }
    其他資訊一律寫到 log。
    """
    today = datetime.today().strftime(DATE_FMT)

    # 1) 從 etl_sync_status 取得 last_tri_date / tri_count
    sync_row = read_etl_sync_status(etf_id) or {}
    sync_last_tri_date = sync_row.get("last_tri_date")   # 可能為 None
    prev_tri_count = int(sync_row.get("tri_count") or 0)
    start = sync_last_tri_date or DEFAULT_START_DATE

    # 2) 取得種子 TRI（若同日沒有就抓 <= 該日的最後一筆；都沒有則用 base）
    seed_tri = base
    seed_date = pd.to_datetime(sync_last_tri_date) if sync_last_tri_date else None
    if seed_date is None:
        tri_last = read_tris_range(etf_id, start=None, end=None, limit=1, order="desc")
        if tri_last and tri_last.get("records"):
            sync_last_tri_date = tri_last["records"][0]["tri_date"]
            seed_tri = float(tri_last["records"][0]["tri"])
            seed_date = pd.to_datetime(sync_last_tri_date)

    # 3) 抓資料（價格必抓；TW 才抓股利）
    df_prices = _df_prices(read_prices_range(etf_id, start=start, end=today))
    if len(df_prices) <= 1:
        logger.info("[TRI][%s] 價格筆數 %d（<=1），跳過增量。region=%s", etf_id, len(df_prices), region)
        return {"etf_id": etf_id, "last_tri_date": sync_last_tri_date, "tri_count_new": prev_tri_count}

    if region == REGION_US:
        mode = "US_ADJ_CLOSE"
        df_calc = _compute_us(df_prices, seed_date, seed_tri, base)
        df_divs = None
    else:
        mode = "TW_CLOSE_PLUS_DIV"
        df_divs = _df_divs(read_dividends_range(etf_id, start=start, end=today))
        df_calc = _compute_tw(df_prices, df_divs, seed_date, seed_tri, base)

    if df_calc.empty:
        logger.info("[TRI][%s] 計算後無新增列。region=%s mode=%s", etf_id, region, mode)
        return {"etf_id": etf_id, "last_tri_date": sync_last_tri_date, "tri_count_new": prev_tri_count}

    # 4) 依你指定的「統一欄位格式」建 DataFrame 並寫入
    currency = _get_currency_from_region(region, etf_id)
    tri_dates_str = pd.to_datetime(df_calc["tri_date"]).dt.strftime(DATE_FMT)
    tri_vals = df_calc["tri"].astype(float)
    n = len(df_calc)

    output = pd.DataFrame({
        "etf_id": [etf_id] * n,
        "tri_date": tri_dates_str,
        "tri": tri_vals,
        "currency": [currency] * n,
    })[["etf_id", "tri_date", "tri", "currency"]]

    # 寫入前保險：按日排序＋同日去重（保留最後一筆）
    output = output.sort_values("tri_date").drop_duplicates(subset=["tri_date"], keep="last")
    write_etf_tris_to_db(output)

    last_tri_date_new = output["tri_date"].iloc[-1]
    tri_added = int(n)
    tri_count_new = prev_tri_count + tri_added

    logger.info(
        "[TRI][%s] 寫入 %d 筆（%s → %s），region=%s mode=%s tri_count: %d→%d",
        etf_id, tri_added, output['tri_date'].iloc[0], last_tri_date_new,
        region, mode, prev_tri_count, tri_count_new
    )

    return {"etf_id": etf_id, "last_tri_date": last_tri_date_new, "tri_count_new": tri_count_new}
