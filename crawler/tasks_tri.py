# crawler/tasks_tri.py
from __future__ import annotations
from typing import Dict, Optional, Any, List
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
from database import SessionLocal

DATE_FMT = "%Y-%m-%d"

def _normalize_records(payload: Any, key: Optional[str] = None) -> List[Dict[str, Any]]:
    """[輔助函式] 把 DB 可能回傳的 2 種樣式轉為 list[dict]：
       - [{"tri_date": "...", ...}, ...]
       - {"records": [ ... ]}
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if key and key in payload and isinstance(payload[key], list):
            return payload[key]
        if "records" in payload and isinstance(payload["records"], list):
            return payload["records"]
    return []

def _df_prices(payload)->pd.DataFrame:
    """[輔助函式] 把價格資料轉成 DataFrame，正規欄位名/型別、排序去重。"""
    recs = _normalize_records(payload)  # ← 統一解包
    if not recs:
        return pd.DataFrame(columns=["trade_date","close","adj_close"])
    df = pd.DataFrame.from_records(recs).rename(columns={"date":"trade_date"})
    if "trade_date" not in df.columns:
        # 有些實作直接用 'trade_date'，這裡保護一下
        if "date" in df.columns:
            df = df.rename(columns={"date": "trade_date"})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    # 可用欄位保護
    if "close" not in df.columns: df["close"] = np.nan
    if "adj_close" not in df.columns: df["adj_close"] = np.nan
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df = df.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    return df

def _df_divs(payload)->pd.DataFrame:
    """[輔助函式] 把股利資料轉成 DataFrame，彙總同日股利。"""
    recs = _normalize_records(payload)  # ← 統一解包
    if not recs:
        return pd.DataFrame(columns=["ex_date","dividend_per_unit"])
    df = pd.DataFrame.from_records(recs).rename(columns={"date":"ex_date"})
    if "ex_date" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "ex_date"})
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    if "dividend_per_unit" not in df.columns:
        df["dividend_per_unit"] = 0.0
    df["dividend_per_unit"] = pd.to_numeric(df["dividend_per_unit"], errors="coerce").fillna(0.0)
    return df.groupby("ex_date", as_index=False)["dividend_per_unit"].sum()

def _compute_us(df_prices: pd.DataFrame, seed_date: Optional[pd.Timestamp], seed_tri: float, base: float)->pd.DataFrame:
    """[輔助函式] 美股 TRI：用 adj_close 連乘。"""
    if df_prices.empty: 
        return pd.DataFrame(columns=["tri_date","tri"])
    df = df_prices[["trade_date","adj_close"]].dropna()
    if df.empty: 
        return pd.DataFrame(columns=["tri_date","tri"])
    df = df.sort_values("trade_date")
    df["factor"] = df["adj_close"] / df["adj_close"].shift(1)

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

def _compute_tw(df_prices: pd.DataFrame, df_divs: pd.DataFrame,
                seed_date: Optional[pd.Timestamp], seed_tri: float, base: float)->pd.DataFrame:
    """[輔助函式] 台股 TRI：(close + dividend) / prev_close 連乘。"""
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

def build_tri(etf_id: str, region: str, base: float = TRI_BASE) -> Dict:
    """
    回傳僅：
      { "etf_id": str, "last_tri_date": str|None, "tri_count_new": int }
    其他資訊一律寫到 log。
    """
    try:
        with SessionLocal.begin() as session:
            today = datetime.today().strftime(DATE_FMT)

            # 1) 從 etl_sync_status 取得 last_tri_date / tri_count
            sync_row = read_etl_sync_status(etf_id=etf_id, session=session)
            if isinstance(sync_row, list):    # 防禦（有些實作回 list）
                sync_row = sync_row[0] if sync_row else None
            if sync_row:
                sync_last_tri_date = sync_row.get("last_tri_date")
                prev_tri_count = int(sync_row.get("tri_count") or 0)
            else:
                # 第一次建 TRI（etl_sync_status 還沒有紀錄）
                sync_last_tri_date = None
                prev_tri_count = 0
                logger.info("[TRI][%s] etl_sync_status 無既有 TRI 紀錄，視為第一次建 TRI。", etf_id)
            start = sync_last_tri_date or DEFAULT_START_DATE

            # 2) 取得種子 TRI（若同日沒有就抓 <= 該日的最後一筆；都沒有則用 base）
            seed_tri = base
            seed_date = pd.to_datetime(sync_last_tri_date) if sync_last_tri_date else None

            if seed_date is not None:
                # 有紀錄就抓「<= seed_date」的最後一筆作為種子（函式只支援 start_date/end_date）
                tri_rows = _normalize_records(
                    read_tris_range(etf_id=etf_id, start_date=None, end_date=seed_date.strftime(DATE_FMT), session=session)
                )
                if tri_rows:
                    last = tri_rows[-1]  # 假設 read_tris_range 預設是升冪
                    seed_date = pd.to_datetime(last.get("tri_date") or last.get("date"))
                    seed_tri = float(last.get("tri") or last.get("value") or base)
            else:
                # 無紀錄：抓所有 TRI 的最後一筆當作種子（如果曾經算過）
                tri_rows_all = _normalize_records(
                    read_tris_range(etf_id=etf_id, start_date=None, end_date=None, session=session)
                )
                if tri_rows_all:
                    last = tri_rows_all[-1]
                    seed_date = pd.to_datetime(last.get("tri_date") or last.get("date"))
                    seed_tri = float(last.get("tri") or last.get("value") or base)

            # ★ Debug：種子資訊
            if seed_date is not None:
                logger.info("[TRI][DBG] %s 種子 TRI：date=%s tri=%.6f", etf_id, seed_date.strftime(DATE_FMT), seed_tri)
            else:
                logger.info("[TRI][DBG] %s 無既有 TRI 種子，將以 base=%.6f 開始。", etf_id, base)

            # 3) 抓資料（價格必抓；TW 才抓股利）
            df_prices = _df_prices(read_prices_range(etf_id, start_date=start, end_date=today, session=session))
            logger.info("[TRI][DBG] %s 取得價格列數=%d（區間 %s→%s）", etf_id, len(df_prices), start, today)  # ★ Debug

            if len(df_prices) <= 1:
                logger.info("[TRI][%s] 價格筆數 %d（<=1），跳過增量。region=%s", etf_id, len(df_prices), region)
                return {"etf_id": etf_id, "last_tri_date": sync_last_tri_date, "tri_count_new": prev_tri_count, "tri_added": 0}

            if region == REGION_US:
                mode = "US_ADJ_CLOSE"
                df_calc = _compute_us(df_prices, seed_date, seed_tri, base)
                df_divs = None
            else:
                mode = "TW_CLOSE_PLUS_DIV"
                df_divs = _df_divs(read_dividends_range(etf_id, start_date=start, end_date=today, session=session))
                df_calc = _compute_tw(df_prices, df_divs, seed_date, seed_tri, base)

            if df_calc.empty:
                logger.info("[TRI][%s] 計算後無新增列。region=%s mode=%s", etf_id, region, mode)
                return {"etf_id": etf_id, "last_tri_date": sync_last_tri_date, "tri_count_new": prev_tri_count, "tri_added": 0}

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
            logger.info("[TRI][DBG] %s 準備寫入 TRI：n=%d，起訖=%s → %s，mode=%s",
                    etf_id, n, output["tri_date"].iloc[0], output["tri_date"].iloc[-1], mode)
            write_etf_tris_to_db(output, session=session)

            last_tri_date_new = output["tri_date"].iloc[-1]
            tri_added = int(n)
            tri_count_new = prev_tri_count + tri_added

            logger.info(
                "[TRI][%s] 寫入 %d 筆（%s → %s），region=%s mode=%s tri_count: %d→%d",
                etf_id, tri_added, output['tri_date'].iloc[0], last_tri_date_new,
                region, mode, prev_tri_count, tri_count_new
            )

        return {"etf_id": etf_id, "last_tri_date": last_tri_date_new, "tri_count_new": tri_count_new, "tri_added": tri_added}

    except Exception as e:
        logger.exception("[TRI][%s] build_tri 發生異常: %s", etf_id, e)
        # 返回安全的預設值
        return {
            "etf_id": etf_id,
            "last_tri_date": None,
            "tri_count_new": 0,
            "tri_added": 0,
            "error": str(e)
        }
# Celery task 包裝
@app.task(name="crawler.tasks_tri.build_tri_task")
def build_tri_task(etf_id: str, region: str, base: float = TRI_BASE) -> Dict:
    return build_tri(etf_id, region, base)