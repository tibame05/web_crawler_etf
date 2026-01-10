# crawler/tasks_fetch.py
from typing import Dict, Any, List, Optional
import pandas as pd
import yfinance as yf

from crawler import logger
from database.main import write_etf_daily_price_to_db, write_etf_dividend_to_db  
from crawler.worker import app
from crawler.tasks_etf_list_tw import _get_currency_from_region
from database import SessionLocal

def _today_str() -> str:
    return pd.Timestamp.today().strftime("%Y-%m-%d")

def _to_local_calendar(index, local_tz: str) -> pd.DatetimeIndex:
    """將 DatetimeIndex 轉為在地時區的「日曆日」（00:00）。"""
    idx = pd.DatetimeIndex(index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    return idx.tz_convert(local_tz).normalize()

def _deadjust_by_future_splits(div_series: pd.Series, splits_series: pd.Series, local_tz: str) -> pd.Series:
    """
    將 yfinance 的 split-adjusted 現金股利『反調整』回事件當時口徑。
    規則：每筆配息 ×（該配息日之後所有拆分比率的乘積）。
    - yfinance.splits 的值可能是 4.0（4-for-1）或 0.25（同事件另一種表示），
      我們統一成 >=1 的放大倍數：f = r if r>=1 else 1/r。
    """
    if div_series is None or div_series.empty:
        return div_series

    # 先把配息索引轉成在地日曆日
    div_series = div_series.copy()
    div_series.index = _to_local_calendar(div_series.index, local_tz)

    if splits_series is None or splits_series.empty:
        return div_series.astype(float)

    # 拆分轉在地日曆日 + 轉為放大倍數
    ratios = pd.Series(splits_series.values, index=_to_local_calendar(splits_series.index, local_tz)).sort_index()
    ratios = ratios.apply(lambda r: float(r) if float(r) >= 1.0 else 1.0/float(r))

    # 以 searchsorted 做「之後乘積」：factor_after = total_prod / prod_up_to(ex_date)
    s_idx = ratios.index.values  # numpy datetime64[ns]
    cum = ratios.cumprod().values
    total = cum[-1]

    import numpy as np
    ex_vals = div_series.index.values
    # pos = # of splits with date <= ex_date
    pos = np.searchsorted(s_idx, ex_vals, side="right")
    prod_up_to = np.where(pos > 0, cum[pos - 1], 1.0)
    factor_after = total / prod_up_to  # 若之後無拆分 → =1

    return div_series.astype(float) * pd.Series(factor_after, index=div_series.index)

def _get_splits_series(etf_id: str, local_tz: str) -> pd.Series:
    """
    盡力取得拆分比率序列：
    1) 先用 yfinance.Ticker.splits
    2) 若為空，退回 yfinance.Ticker.actions["Stock Splits"]
    回傳 index=在地日曆日（00:00）、value=拆分倍數(>=1的放大倍數)
    """
    tkr = yf.Ticker(etf_id)

    # A) 先嘗試 tkr.splits
    splits = tkr.splits
    if splits is not None and not splits.empty:
        sidx = pd.DatetimeIndex(splits.index)
        if sidx.tz is None:
            sidx = sidx.tz_localize("UTC")
        sidx = sidx.tz_convert(local_tz).normalize()
        vals = pd.Series(splits.values, index=sidx)
        # 統一成放大倍數（4.0 留 4.0；0.25 轉 4.0）
        vals = vals.apply(lambda r: float(r) if float(r) >= 1.0 else 1.0/float(r))
        return vals.sort_index()

    # B) 退回 tkr.actions（部分市場只在這裡有）
    try:
        actions = tkr.actions  # DataFrame，含 'Dividends', 'Stock Splits'
    except Exception:
        actions = None
    if actions is not None and not actions.empty and "Stock Splits" in actions.columns:
        ss = actions["Stock Splits"].dropna()
        ss = ss[ss != 0]  # 只取不為 0 的拆分事件
        if not ss.empty:
            sidx = pd.DatetimeIndex(ss.index)
            if sidx.tz is None:
                sidx = sidx.tz_localize("UTC")
            sidx = sidx.tz_convert(local_tz).normalize()
            vals = pd.Series(ss.values, index=sidx)
            vals = vals.apply(lambda r: float(r) if float(r) >= 1.0 else 1.0/float(r))
            return vals.sort_index()

    # 真的沒有拆分
    return pd.Series(dtype=float)

@app.task(name="crawler.tasks_fetch.fetch_daily_prices")
def fetch_daily_prices(etf_id: str, plan: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    依 plan 的區間抓取 ETF 的歷史日價格（trade_date），並寫入 DB。

    參數：
        etf_id (str): ETF 代號，例如 "0050.TW"
        plan (Dict[str, Any]): 包含抓取區間的字典，應包含 "start" (str, YYYY-MM-DD)
    
    回傳：
        Optional[Dict[str, str]]: 
            如果寫入成功，回傳包含 'etf_id', 'latest_date' (str) 和 'new_records_count' (int) 的字典；
            否則回傳 None。
    """
    with SessionLocal.begin() as session:
        # 若 plan 空，表示無新資料，不需抓
        if not plan:
            logger.info("[FETCH][PRICE] %s 無需抓取（plan 空）", etf_id)
            return [] # 回傳空的 list

        # 取得開始結束時間
        start_str = plan.get("start")
        end_str = _today_str()
        logger.info("[FETCH][PRICE] %s %s → %s", etf_id, start_str, end_str)

        # 抓取歷史價格資料
        price_dataframe = yf.download(etf_id, start=start_str, end=end_str, auto_adjust=False, progress=False)

        if price_dataframe.empty or "Volume" not in price_dataframe:
            logger.info("[FETCH][PRICE] %s 無資料", etf_id)
            return []   # 若無資料則跳過該 ETF

        # 處理表頭問題（如果多層表頭）
        if isinstance(price_dataframe.columns, pd.MultiIndex):
            price_dataframe.columns = price_dataframe.columns.droplevel(1)

        # 先把 index 變成欄位，再做欄名正規化（避免 'Date' 沒轉小寫）
        price_dataframe.reset_index(inplace=True)

        # 欄名正規化（全部轉小寫、空白轉底線）
        price_dataframe.columns = (
            price_dataframe.columns.astype(str)
            .str.replace(" ", "_")
            .str.lower()
        )

        # 統一日期欄位型別並去時區（有些環境 index→欄位後仍帶 tz）
        price_dataframe["date"] = pd.to_datetime(price_dataframe["date"], errors="coerce")
        try:
            # 若有時區則去掉（tz-aware→naive）
            if getattr(price_dataframe["date"].dt, "tz", None) is not None:
                price_dataframe["date"] = price_dataframe["date"].dt.tz_convert("UTC").dt.tz_localize(None)
        except Exception:
            # 若 tz_convert 不適用（已經是 naive）就忽略
            pass

        # 去除 Volume=0，並以前值補齊
        price_dataframe = price_dataframe[price_dataframe["volume"] > 0].ffill()

        # 統一欄位格式
        output = pd.DataFrame({
            "etf_id": etf_id,
            "trade_date": price_dataframe["date"].dt.strftime("%Y-%m-%d"),
            "high": price_dataframe["high"].astype(float),
            "low": price_dataframe["low"].astype(float),
            "open": price_dataframe["open"].astype(float),
            "close": price_dataframe["close"].astype(float),
            "adj_close": price_dataframe.get("adj_close", price_dataframe["close"]).astype(float),
            "volume": price_dataframe["volume"].astype("int64"),
        })

        rows: List[Dict[str, Any]] = output.to_dict(orient="records")
        new_records_count = len(rows) # 取得新增筆數

        # 寫入 DB
        if rows:
            write_etf_daily_price_to_db(rows, session=session)
            logger.info("✅ %s 日價格已寫入 DB（%d 筆）", etf_id, len(rows))

            # 取得最後一筆資料的 'trade_date'
            latest_date = output["trade_date"].iloc[-1]
            
            # 回傳包含 etf_id、最新日期和筆數的字典
            return {
                "etf_id": etf_id, 
                "price_latest_date": latest_date,
                "price_new_records_count": new_records_count
            }    
        # 如果 rows 為空，則回傳 None
        return None

@app.task(name="crawler.tasks_fetch.fetch_dividends")
def fetch_dividends(etf_id: str, plan: Dict[str, Any], region: str) -> Optional[Dict[str, str]]:
    """
    依 plan 的區間抓取 ETF 配息資料 (ex_date)，並寫入 DB。

    參數：
        etf_id (str): ETF 代號，例如 "0050.TW"
        plan (Dict[str, Any]): 包含抓取區間的字典，應包含 "start"。
        region (str): ETF 交易地區，用於判斷幣別 (例如 'TW' 或 'US')。

    回傳：
        Optional[Dict[str, Any]]: 
            如果寫入成功，回傳包含 'etf_id', 'latest_date' (str) 和 'new_records_count' (int) 的字典；
            否則回傳 None。
    """
    with SessionLocal.begin() as session:
        # 若 plan 空，表示無新資料，不需抓
        if not plan:
            logger.info("[FETCH][DIV] %s 無需抓取（plan 空）", etf_id)
            return [] # 回傳空的 list

        # 取得開始結束時間
        start_str = plan.get("start")
        end_str = _today_str()
        logger.info("[FETCH][DIV] %s %s → %s", etf_id, start_str, end_str)

        # 判斷幣別
        currency = _get_currency_from_region(region, etf_id)
            
        # 取得 series
        dividends_series = yf.Ticker(etf_id).dividends
        if dividends_series is None or dividends_series.empty:
            logger.info("[FETCH][DIV] %s 無配息資料", etf_id)
            return []

        # 決定在地時區
        local_tz = "Asia/Taipei" if region == "TW" else "America/New_York"

        # 取原始 dividends
        tkr = yf.Ticker(etf_id)
        div_raw = tkr.dividends
        if div_raw is None or div_raw.empty:
            logger.info("[FETCH][DIV] %s 無配息資料", etf_id)
            return []

        # 取得 splits（含 fallback）
        spl_raw = _get_splits_series(etf_id, local_tz)

        # 反拆分：把 yfinance 的回溯調整「乘回去」
        div_fix = _deadjust_by_future_splits(div_raw, spl_raw, local_tz)

        # （可選）Debug：印出反調整前後的前幾筆
        try:
            dbg_before = div_raw.copy()
            dbg_before.index = _to_local_calendar(dbg_before.index, local_tz)
            logger.info("[DIV][DBG] raw head: %s", list(zip(dbg_before.index[:3].date, dbg_before.values[:3])))
            logger.info("[DIV][DBG] splits: %s", list(zip(spl_raw.index.date if len(spl_raw)>0 else [], spl_raw.values if len(spl_raw)>0 else [])))
            dbg_after = div_fix.head(3)
            logger.info("[DIV][DBG] fixed head: %s", list(zip(dbg_after.index[:3].date, dbg_after.values[:3])))
        except Exception:
            pass

        # 在地日曆日的區間篩選（注意：div_fix 的 index 已是在地日曆日）
        start_d = pd.Timestamp(start_str).tz_localize(local_tz).normalize()
        end_d   = pd.Timestamp(end_str).tz_localize(local_tz).normalize()
        div_fix = div_fix[(div_fix.index >= start_d) & (div_fix.index <= end_d)]
        if div_fix.empty:
            logger.info("[FETCH][DIV] %s 指定區間 (%s → %s) 無配息資料", etf_id, start_d.date(), end_d.date())
            return []

        # 組輸出
        div_fix = div_fix.rename_axis("ex_date")
        df = div_fix.reset_index()
        df.rename(columns={df.columns[0]: "ex_date", df.columns[1]: "dividend_per_unit"}, inplace=True)

        # 組輸出（此時 ex_date 已是「在地日曆日」，不會+1/-1）
        output = pd.DataFrame({
            "etf_id": etf_id,
            "ex_date": pd.to_datetime(df["ex_date"]).dt.strftime("%Y-%m-%d"),
            "dividend_per_unit": df["dividend_per_unit"].astype(float).round(6),
            "currency": currency,
        })

        rows: List[Dict[str, Any]] = output.to_dict(orient="records")
        if not rows:
            return []

        write_etf_dividend_to_db(rows, session=session)
        logger.info("✅ %s 配息資料已寫入 DB（%d 筆）", etf_id, len(rows))
        return {
            "etf_id": etf_id,
            "dividend_latest_date": output["ex_date"].iloc[-1],
            "dividend_new_records_count": len(rows),
        }

