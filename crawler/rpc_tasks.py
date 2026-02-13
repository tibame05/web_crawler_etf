# crawler/rpc_tasks.py
from typing import List, Dict, Any

# --- 原始爬蟲／計算任務 ---
from crawler.tasks_etf_list_tw import fetch_tw_etf_list
from crawler.tasks_etf_list_us import fetch_us_etf_list
from crawler.tasks_align import align_step0
from crawler.tasks_plan import plan_price_fetch, plan_dividend_fetch
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends
from crawler.tasks_tri import build_tri
from crawler.tasks_backtests import backtest_windows_from_tri

from crawler.config import REGION_TW, REGION_US

# =================================================================
# 內部通用底層函式 (不對外暴露)
# =================================================================

def _rpc_align_step0(region: str, src_rows: list, queue: str, use_yfinance: bool = True) -> list:
    return align_step0.apply_async(
        kwargs={"region": region, "src_rows": src_rows, "use_yfinance": use_yfinance},
        queue=queue
    ).get(timeout=60)

def _rpc_plan_price(etf_id: str, inception_date: str, queue: str) -> dict:
    return plan_price_fetch.apply_async(
        kwargs={"etf_id": etf_id, "inception_date": inception_date}, 
        queue=queue
    ).get()

def _rpc_plan_dividend(etf_id: str, inception_date: str, queue: str) -> dict:
    return plan_dividend_fetch.apply_async(
        kwargs={"etf_id": etf_id, "inception_date": inception_date}, 
        queue=queue
    ).get()

def _rpc_async_fetch_prices(etf_id: str, plan: dict, queue: str):
    return fetch_daily_prices.apply_async(
        kwargs={"etf_id": etf_id, "plan": plan},
        queue=queue
    )

def _rpc_async_fetch_dividends(etf_id: str, plan: dict, region: str, queue: str):
    return fetch_dividends.apply_async(
        kwargs={"etf_id": etf_id, "plan": plan, "region": region},
        queue=queue
    )

def _rpc_async_build_tri(etf_id: str, region: str, queue: str):
    return build_tri.apply_async(
        kwargs={"etf_id": etf_id, "region": region}, 
        queue=queue
    )

def _rpc_async_backtest(etf_id: str, end_date: str, windows_years: int, queue: str):
    return backtest_windows_from_tri.apply_async(
        kwargs={"etf_id": etf_id, "end_date": end_date, "windows_years": windows_years},
        queue=queue
    )

# =================================================================
# 台股專用介面 (TW) - 對應 queue="crawler_tw"
# =================================================================

def rpc_fetch_tw_etf_list(crawler_url: str) -> list:
    return fetch_tw_etf_list.apply_async(
        kwargs={"crawler_url": crawler_url, "region": REGION_TW},
        queue="crawler_tw"
    ).get(timeout=60)

def rpc_align_step0_tw(src_rows: list, use_yfinance: bool = True) -> list:
    return _rpc_align_step0(REGION_TW, src_rows, "crawler_tw", use_yfinance)

def rpc_plan_price_fetch_tw(etf_id: str, inception_date: str) -> dict:
    return _rpc_plan_price(etf_id, inception_date, "crawler_tw")

def rpc_plan_dividend_fetch_tw(etf_id: str, inception_date: str) -> dict:
    return _rpc_plan_dividend(etf_id, inception_date, "crawler_tw")

def rpc_async_fetch_daily_prices_tw(etf_id: str, plan: dict):
    return _rpc_async_fetch_prices(etf_id, plan, "crawler_tw")

def rpc_async_fetch_dividends_tw(etf_id: str, plan: dict):
    return _rpc_async_fetch_dividends(etf_id, plan, REGION_TW, "crawler_tw")

def rpc_async_build_tri_tw(etf_id: str):
    return _rpc_async_build_tri(etf_id, REGION_TW, "crawler_tw")

def rpc_async_backtest_windows_tw(etf_id: str, end_date: str, windows_years: int):
    return _rpc_async_backtest(etf_id, end_date, windows_years, "crawler_tw")

# =================================================================
# 美股專用介面 (US) - 對應 queue="crawler_us"
# =================================================================

def rpc_fetch_us_etf_list(crawler_url: str) -> list:
    return fetch_us_etf_list.apply_async(
        kwargs={"crawler_url": crawler_url, "region": REGION_US},
        queue="crawler_us"
    ).get(timeout=60)

def rpc_align_step0_us(src_rows: list, use_yfinance: bool = True) -> list:
    return _rpc_align_step0(REGION_US, src_rows, "crawler_us", use_yfinance)

def rpc_plan_price_fetch_us(etf_id: str, inception_date: str) -> dict:
    return _rpc_plan_price(etf_id, inception_date, "crawler_us")

def rpc_plan_dividend_fetch_us(etf_id: str, inception_date: str) -> dict:
    return _rpc_plan_dividend(etf_id, inception_date, "crawler_us")

def rpc_async_fetch_daily_prices_us(etf_id: str, plan: dict):
    return _rpc_async_fetch_prices(etf_id, plan, "crawler_us")

def rpc_async_fetch_dividends_us(etf_id: str, plan: dict):
    return _rpc_async_fetch_dividends(etf_id, plan, REGION_US, "crawler_us")

def rpc_async_build_tri_us(etf_id: str):
    return _rpc_async_build_tri(etf_id, REGION_US, "crawler_us")

def rpc_async_backtest_windows_us(etf_id: str, end_date: str, windows_years: int):
    return _rpc_async_backtest(etf_id, end_date, windows_years, "crawler_us")