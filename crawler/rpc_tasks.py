# --- 原始爬蟲／計算任務 ---
from crawler.tasks_etf_list_tw import fetch_tw_etf_list
from crawler.tasks_align import align_step0
from crawler.tasks_plan import plan_price_fetch, plan_dividend_fetch
from crawler.tasks_fetch import fetch_daily_prices, fetch_dividends
from crawler.tasks_tri import build_tri
from crawler.tasks_backtests import backtest_windows_from_tri


def rpc_fetch_tw_etf_list(crawler_url: str, region: str) -> list:
    return fetch_tw_etf_list.apply_async(
        kwargs={"crawler_url": crawler_url, "region": region},
        queue="crawler_tw"
    ).get(timeout=60)

def rpc_align_step0(region: str, src_rows: list, use_yfinance: bool = True) -> list:
    return align_step0.apply_async(
        kwargs={"region": region, "src_rows": src_rows, "use_yfinance": use_yfinance},
        queue="crawler_tw"
    ).get(timeout=60)

def rpc_plan_price_fetch(etf_id: str, inception_date: str) -> dict:
    return plan_price_fetch.apply_async(
        kwargs={"etf_id": etf_id, "inception_date": inception_date}, 
        queue="crawler_tw"
    ).get()

def rpc_plan_dividend_fetch(etf_id: str, inception_date: str) -> dict:
    return plan_dividend_fetch.apply_async(
        kwargs={"etf_id": etf_id, "inception_date": inception_date}, 
        queue="crawler_tw"
    ).get()

def rpc_async_fetch_daily_prices(etf_id: str, plan: dict):
    """注意：此處回傳 AsyncResult 物件，供後續並行收集"""
    return fetch_daily_prices.apply_async(
        kwargs={"etf_id": etf_id, "plan": plan},
        queue="crawler_tw"
    )

def rpc_async_fetch_dividends(etf_id: str, plan: dict, region: str):
    """注意：此處回傳 AsyncResult 物件，供後續並行收集"""
    return fetch_dividends.apply_async(
        kwargs={"etf_id": etf_id, "plan": plan, "region": region},
        queue="crawler_tw"
    )

def rpc_async_build_tri(etf_id: str, region: str):
    return build_tri.apply_async(
        kwargs={"etf_id": etf_id, "region": region}, 
        queue="crawler_tw"
    )

def rpc_async_backtest_windows(etf_id: str, end_date: str, windows_years: int):
    return backtest_windows_from_tri.apply_async(
        kwargs={"etf_id": etf_id, "end_date": end_date, "windows_years": windows_years},
        queue="crawler_tw"
    )