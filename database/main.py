import pandas as pd
from datetime import datetime, date
from typing import Optional, Any, Generator, List, Dict
from contextlib import contextmanager

from sqlalchemy.orm import Session

from sqlalchemy import Table, text
from sqlalchemy.dialects.mysql import (
    insert,  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update
)

from database import logger, SessionLocal
from database.models import (
    etfs_table,
    etf_daily_prices_table,
    etf_dividends_table,
    etf_tris_table,
    etf_backtests_table,
    etl_sync_status_table,
)


def _filter_and_replace_nan(
    records: List[Dict[str, Any]], required_fields: List[str]
) -> List[Dict[str, Any]]:
    """
    過濾資料並將 NaN 轉為 None，移除主鍵缺失的資料列。

    parameters:
        records (List[Dict[str, Any]]): 原始資料紀錄清單
        required_fields (List): 主鍵欄位，任一欄為缺失或 NaN 則該列會被移除

    returns:
        List[Dict[str, Any]]: 處理後的紀錄清單
    """

    df = pd.DataFrame(records)

    # 移除缺少主鍵的列
    df_clean = df.dropna(subset=required_fields)
    df_clean = df_clean.astype(object)  # 全欄轉 object，避免轉成 None 後又變成 NaN

    # NaN → None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    return df_clean.to_dict(orient="records")


def _upsert_records_to_db(
    records: List[Dict[str, Any]],
    table: Table,
    primary_keys: List[str],
    session: Optional[Session] = None,
):
    """
    將資料寫入資料庫，若主鍵已存在則更新該筆資料。

    parameters:
        records (List[Dict[str, Any]]): 欲寫入的資料紀錄清單
        table (Table): SQLAlchemy 定義的資料表物件
        primary_keys (List[str]): 主鍵欄位名稱，用於排除 UPSERT 更新的欄位
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    if not records:
        logger.error(f"No records to upsert for table {table.name}")
        return

    insert_stmt = insert(table)
    update_stmt = insert_stmt.on_duplicate_key_update(
        {
            col.name: insert_stmt.inserted[col.name]
            for col in table.columns
            if col.name not in primary_keys
        }
    )

    try:
        with get_session(session) as s:
            s.execute(update_stmt, records)
        logger.info(f"Upserted {len(records)} records into table {table.name}")
    except Exception as e:
        logger.error(f"Upsert to {table.name} failed: {e}", exc_info=True)


def write_etfs_to_db(records: List[Dict[str, Any]], session: Optional[Session] = None):
    """
    將 ETF 基本資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETF 基本資料紀錄，每筆資料需包含主鍵欄位 (etf_id)
            及其他對應 `etfs_table` 欄位的資料。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETF records to DB")
    _upsert_records_to_db(cleaned_records, etfs_table, primary_keys, session)


def write_etf_daily_price_to_db(
    records: List[Dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 每日價格資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETF 每日價格紀錄，每筆資料需包含主鍵欄位 (etf_id, trade_date)
            以及價格相關欄位 (open, close, high, low, volume, adj_close)。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "trade_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETF daily price records to DB")
    _upsert_records_to_db(
        cleaned_records, etf_daily_prices_table, primary_keys, session
    )


def write_etf_dividend_to_db(
    records: List[Dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 配息資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETF 配息紀錄，每筆資料需包含主鍵欄位 (etf_id, ex_date)
            及配息金額等欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "ex_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETF dividend records to DB")
    _upsert_records_to_db(cleaned_records, etf_dividends_table, primary_keys, session)


def write_etf_tris_to_db(
    records: List[Dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 含息累積指數 (TRI) 資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETF TRI 紀錄，每筆資料需包含主鍵欄位 (etf_id, tri_date)
            及 TRI 數值欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "tri_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETF TRI records to DB")
    _upsert_records_to_db(cleaned_records, etf_tris_table, primary_keys, session)


def write_etf_backtest_results_to_db(
    records: List[Dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 回測結果寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETF 回測結果記錄，每筆資料需包含主鍵欄位 (etf_id, label)
            及回測績效相關指標。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "label"]  # 更新主鍵包含 label
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETF backtest records to DB")
    _upsert_records_to_db(cleaned_records, etf_backtests_table, primary_keys, session)


def write_etl_sync_status_to_db(
    records: List[Dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETL 同步狀態寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (List[Dict[str, Any]]):
            ETL 同步狀態紀錄，每筆資料需包含主鍵欄位 (etf_id)
            及同步狀態相關欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    logger.info(f"Writing {len(cleaned_records)} ETL sync status records to DB")
    _upsert_records_to_db(cleaned_records, etl_sync_status_table, primary_keys, session)


def read_etfs_id(
    session: Optional[Session] = None, region: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    讀取所有 ETF 的 etf_id 與 region。

    parameters:
        session (Session, optional): 可傳入既有 Session，否則自動建立
        region (str, optional): 若指定，僅回傳該市場區域的 ETF

    returns:
        List[Dict[str, Any]]: ETF 基本識別資訊清單
            - etf_id (str)
            - region (str)
    """

    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, region
            FROM etfs
            WHERE status = 'ACTIVE'
        """
        if region:
            sql += " AND region = :region"
        rows = s.execute(text(sql), {"region": region} if region else {})

        for r in rows:
            records.append({"etf_id": r.etf_id, "region": r.region})

        return records


def read_etl_sync_status(
    etf_id: str, session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    讀取 ETL 同步狀態表，回傳各 ETF 的最新資料狀態。

    parameters:
        etf_id (str, optional): 若指定，僅回傳該 ETF 的同步狀態
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        List[Dict[str, Any]]: 各 ETF 的同步狀態資訊
            - etf_id (str)
            - last_price_date (str | None)
            - price_count (int)
            - last_dividend_ex_date (str | None)
            - dividend_count (int)
            - last_tri_date (str | None)
            - tri_count (int)
            - updated_at (str | None)
    """

    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, last_price_date, price_count,
                   last_dividend_ex_date, dividend_count,
                   last_tri_date, tri_count, updated_at
            FROM etl_sync_status
            WHERE etf_id = :etf_id
        """
        rows = s.execute(text(sql), {"etf_id": etf_id})

        for r in rows:
            records.append(
                {
                    "etf_id": r.etf_id,
                    "last_price_date": _to_date_str(r.last_price_date),
                    "price_count": int(r.price_count)
                    if r.price_count is not None
                    else 0,
                    "last_dividend_ex_date": _to_date_str(r.last_dividend_ex_date),
                    "dividend_count": int(r.dividend_count)
                    if r.dividend_count is not None
                    else 0,
                    "last_tri_date": _to_date_str(r.last_tri_date),
                    "tri_count": int(r.tri_count) if r.tri_count is not None else 0,
                    "updated_at": _to_datetime_str(r.updated_at),
                }
            )
        return records


def read_prices_range(
    etf_id: str, start_date: str, end_date: str, session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的每日價格資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        List[Dict[str, Any]]: ETF 每日價格紀錄
            - etf_id (str)
            - trade_date (str)
            - open, high, low, close, adj_close (float | None)
            - volume (int | None)
    """

    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, trade_date, open, high, low, close, adj_close, volume
            FROM etf_daily_prices
            WHERE etf_id = :etf_id AND trade_date BETWEEN :start AND :end
            ORDER BY trade_date ASC
        """
        rows = s.execute(
            text(sql), {"etf_id": etf_id, "start": start_date, "end": end_date}
        )

        for r in rows:
            records.append(
                {
                    "etf_id": r.etf_id,
                    "trade_date": _to_date_str(r.trade_date),
                    "open": float(r.open) if r.open is not None else None,
                    "high": float(r.high) if r.high is not None else None,
                    "low": float(r.low) if r.low is not None else None,
                    "close": float(r.close) if r.close is not None else None,
                    "adj_close": float(r.adj_close)
                    if r.adj_close is not None
                    else None,
                    "volume": int(r.volume) if r.volume is not None else None,
                }
            )

        return records


def read_dividends_range(
    etf_id: str, start_date: str, end_date: str, session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的配息資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        List[Dict[str, Any]]: ETF 配息紀錄
            - etf_id (str)
            - ex_date (str)
            - dividend_per_unit (float | None)
            - currency (str)
    """

    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, ex_date, dividend_per_unit, currency
            FROM etf_dividends
            WHERE etf_id = :etf_id
              AND ex_date BETWEEN :start AND :end
            ORDER BY ex_date ASC
        """
        rows = s.execute(
            text(sql), {"etf_id": etf_id, "start": start_date, "end": end_date}
        )

        for r in rows:
            records.append(
                {
                    "etf_id": r.etf_id,
                    "ex_date": _to_date_str(r.ex_date),
                    "dividend_per_unit": float(r.dividend_per_unit)
                    if r.dividend_per_unit is not None
                    else None,
                    "currency": r.currency,
                }
            )

        return records


def read_tris_range(
    etf_id: str, start_date: str, end_date: str, session: Optional[Session] = None
) -> List[Dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的 TRI (含息累積指數) 資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        List[Dict[str, Any]]: ETF TRI 紀錄
            - etf_id (str)
            - tri_date (str)
            - tri (float | None)
    """

    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, tri_date, tri
            FROM etf_tris
            WHERE etf_id = :etf_id AND tri_date BETWEEN :start AND :end
            ORDER BY tri_date ASC
        """
        rows = s.execute(
            text(sql), {"etf_id": etf_id, "start": start_date, "end": end_date}
        )

        for r in rows:
            records.append(
                {
                    "etf_id": r.etf_id,
                    "tri_date": _to_date_str(r.tri_date),
                    "tri": float(r.tri) if r.tri is not None else None,
                }
            )

        return records


def _to_date_str(dt: Optional[date]) -> Optional[str]:
    """
    將 `date` 物件轉換為字串 (YYYY-MM-DD 格式)。

    parameters:
        dt (date, optional): 欲轉換的日期物件，若為 None 則回傳 None

    returns:
        str | None: 轉換後的日期字串，或 None
    """

    return dt.strftime("%Y-%m-%d") if dt else None


def _to_datetime_str(dt: Optional[datetime]) -> Optional[str]:
    """
    將 `date` 物件轉換為字串 (YYYY-MM-DD 格式)。

    parameters:
        dt (date, optional): 欲轉換的日期物件，若為 None 則回傳 None

    returns:
        str | None: 轉換後的日期字串，或 None
    """

    return dt.isoformat() if dt else None


# ======================
# 共用 Session 管理
# ======================


@contextmanager
def get_session(session: Optional[Session] = None) -> Generator[Session, None, None]:
    """
    提供統一的 Session 管理機制。
    - 若呼叫端已傳入 session，直接使用。
    - 否則自動建立新的 Session，並確保正確 commit/rollback。

    parameters:
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        Generator[Session]: SQLAlchemy Session 物件
    """

    if session is not None:
        yield session
    else:
        with SessionLocal.begin() as s:
            yield s