import pandas as pd
from datetime import datetime, date
from typing import Optional, Any, Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    ForeignKey,
    Date,
    DateTime,
    INT,
    VARCHAR,
    DECIMAL,
    BIGINT,
    Enum,
    create_engine,
    text,
)
from sqlalchemy.dialects.mysql import (
    insert,  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update
)

from database.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT
from . import SessionLocal

# 建立連接到 MySQL 的資料庫引擎，不指定資料庫
engine_no_db = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
    connect_args={"charset": "utf8mb4"},
)

# ! 連線，建立 etf 資料庫（如果不存在）
# with engine_no_db.connect() as conn:
#     conn.execute(
#         text(
#             "CREATE DATABASE IF NOT EXISTS etf CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
#         )
#     )

with engine_no_db.connect() as conn:
    conn.execute(
        text(
            "CREATE DATABASE IF NOT EXISTS etf_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
    )

# ! 指定連到 etf 資料庫
# engine = create_engine(
#     f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/etf",
#     # echo=True,  # 所有 SQL 指令都印出來（debug 用）
#     pool_pre_ping=True,  # 連線前先 ping 一下，確保連線有效
# )

engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/etf_test",
    # echo=True,  # 所有 SQL 指令都印出來（debug 用）
    pool_pre_ping=True,  # 連線前先 ping 一下，確保連線有效
)

metadata = MetaData()
# ETF 基本資料表
etfs_table = Table(
    "etfs",
    metadata,
    Column("etf_id", VARCHAR(20), primary_key=True),  # ETF 代碼
    Column("etf_name", VARCHAR(100)),  # ETF 名稱
    Column("region", VARCHAR(10)),  # 市場區域（台/美）
    Column("currency", VARCHAR(10)),  # 交易幣別
    Column("expense_ratio", DECIMAL(6, 4)),  # 管理費
    Column("inception_date", Date),  # 成立日
    Column("status", Enum("ACTIVE", "DELISTED")),  # 上市狀態
)

# ETF 每日價格資料表
etf_daily_prices_table = Table(
    "etf_daily_prices",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("trade_date", Date, primary_key=True),   # 交易日
    Column("open", DECIMAL(18, 6)),                 # 開盤價
    Column("high", DECIMAL(18, 6)),                 # 最高價
    Column("low", DECIMAL(18, 6)),                  # 最低價
    Column("close", DECIMAL(18, 6)),                # 收盤價
    Column("adj_close", DECIMAL(18, 6)),            # 調整後收盤價
    Column("volume", BIGINT),                       # 成交量
)

# ETF 配息資料表
etf_dividends_table = Table(
    "etf_dividends",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("ex_date", Date, primary_key=True),      # 除息日
    Column("dividend_per_unit", DECIMAL(10, 4)),    # 每單位配發現金股利
    Column("currency", VARCHAR(10)),                # 股利幣別
)

# ETF 含息累積指數資料表
etf_tris_table = Table(
    "etf_tris",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("tri_date", Date, primary_key=True),     # TRI 日期
    Column("tri", DECIMAL(20, 8)),                  # 含息累積指數
    Column("currency", VARCHAR(10)),                # 幣別
)

# ETF 回測結果資料表
etf_backtests_table = Table(
    "etf_backtests",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("start_date", Date, primary_key=True),   # 回測起始日
    Column("end_date", Date, nullable=False),       # 回測結束日
    Column("cagr", DECIMAL(8, 6)),                  # 年化報酬率
    Column("sharpe_ratio", DECIMAL(8, 6)),          # 夏普比率
    Column("max_drawdown", DECIMAL(8, 6)),          # 最大回撤
    Column("total_return", DECIMAL(8, 6)),          # 總報酬率
    Column("volatility", DECIMAL(8, 6)),            # 年化波動
)

# ETL 同步狀態資料表
etl_sync_status_table = Table(
    "etl_sync_status",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("last_price_date", Date),  # 最後價格日期
    Column("price_count", INT),  # 價格筆數
    Column("last_dividend_ex_date", Date),  # 最後除息日
    Column("dividend_count", INT),  # 配息筆數
    Column("last_tri_date", Date),  # 最後 TRI 日期
    Column("tri_count", INT),  # TRI 筆數
    Column("updated_at", DateTime),  # 更新時間
)

# 如果資料表不存在，則建立它們
metadata.create_all(engine)


def _filter_and_replace_nan(
    records: list[dict[str, Any]], required_fields: list
) -> list[dict[str, Any]]:
    """
    過濾資料並將 NaN 轉為 None，移除主鍵缺失的資料列。

    parameters:
        records (list[dict[str, Any]]): 原始資料紀錄清單
        required_fields (list): 主鍵欄位，任一欄為缺失或 NaN 則該列會被移除

    returns:
        list[dict[str, Any]]: 處理後的紀錄清單
    """

    df = pd.DataFrame(records)

    # 移除缺少主鍵的列
    df_clean = df.dropna(subset=required_fields)
    df_clean = df_clean.astype(object)  # 全欄轉 object，避免轉成 None 後又變成 NaN

    # NaN → None
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    return df_clean.to_dict(orient="records")


def _upsert_records_to_db(
    records: list[dict[str, Any]],
    table: Table,
    primary_keys: list[str],
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
        return

    insert_stmt = insert(table)
    update_stmt = insert_stmt.on_duplicate_key_update(
        {
            col.name: insert_stmt.inserted[col.name]
            for col in table.columns
            if col.name not in primary_keys
        }
    )

    with get_session(session) as s:
        s.execute(update_stmt, records)


def write_etfs_to_db(records: list[dict[str, Any]], session: Optional[Session] = None):
    """
    將 ETF 基本資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETF 基本資料紀錄，每筆資料需包含主鍵欄位 (etf_id)
            及其他對應 `etfs_table` 欄位的資料。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etfs_table, primary_keys, session)


def write_etf_daily_price_to_db(
    records: list[dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 每日價格資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETF 每日價格紀錄，每筆資料需包含主鍵欄位 (etf_id, trade_date)
            以及價格相關欄位 (open, close, high, low, volume, adj_close)。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "trade_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etf_daily_prices_table, primary_keys, session)


def write_etf_dividend_to_db(
    records: list[dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 配息資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETF 配息紀錄，每筆資料需包含主鍵欄位 (etf_id, ex_date)
            及配息金額等欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "ex_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etf_dividends_table, primary_keys, session)


def write_etf_tris_to_db(
    records: list[dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 含息累積指數 (TRI) 資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETF TRI 紀錄，每筆資料需包含主鍵欄位 (etf_id, tri_date)
            及 TRI 數值欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "tri_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etf_tris_table, primary_keys, session)


def write_etf_backtest_results_to_db(
    records: list[dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETF 回測結果寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETF 回測結果紀錄，每筆資料需包含主鍵欄位 (etf_id, start_date)
            及回測績效相關指標。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id", "start_date"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etf_backtests_table, primary_keys, session)


def write_etl_sync_status_to_db(
    records: list[dict[str, Any]], session: Optional[Session] = None
):
    """
    將 ETL 同步狀態寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        records (list[dict[str, Any]]):
            ETL 同步狀態紀錄，每筆資料需包含主鍵欄位 (etf_id)
            及同步狀態相關欄位。
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        None
    """

    primary_keys = ["etf_id"]
    cleaned_records = _filter_and_replace_nan(records, primary_keys)
    _upsert_records_to_db(cleaned_records, etl_sync_status_table, primary_keys, session)





# ======================
# A. STEP1 名單對齊相關
# ======================


def read_etfs_id(
    session: Optional[Session] = None, region: Optional[str] = None
) -> list[dict[str, Any]]:
    """
    讀取所有 ETF 的 etf_id 與 region。

    parameters:
        session (Session, optional): 可傳入既有 Session，否則自動建立
        region (str, optional): 若指定，僅回傳該市場區域的 ETF

    returns:
        list[dict[str, Any]]: ETF 基本識別資訊清單
            - etf_id (str)
            - region (str)
    """
    
    records = []
    with get_session(session) as s:
        sql = """
            SELECT etf_id, region
            FROM etfs
        """
        if region:
            sql += " WHERE region = :region"
        rows = s.execute(text(sql), {"region": region} if region else {})

        for r in rows:
            records.append({"etf_id": r.etf_id, "region": r.region})

        return records


# ======================
# B. STEP1 規劃／抓取
# ======================


def read_etl_sync_status(session: Optional[Session] = None) -> list[dict[str, Any]]:
    """
    讀取 ETL 同步狀態表，回傳各 ETF 的最新資料狀態。

    parameters:
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        list[dict[str, Any]]: 各 ETF 的同步狀態資訊
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
        """
        rows = s.execute(text(sql))

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


# ======================
# C. STEP2 建 TRI
# ======================


def read_prices_range(
    etf_id: str, start_date: str, end_date: str, session: Optional[Session] = None
) -> list[dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的每日價格資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        list[dict[str, Any]]: ETF 每日價格紀錄
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
) -> list[dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的配息資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        list[dict[str, Any]]: ETF 配息紀錄
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


# ======================
# D. 回測
# ======================


def read_tris_range(
    etf_id: str, start_date: str, end_date: str, session: Optional[Session] = None
) -> list[dict[str, Any]]:
    """
    讀取指定 ETF 在區間內的 TRI (含息累積指數) 資料。

    parameters:
        etf_id (str): ETF 代碼
        start_date (str): 起始日期 (YYYY-MM-DD)
        end_date (str): 結束日期 (YYYY-MM-DD)
        session (Session, optional): 可傳入既有 Session，否則自動建立

    returns:
        list[dict[str, Any]]: ETF TRI 紀錄
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