import pandas as pd
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
    create_engine,
    text,
)
from sqlalchemy.dialects.mysql import (
    insert,  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update
)

from database.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT

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
    Column("etf_id", VARCHAR(20), primary_key=True),    # ETF 代碼
    Column("etf_name", VARCHAR(100)),                   # ETF 名稱
    Column("region", VARCHAR(10)),                      # 市場區域（台/美）
    Column("currency", VARCHAR(10)),                    # 交易幣別
    Column("expense_ratio", DECIMAL(6, 4)),             # 管理費
    Column("inception_date", Date),                     # 成立日
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
    Column("last_price_date", Date),                # 最後價格日期
    Column("price_count", INT),                     # 價格筆數
    Column("last_dividend_ex_date", Date),          # 最後除息日
    Column("dividend_count", INT),                  # 配息筆數
    Column("updated_at", DateTime),                 # 更新時間
)

# 如果資料表不存在，則建立它們
metadata.create_all(engine)


def filter_and_replace_nan(df: pd.DataFrame, required_fields: list) -> pd.DataFrame:
    """
    將 DataFrame 中的 NaN 轉為 None，並過濾掉主鍵缺失的資料列。

    parameters:
        df (pd.DataFrame): 原始資料表格
        required_fields (list): 主鍵欄位，任一欄為 NaN 則該列會被移除

    returns:
        pd.DataFrame: 已處理完成的資料表格，NaN 轉為 None、主鍵欄位無缺漏
    """

    df_clean = df.dropna(subset=required_fields)
    df_clean = df_clean.astype(object)  # 全欄轉 object，避免轉成 None 後又變成 NaN
    return df_clean.where(pd.notnull(df_clean), None)


def upsert_dataframe_to_db(df: pd.DataFrame, table: Table, primary_keys: list):
    """
    將 DataFrame 資料寫入資料庫，若主鍵已存在則更新該筆資料。

    parameters:
        df (pd.DataFrame): 欲寫入的資料
        table (Table): SQLAlchemy 定義的資料表物件
        primary_keys (list): 主鍵欄位名稱，用於排除 UPSERT 更新的欄位

    returns:
        None
    """
    records = df.to_dict(orient="records")
    insert_stmt = insert(table)
    update_stmt = insert_stmt.on_duplicate_key_update(
        {
            col.name: insert_stmt.inserted[col.name]
            for col in table.columns
            if col.name not in primary_keys
        }
    )

    with engine.begin() as conn:
        conn.execute(update_stmt, records)


def write_etfs_to_db(etfs_df: pd.DataFrame):
    """
    將 ETF 基本資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etfs_df (pd.DataFrame):
            ETF 基本資料表格。每列資料應對應資料表 `etfs_table` 的欄位結構，並包含主鍵欄位（如 id）。

    returns:
        None
    """

    primary_keys = ["etf_id"]

    etfs_df = filter_and_replace_nan(etfs_df, primary_keys)

    upsert_dataframe_to_db(etfs_df, etfs_table, primary_keys)


def write_etf_daily_price_to_db(etf_daily_price_df: pd.DataFrame):
    """
    將 ETF 每日價格資料寫入資料庫，若主鍵已存在則執行更新。

    parameters:
        etf_daily_price_df (pd.DataFrame):
            ETF 每日價格資料。每筆資料需包含主鍵欄位（etf_id, date）與其他價格欄位，如 open、close、volume 等。

    returns:
        None
    """

    primary_keys = ["etf_id", "trade_date"]

    etf_daily_price_df = filter_and_replace_nan(etf_daily_price_df, primary_keys)

    upsert_dataframe_to_db(etf_daily_price_df, etf_daily_prices_table, primary_keys)


def write_etf_dividend_to_db(etf_dividend_df: pd.DataFrame):
    """
    將 ETF 配息資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etf_dividend_df (pd.DataFrame):
            ETF 配息資料。每筆資料應包含主鍵欄位（etf_id, date）與配息金額等其他欄位。

    returns:
        None
    """

    primary_keys = ["etf_id", "ex_date"]

    etf_dividend_df = filter_and_replace_nan(etf_dividend_df, primary_keys)

    upsert_dataframe_to_db(etf_dividend_df, etf_dividends_table, primary_keys)


def write_etf_tris_to_db(etf_tris_df: pd.DataFrame):
    """
    將 ETF 含息累積指數資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etf_tris_df (pd.DataFrame):
            ETF 含息累積指數資料。每筆資料應包含主鍵欄位（etf_id, tri_date）與含息累積指數等其他欄位。

    returns:
        None
    """

    primary_keys = ["etf_id", "tri_date"]

    etf_tris_df = filter_and_replace_nan(etf_tris_df, primary_keys)

    upsert_dataframe_to_db(etf_tris_df, etf_tris_table, primary_keys)


def write_etf_backtest_results_to_db(etf_backtest_df: pd.DataFrame):
    """
    將 ETF 回測結果寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etf_backtest_df (pd.DataFrame):
            ETF 回測結果資料。每一列應包含主鍵欄位（如 etf_id）及其他欲寫入或更新的欄位。
            欄位名稱需對應資料表 `etf_backtest_results` 的欄位定義。

    returns:
        None
    """

    primary_keys = ["etf_id", "start_date"]

    etf_backtest_df = filter_and_replace_nan(etf_backtest_df, primary_keys)

    upsert_dataframe_to_db(etf_backtest_df, etf_backtests_table, primary_keys)


def write_etl_sync_status_to_db(etl_sync_status_df: pd.DataFrame):
    """
    將 ETL 同步狀態資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etl_sync_status_df (pd.DataFrame):
            ETL 同步狀態資料。每一列應包含主鍵欄位（如 etf_id）及其他欲寫入或更新的欄位。
            欄位名稱需對應資料表 `etl_sync_status` 的欄位定義。

    returns:
        None
    """

    primary_keys = ["etf_id"]

    etl_sync_status_df = filter_and_replace_nan(etl_sync_status_df, primary_keys)

    upsert_dataframe_to_db(etl_sync_status_df, etl_sync_status_table, primary_keys)
