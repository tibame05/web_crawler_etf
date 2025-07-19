import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    ForeignKey,
    Date,
    Float,
    VARCHAR,
    DECIMAL,
    BIGINT,
    create_engine,
    text,
)
from sqlalchemy.dialects.mysql import (
    insert  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update
)

from database.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT

# 建立連接到 MySQL 的資料庫引擎，不指定資料庫
engine_no_db = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
    connect_args={"charset": "utf8mb4"},
)

# 連線，建立 etf 資料庫（如果不存在）
with engine_no_db.connect() as conn:
    conn.execute(
        text(
            "CREATE DATABASE IF NOT EXISTS etf CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
    )

# 指定連到 etf 資料庫
engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/etf",
    # echo=True,  # 所有 SQL 指令都印出來（debug 用）
    pool_pre_ping=True,  # 連線前先 ping 一下，確保連線有效
)

# 建立 etfs、etf_daily_price 資料表（如果不存在）
metadata = MetaData()
# ETF 基本資料表
etfs_table = Table(
    "etfs_us",
    metadata,
    Column("etf_id", VARCHAR(20), primary_key=True),  # ETF 代碼
    Column("etf_name", VARCHAR(100)),  # ETF 名稱
    Column("region", VARCHAR(10)),  # 地區
    Column("currency", VARCHAR(10)),  # 幣別
)

# ETF 每日價格資料表
etf_daily_price_table = Table(
    "etf_daily_price_us",
    metadata,
    Column(
        "etf_id", VARCHAR(20), ForeignKey("etfs_us.etf_id"), primary_key=True
    ),  # ETF 代碼
    Column("date", Date, primary_key=True),  # 日期
    Column("adj_close", DECIMAL(10, 4)),  # 調整後收盤價
    Column("close", DECIMAL(10, 4)),  # 收盤價
    Column("high", DECIMAL(10, 4)),  # 最高價
    Column("low", DECIMAL(10, 4)),  # 最低價
    Column("open", DECIMAL(10, 4)),  # 開盤價
    Column("volume", BIGINT),  # 成交量
    Column("rsi", Float),  # 相對強弱指標
    Column("ma5", Float),  # 5 日移動平均
    Column("ma20", Float),  # 20 日移動平均
    Column("macd_line", Float),  # MACD 線
    Column("macd_signal", Float),  # MACD 信號線
    Column("macd_hist", Float),  # MACD 柱狀圖
    Column("pct_k", Float),  # 隨機指標 K 值
    Column("pct_d", Float),  # 隨機指標 D 值
    Column("daily_return", DECIMAL(8, 6)),  # 日報酬率
    Column("cumulative_return", DECIMAL(10, 6)),  # 累積報酬率
)

# ETF 配息資料表
etf_dividend_table = Table(
    "etf_dividend_us",
    metadata,
    Column(
        "etf_id", VARCHAR(20), ForeignKey("etfs_us.etf_id"), primary_key=True
    ),  # ETF 代碼
    Column("date", Date, primary_key=True),  # 除息日
    Column("dividend_per_unit", DECIMAL(10, 4)),  # 每單位配息金額
    Column("currency", VARCHAR(10)),  # 幣別
)

# ETF 回測結果資料表
etf_backtest_results_table = Table(
    "etf_backtest_results_us",
    metadata,
    Column(
        "etf_id", VARCHAR(20), ForeignKey("etfs_us.etf_id"), primary_key=True
    ),  # ETF 代碼
    Column("backtest_start", Date, nullable=False),  # 回測開始日期
    Column("backtest_end", Date, nullable=False),  # 回測結束日期
    Column("total_return", DECIMAL(8, 6)),  # 總報酬率
    Column("cagr", DECIMAL(8, 6)),  # 年化報酬率
    Column("max_drawdown", DECIMAL(8, 6)),  # 最大回撤
    Column("sharpe_ratio", DECIMAL(8, 6)),  # 夏普比率
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


def write_etf_list_us_db(etf_list_us: pd.DataFrame):
    """
    將 ETF 基本資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etfs_df (pd.DataFrame):
            ETF 基本資料表格。每列資料應對應資料表 `etfs_table` 的欄位結構，並包含主鍵欄位（如 id）。

    returns:
        None
    """

    primary_keys = ["etf_id"]

    etfs_df = filter_and_replace_nan(etf_list_us, primary_keys)

    upsert_dataframe_to_db(etfs_df, etfs_table, primary_keys)


def write_crawler_etf_us_to_db(crawler_etf_us: pd.DataFrame):
    """
    將 ETF 每日價格資料寫入資料庫，若主鍵已存在則執行更新。

    parameters:
        etf_daily_price_df (pd.DataFrame):
            ETF 每日價格資料。每筆資料需包含主鍵欄位（etf_id, date）與其他價格欄位，如 open、close、volume 等。

    returns:
        None
    """

    primary_keys = ["etf_id", "date"]

    etf_daily_price_df = filter_and_replace_nan(crawler_etf_us, primary_keys)

    upsert_dataframe_to_db(etf_daily_price_df, etf_daily_price_table, primary_keys)


def write_backtest_utils_us_to_db(backtest_utils_us: pd.DataFrame):
    """
    將 ETF 回測結果寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etf_backtest_df (pd.DataFrame):
            ETF 回測結果資料。每一列應包含主鍵欄位（如 etf_id）及其他欲寫入或更新的欄位。
            欄位名稱需對應資料表 `etf_backtest_results` 的欄位定義。

    returns:
        None
    """

    primary_keys = ["etf_id"]

    etf_backtest_df = filter_and_replace_nan(backtest_utils_us, primary_keys)

    upsert_dataframe_to_db(etf_backtest_df, etf_backtest_results_table, primary_keys)



def write_crawler_etf_dps_us_to_db(crawler_etf_dps_us: pd.DataFrame):
    """
    將 ETF 配息資料寫入資料庫，若主鍵已存在則更新資料。

    parameters:
        etf_dividend_df (pd.DataFrame):
            ETF 配息資料。每筆資料應包含主鍵欄位（etf_id, date）與配息金額等其他欄位。

    returns:
        None
    """

    primary_keys = ["etf_id", "date"]

    etf_dividend_df = filter_and_replace_nan(crawler_etf_dps_us, primary_keys)

    upsert_dataframe_to_db(etf_dividend_df, etf_dividend_table, primary_keys)
