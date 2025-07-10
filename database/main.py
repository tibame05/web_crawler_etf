import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    ForeignKey,
    Date,
    Float,
    String,
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

# 連線
conn = engine_no_db.connect()

# 建立 etf 資料庫（如果不存在）
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
etfs_table = Table(
    "etfs",
    metadata,
    Column("id", VARCHAR(20), primary_key=True),  # ETF 代碼
    Column("name", VARCHAR(100)),  # ETF 名稱
    Column("region", VARCHAR(10)),  # 地區
    Column("currency", VARCHAR(10)),  # 幣別
)

etf_daily_price_table = Table(
    "etf_daily_price",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.id"), primary_key=True),  # ETF 代碼
    Column("date", Date, primary_key=True),  # 日期
    Column("open", DECIMAL(10, 4)),  # 開盤價
    Column("high", DECIMAL(10, 4)),  # 最高價
    Column("low", DECIMAL(10, 4)),  # 最低價
    Column("close", DECIMAL(10, 4)),  # 收盤價
    Column("adj_close", DECIMAL(10, 4)),  # 調整後收盤價
    Column("volume", BIGINT),  # 成交量
)
# 如果資料表不存在，則建立它們
metadata.create_all(engine)


# 假資料
df_etfs = pd.DataFrame(
    [
        {"id": "0050", "name": "test1", "region": "TW", "currency": "TWD"},
        {"id": "0010", "name": "test2", "region": "USD", "currency": "USD"},
    ]
)

df_etf_daily_price = pd.DataFrame(
    [
        {
            "etf_id": "0050",
            "date": "2025-07-04",
            "open": 100.55,
            "high": 200.55,
            "low": 50.55,
            "close": 100.00,
            "adj_close": 230.00,
            "volume": 10000,
        },
        {
            "etf_id": "0010",
            "date": "2025-07-04",
            "open": 100.55,
            "high": 200.55,
            "low": 50.55,
            "close": 100.00,
            "adj_close": 230.00,
            "volume": 30000,
        },
    ]
)


# 遍歷 單筆寫入資料
for _, row in df_etfs.iterrows():
    # 使用 SQLAlchemy 的 insert 語句建立插入語法
    insert_etfs_stmt = insert(etfs_table).values(**row.to_dict())

    # 加上 on_duplicate_key_update 的邏輯：
    # 若主鍵重複（id 已存在），就更新其他欄位為新值
    update_etfs_stmt = insert_etfs_stmt.on_duplicate_key_update(
        **{
            col.name: insert_etfs_stmt.inserted[col.name]
            for col in etfs_table.columns
            if col.name not in ("id")
        }
    )

    # 執行 SQL 語句，寫入資料表
    with engine.begin() as conn:
        conn.execute(update_etfs_stmt)


# 批次寫入中大量資料
records_etf_daily = df_etf_daily_price.to_dict(orient="records")

insert_etf_daily_stmt = insert(etf_daily_price_table)
update_etf_daily_stmt = insert_etf_daily_stmt.on_duplicate_key_update(
    {
        col.name: insert_etf_daily_stmt.inserted[col.name]
        for col in etf_daily_price_table.columns
        if col.name not in ("etf_id", "date")
    }
)

with engine.begin() as conn:
    conn.execute(update_etf_daily_stmt, records_etf_daily)
