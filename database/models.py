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

from database.config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)

# 建立連接到 MySQL 的資料庫引擎，不指定資料庫
engine_no_db = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
    connect_args={"charset": "utf8mb4"},
)

# 連線，建立 etf 資料庫（如果不存在）
with engine_no_db.connect() as conn:
    conn.execute(
        text(
            f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
    )

# 指定連到 etf 資料庫
engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
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
    Column("trade_date", Date, primary_key=True),  # 交易日
    Column("open", DECIMAL(18, 6)),  # 開盤價
    Column("high", DECIMAL(18, 6)),  # 最高價
    Column("low", DECIMAL(18, 6)),  # 最低價
    Column("close", DECIMAL(18, 6)),  # 收盤價
    Column("adj_close", DECIMAL(18, 6)),  # 調整後收盤價
    Column("volume", BIGINT),  # 成交量
)

# ETF 配息資料表
etf_dividends_table = Table(
    "etf_dividends",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("ex_date", Date, primary_key=True),  # 除息日
    Column("dividend_per_unit", DECIMAL(10, 4)),  # 每單位配發現金股利
    Column("currency", VARCHAR(10)),  # 股利幣別
)

# ETF 含息累積指數資料表
etf_tris_table = Table(
    "etf_tris",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("tri_date", Date, primary_key=True),  # TRI 日期
    Column("tri", DECIMAL(20, 8)),  # 含息累積指數
    Column("currency", VARCHAR(10)),  # 幣別
)

# ETF 回測結果資料表
etf_backtests_table = Table(
    "etf_backtests",
    metadata,
    Column("etf_id", VARCHAR(20), ForeignKey("etfs.etf_id"), primary_key=True),
    Column("start_date", Date, primary_key=True),  # 回測起始日
    Column("end_date", Date, nullable=False),  # 回測結束日
    Column("cagr", DECIMAL(8, 6)),  # 年化報酬率
    Column("sharpe_ratio", DECIMAL(8, 6)),  # 夏普比率
    Column("max_drawdown", DECIMAL(8, 6)),  # 最大回撤
    Column("total_return", DECIMAL(8, 6)),  # 總報酬率
    Column("volatility", DECIMAL(8, 6)),  # 年化波動
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
