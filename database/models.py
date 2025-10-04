"""
定義 Table schema
"""
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
