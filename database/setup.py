"""
setup.py 初始化建庫＋建表
執行方式：python database/setup.py
"""
from sqlalchemy import create_engine, text
from database.config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)
from database.models import metadata

# 建庫
engine_no_db = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
    connect_args={"charset": "utf8mb4"},
)
with engine_no_db.connect() as conn:
    conn.execute(
        text(
            f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
    )
print(f"Database '{MYSQL_DATABASE}' created or already exists.")

# 建表
engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
    pool_pre_ping=True,
)
metadata.create_all(engine)
print("All tables created.")
