# 放在 database/__init__.py 或 database/session.py
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from utils.log import get_logger
from database.config import (
    MYSQL_ACCOUNT,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_DATABASE,
)

logger = get_logger(__name__)

engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
    pool_pre_ping=True
)

# 建立 session factory（全專案共用）
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
