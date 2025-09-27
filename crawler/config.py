from dotenv import load_dotenv
from pathlib import Path
import os

# 指定 .env 路徑
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT", 5672)
WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")

"""
crawler.config
集中放置本專案爬蟲/計算流程會用到的常數與預設值。
這裡不做任何 I/O，只提供被 import 使用的設定值。
"""

# ---- 區域/幣別常數 ----
REGION_TW: str = "TW"
REGION_US: str = "US"

# ---- 時間相關 ----
# 預設歷史資料抓取起始日（若資料庫沒有游標，就從這天開始）
DEFAULT_START_DATE: str = "2015-01-01"

# TRI 的基期值
TRI_BASE: float = 100.0

# 回測視窗（以「天」為單位）：1年、3年、10年
BACKTEST_HORIZONS_DAYS: list[int] = [365, 3 * 365, 10 * 365]

# 一些安全邊界設定（可視情況使用）
MIN_TRADING_DAYS_FOR_BACKTEST: int = 200  # 少於此天數不做回測