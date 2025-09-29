from dotenv import load_dotenv
from pathlib import Path
import os

# 指定 .env 路徑
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_ACCOUNT = os.getenv("MYSQL_ACCOUNT")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

if not all([MYSQL_HOST, MYSQL_ACCOUNT, MYSQL_PASSWORD, MYSQL_DATABASE]):
    raise ValueError(
        "請確認 .env 檔案中已設定 MYSQL_HOST, MYSQL_ACCOUNT, MYSQL_PASSWORD, MYSQL_DATABASE"
    )