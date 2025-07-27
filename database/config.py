from dotenv import load_dotenv
from pathlib import Path
import os

# 指定 .env 路徑
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

MYSQL_HOST = os.getenv("MYSQL_HOST", "35.208.106.174")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_ACCOUNT = os.getenv("MYSQL_ACCOUNT", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "zuJwix-6marby-nyrbov")
