import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

LOG_DIR = Path(__file__).resolve().parent.parent / "log"
LOG_DIR.mkdir(exist_ok=True)


_loggers = {}

def get_logger(name: str = "etf_lab"):
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)

    # 設定 log 等級為 INFO（只記錄 info 以上的訊息）
    logger.setLevel(logging.INFO)

    log_file = LOG_DIR / f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log"

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    # 清除舊 handler，避免重複
    logger.handlers.clear()

    # 建立檔案 handler，最多保留 30 天，UTF-8 編碼
    file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 若需同時印出到 console，可加上 StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    _loggers[name] = logger
    return logger