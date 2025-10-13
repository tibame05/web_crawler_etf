# crawler/align_step0.py
"""
A. 名單對齊 — 依照目前實作的最小策略版
流程：
  * main_tw / main_us 呼叫
  1) 抓名單（爬蟲） → 只取 etf_id, etf_name, region, currency
  * align_step0
  2) 讀 DB 基本資料（read_etfs_id）
  3) 集合比對（寫入 log）：
     - new_ids       = crawled_ids - db_ids
     - intersect_ids = crawled_ids ∩ db_ids
     - missing_ids   = db_ids - crawled_ids
  4) 合併規則（整筆級）：同一 etf_id 若爬蟲有 → 以爬蟲整筆取代 DB；若爬蟲沒有 → 保留 DB 既有資料
  * _enrich_with_yfinance
  5) yfinance 補慢變欄位（expense_ratio, inception_date, status）
     - status 以 yfinance 結果為準（'active' 或 'delisted'）；本版本不特別將 new 標成 'new'
  * align_step0
  6) 全部 etf_ids（爬蟲 ∪ DB）寫入 etl_sync_status（僅 etf_id；其他欄位為 None/0）
  7) 批次寫回 etfs（以第 4 步合併結果為基礎，套上第 5 步 yfinance 補值）
回傳：
  final_ids: 本次要處理的清單（排除 yfinance 判定為 'delisted' 的 etf_id）
"""

from __future__ import annotations
from typing import Dict, Any, List, Set
from datetime import datetime, timezone
import yfinance as yf
import json

from crawler import logger
#from crawler.worker import app  # 僅初始化
from database.main import (
    read_etfs_id,
    write_etfs_to_db,
)


def _norm_id(x: str) -> str:
    """
    參數:
        x: 原始字串 (可能為 None)
    回傳:
        規範化後的字串 (去空白、大寫，None→"")
    """
    return (x or "").strip().upper()

def _enrich_with_yfinance(etf_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    參數:
        etf_ids(List[str]): ETF 代碼清單
    回傳:
        Dict[str, Dict[str, Any]]
        {etf_id: {"expense_ratio": float|None, "inception_date": str|None, "status": str|None}}
    """
    out: Dict[str, Dict[str, Any]] = {}

    for eid in etf_ids:
        # 1) 判斷 active / delisted
        status_val = "delisted"
        try:
            tk = yf.Ticker(eid)
            hist = tk.history(period="1mo", interval="1d")
            status_val = "active" if (hasattr(hist, "empty") and not hist.empty) else "delisted"
        except Exception as e:
            logger.warning("[STEP0] ETF %s 無法取得歷史資料，判斷為 delisted: %s", eid, e)
            tk = None

        expense = None
        inception = None

        if tk is not None:
            # 2) 合併 fast_info + get_info()（避免被 fast_info 短路）
            info = {}
            try:
                fi = getattr(tk, "fast_info", {}) or {}
                if isinstance(fi, dict):
                    info.update(fi)
            except Exception:
                pass
            try:
                gi = tk.get_info() if hasattr(tk, "get_info") else (getattr(tk, "info", {}) or {})
                if isinstance(gi, dict):
                    info.update(gi)  # 以完整資料覆蓋
            except Exception:
                pass

            # 3) 取費用率（多鍵 fallback）
            for k in ("netExpenseRatio", "annualReportExpenseRatio", "expenseRatio",
                      "trailingAnnualExpenseRatio", "fundExpenseRatio"):
                v = info.get(k)
                if v is not None:
                    try:
                        v = float(v)
                        if v > 1.0:  # 百分數轉比率
                            v /= 100.0
                        if v >= 0:
                            expense = v
                    except Exception:
                        pass
                    break

            # 4) 取成立日（多鍵 fallback + 毫秒/秒）
            raw = None
            for k in ("fundInceptionDate", "inceptionDate",
                      "firstTradeDateMilliseconds", "firstTradeDate", "firstTradeDateEpochUtc"):
                if info.get(k) is not None:
                    raw = info.get(k)
                    break

            if raw is not None:
                from datetime import datetime, timezone, date
                if isinstance(raw, (int, float)):
                    ts = float(raw)
                    if ts > 1e12:  # 毫秒 -> 秒
                        ts /= 1000.0
                    if ts > 10000:
                        try:
                            inception = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                        except Exception:
                            inception = None
                elif isinstance(raw, datetime):
                    inception = raw.date().isoformat()
                elif isinstance(raw, date):
                    inception = raw.isoformat()
                elif isinstance(raw, str):
                    # 多半已是 YYYY-MM-DD
                    inception = raw

        out[eid] = {
            "expense_ratio":  expense,
            "inception_date": inception,
            "status":         status_val,
        }

    return out


#@app.task()
def align_step0(
    *,
    region: str,
    src_rows: List[Dict[str, Any]],
    use_yfinance: bool = True,
    session=None,
) -> List[Dict[str, str]]:
    """
    參數:
        region: 市場代碼 (例如 "TW")
        src_rows: 爬蟲取得的清單 (list of dict)
        use_yfinance: 是否使用 yfinance 補 slow-changing 欄位
    回傳:
        (final_etfs_data, report)
        final_etfs_data (List[Dict[str, str]]): 
            排除 delisted 後，包含 'etf_id' 和 'inception_date' 的清單。
            格式為: [{'etf_id': '...', 'inception_date': '...'}, ...]
        report: 對齊與寫入摘要資訊（log 用）
    """
    t0 = datetime.now()
    logger.info("[%s][STEP0] 名單對齊開始 ...", region)

    # --- 讀取 DB 並判讀爬蟲 vs 資料庫比對（僅寫log，不影響後續合併邏輯） ---
    crawled_ids: Set[str] = {_norm_id(r["etf_id"]) for r in src_rows if r.get("etf_id")}
    db_rows: List[Dict[str, Any]] = read_etfs_id(region=region, session=session) or []
    db_ids: Set[str] = {_norm_id(r["etf_id"]) for r in db_rows if (isinstance(r, dict) and r.get("etf_id"))}

    new_ids       = sorted(crawled_ids - db_ids)    # 新增的etf_id
    intersect_ids = sorted(crawled_ids & db_ids)    # 在資料庫也在爬蟲的etf_id
    missing_ids   = sorted(db_ids - crawled_ids)   # 在資料庫不在爬蟲的etf_id

    logger.info("[%s][STEP0] source=%d, db=%d | new=%d, intersect=%d, missing=%d",
                region, len(crawled_ids), len(db_ids),
                len(new_ids), len(intersect_ids), len(missing_ids))

    # --- 整合 etf_rows：同一 etf_id 以「爬蟲整筆取代 DB」；爬不到則沿用 DB ---
    src_by_id = {
        _norm_id(r["etf_id"]): {
            **r,
            "etf_id": _norm_id(r["etf_id"]),
            "region": r.get("region") or region, # 爬蟲 row 若缺 region 則以函式參數 region 補上
        }
        for r in src_rows if r.get("etf_id")
    }
    db_by_id = {
        _norm_id(r["etf_id"]): {
            **r,
            "etf_id": _norm_id(r["etf_id"]),
        }
        for r in db_rows if r.get("etf_id")
    }
    etf_ids_all: Set[str] = crawled_ids | db_ids

    etf_rows_by_id: Dict[str, Dict[str, Any]] = {}
    for eid in sorted(etf_ids_all):
        if eid in src_by_id:
            # 有爬到 → 用爬蟲整筆覆蓋（不做欄位合併）
            row = dict(src_by_id[eid])
        else:
            # 沒爬到 → 照舊保留 DB 的那筆
            row = dict(db_by_id.get(eid, {}))

        # 基本欄位安全補齊（確保最終 row 一定具備 etf_id / region）
        row.setdefault("etf_id", eid)
        row.setdefault("region", region)

        etf_rows_by_id[eid] = row

    # --- yfinance：對「爬蟲 ∪ DB」的所有 etf_id 進行補值（expense_ratio / inception_date / status）---
    # 註：status 以 yfinance 為準（active / delisted）
    yf_map: Dict[str, Dict[str, Any]] = {}
    if use_yfinance:
        # 爬蟲與資料庫的id都要補
        yf_map = _enrich_with_yfinance(etf_ids_all)
        logger.info("[%s][STEP0] yfinance 完成補值（目標 %d）", region, len(etf_ids_all))
    else:
        logger.info("[%s][STEP0] 跳過 yfinance 補值（use_yfinance=False）", region)

    # --- 準備寫入 etfs：以「合併後的 row（爬蟲整筆覆蓋或 DB 沿用）」為基礎，套上 yfinance 補值 ---
    to_write: List[Dict[str, Any]] = []

    for eid in sorted(etf_rows_by_id.keys()):
        base = etf_rows_by_id[eid]  # 爬蟲 | DB 的原始資料

        yv = yf_map.get(eid, {}) if use_yfinance else {}    # yfinance 補值

        row = {
            "etf_id":         eid,
            "etf_name":       base.get("etf_name"),
            "region":         base.get("region") or region,
            "currency":       base.get("currency"),
            "expense_ratio":  yv.get("expense_ratio"),
            "inception_date": yv.get("inception_date"),
            "status":         yv.get("status"),  # 已預處理成 active / delisted
        }

        to_write.append(row)

    # --- 寫入DB: etfs 資料表 ---
    try:
        write_etfs_to_db(to_write, session=session)
        logger.info("[%s][STEP0] 已批次寫入 etfs（共 %d 筆）", region, len(to_write))
    except Exception as e:
        logger.exception("[%s][STEP0] 批次寫入 etfs 失敗：%s", region, e)

    # --- 產出 final_etfs_data：排除 yfinance 判定為 'delisted' 的 etf_id，並包含 inception_date ---
    final_ids_list: List[str] = []
    final_etfs_data: List[Dict[str, str]] = []

    for row in to_write:
        if row.get("status") != "delisted":
            final_ids_list.append(row["etf_id"])
            final_etfs_data.append({
                "etf_id": row["etf_id"],
                "inception_date": row.get("inception_date") or "", # 確保是字串，若為 None 則給空字串
            })

    # --- report 給 log 用 ---
    report = {
        "started_at": t0.isoformat(timespec="seconds"),
        "region": region,
        "source_count": len(crawled_ids),
        "db_count": len(db_ids),
        "new_count": len(new_ids),
        "intersect_count": len(intersect_ids),
        "missing_count": len(missing_ids),
        "written_count": len(to_write),
        "final_active_count": len(final_ids_list),
        "yfinance_used": bool(use_yfinance),
    }

    logger.info("[%s][STEP0] 名單對齊報告：\n%s", region, json.dumps(report, ensure_ascii=False, indent=2))

    return final_etfs_data
