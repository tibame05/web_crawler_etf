# crawler/align_step0.py
"""
A. 名單對齊 — 依照目前實作的最小策略版
流程：
  * main_tw / main_us 呼叫
  1) 抓名單（爬蟲） → 只取 etf_id, etf_name, region, currency
  * align_step0
  2) 讀 DB 基本資料（read_etfs_basic）
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
from typing import Dict, Any, List, Tuple, Set
from datetime import datetime, timezone
import yfinance as yf
import json

from crawler import logger
from crawler.worker import app  # 僅初始化
from database.main import (
    read_etfs_basic,
    clear_table, 
    write_etl_sync_status_to_db,
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
        etf_ids: ETF 代碼清單
    回傳:
        {etf_id: {"expense_ratio": float|None, "inception_date": str|None, "status": str|None}}
    """
    out: Dict[str, Dict[str, Any]] = {}
    for eid in etf_ids:
        # 處理 status
        try:
            tk = yf.Ticker(eid)
            hist = tk.history(period="1mo", interval="1d")
            if hist.empty:
                status_val = "delisted"
            else:
                status_val = "active"
        except Exception as e:
            logger.warning("[STEP0] ETF %s 無法取得歷史資料，判斷為 delisted: %s", eid, e)
            status_val = "delisted"

        # 其他欄位
        try:
            # 處理 expense_ratio 管理費
            info = getattr(tk, "fast_info", {}) or getattr(tk, "info", {}) or {}

            exp = (
                info.get("netExpenseRatio")
                or info.get("annualReportExpenseRatio")
                or info.get("expenseRatio")
            )
            if exp is not None:
                try:
                    exp = float(exp)
                    if exp > 1.0:
                        exp /= 100.0
                except:
                    exp = None

            # 處理 inception_date 成立日
            inception = info.get("inceptionDate") or info.get("firstTradeDate")
            if isinstance(inception, (int, float)) and inception > 10000:
                inception = datetime.fromtimestamp(inception, tz=timezone.utc).date().isoformat()
            elif hasattr(inception, "isoformat"):
                inception = inception.isoformat()
            else:
                inception = None

        except Exception as e:
            logger.warning("[STEP0] ETF %s 費用率／成立日補值失敗: %s", eid, e)
            exp = None
            inception = None

        out[eid] = {
            "expense_ratio": exp,
            "inception_date": inception,
            "status": status_val,
        }

    return out


@app.task()
def align_step0(
    *,
    region: str,
    src_rows: List[Dict[str, Any]],
    use_yfinance: bool = True,
) -> Tuple[List[str], Dict[str, Any]]:
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
    db_rows: List[Dict[str, Any]] = read_etfs_basic(region=region) or []
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

    # --- 僅清空 etfs 資料表（先刪後寫策略） ---
    try:
        clear_table("etfs")
        logger.info("[%s][STEP0] 已清空資料表：etfs", region)
    except Exception as e:
        logger.exception("[%s][STEP0] 清空 etfs 失敗：%s", region, e)
        raise

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
        write_etfs_to_db(to_write)
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
