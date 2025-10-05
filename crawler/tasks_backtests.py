# --- 匯入所需的函式庫 ---
from datetime import datetime, date
from dateutil.relativedelta import relativedelta  # 用於方便地進行日期加減（例如加減年份）
import pandas as pd  # 用於資料處理，特別是時間序列
import numpy as np  # 用於數值計算，例如 NaN (非數值)
from typing import Dict, Iterable, Optional, List  # 用於型別提示，增加程式碼可讀性

# --- 從專案其他模組匯入 ---
from crawler.config import BACKTEST_WINDOWS_YEARS  # 匯入預設的回測年期設定，例如 [1, 3, 10]
from crawler.worker import app  # 匯入 Celery app，用於定義背景任務
from crawler import logger  # 匯入日誌記錄器
from database.main import write_etf_backtest_results_to_db, read_tris_range  # 匯入資料庫讀寫函式

# --- 定義常數 ---
DATE_FMT = "%Y-%m-%d"  # 定義統一的日期格式字串

# ------------- 工具：把從資料庫讀取的 payload 轉成 TRI 的 pandas.Series -------------
def _records_to_tri_series(payload) -> pd.Series:
    """將資料庫回傳的 records 轉換為以時間為索引的 pandas Series"""
    # 從 payload 中取得 'records' 列表，如果 payload 為空或沒有 'records' 鍵，則返回空列表
    recs = (payload or {}).get("records", [])
    # 如果沒有任何紀錄，直接回傳一個空的浮點數型別 Series
    if not recs:
        return pd.Series(dtype=float)
    
    # 將紀錄列表轉換為 pandas DataFrame
    df = pd.DataFrame.from_records(recs)
    
    # 為了欄位名稱統一，如果 'tri_date' 不存在但 'date' 存在，則將 'date' 更名為 'tri_date'
    if "tri_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "tri_date"})
        
    # 建立一個 Series，索引是轉換為 datetime 格式的 'tri_date'，值是轉換為浮點數的 'tri'
    s = pd.Series(df["tri"].astype(float).values, index=pd.to_datetime(df["tri_date"]))
    
    # 依照時間索引排序並回傳
    return s.sort_index()

# ------------- 指標計算：內含無風險利率日化、最大回撤等，皆以 TRI 計算 -------------
def _compute_metrics_from_tri(
    tri: pd.Series,
    *,
    risk_free_rate_annual: float = 0.0,  # 年化無風險利率，預設為 0
    annualization: int = 252,  # 年化因子，通常使用一年的交易日數，預設 252
    use_calendar_years: bool = True,  # 是否使用日曆年計算 CAGR，預設是
) -> dict:
    """根據總報酬指數(TRI)序列計算各項績效指標"""
    # 移除 Series 中的 NaN 值，並確保型別為浮點數
    s = tri.dropna().astype(float)
    # 如果資料點少於 2 個，或是有任何 TRI 值小於等於 0，則無法計算，回傳 NaN
    if s.size < 2 or (s <= 0).any():
        return {
            "total_return": np.nan,
            "cagr": np.nan,
            "volatility": np.nan,
            "sharpe_ratio": np.nan,
            "max_drawdown": np.nan,
        }

    # 計算總報酬率 = (期末價值 / 期初價值) - 1
    total_return = s.iloc[-1] / s.iloc[0] - 1.0

    # 計算年化複合成長率 (CAGR)
    if use_calendar_years and isinstance(s.index, (pd.DatetimeIndex, pd.PeriodIndex)):
        # 如果使用日曆年，計算實際經過的總天數
        days = (s.index[-1] - s.index[0]).days
        # 將天數轉換為年數（考慮閏年，使用 365.25）
        years = days / 365.25 if days > 0 else np.nan
        # 計算 CAGR = (期末價值 / 期初價值)^(1/年數) - 1
        cagr = (s.iloc[-1] / s.iloc[0]) ** (1.0 / years) - 1.0 if years and years > 0 else np.nan
    else:
        # 如果不使用日曆年，則用交易日數來估算
        n = s.size - 1  # 總區間數
        # 計算 CAGR = (期末價值 / 期初價值)^(年化因子/總區間數) - 1
        cagr = (s.iloc[-1] / s.iloc[0]) ** (annualization / n) - 1.0 if n > 0 else np.nan

    # 計算每日報酬率
    r = s.pct_change().dropna()
    
    # 如果每日報酬率序列是空的，或波動為 0，則波動度和夏普比率無法正常計算
    if r.empty or r.std(ddof=1) == 0:
        volatility_ann = 0.0
        sharpe_ratio = np.nan
    else:
        # 計算年化波動度 = 每日報酬率標準差 * sqrt(年化因子)
        volatility_ann = r.std(ddof=1) * np.sqrt(annualization)
        # 將年化無風險利率轉換為每日無風險利率
        rf_daily = risk_free_rate_annual / annualization
        # 計算每日超額報酬 = 每日報酬率 - 每日無風險利率
        excess = r - rf_daily
        # 計算夏普比率 = (年化超額報酬) / 年化波動度
        sharpe_ratio = (excess.mean() * np.sqrt(annualization)) / r.std(ddof=1)

    # 計算最大回撤 (Max Drawdown, MDD)
    # 計算截至每一天的歷史最高點
    peak = s.cummax()
    # 計算每一天的回撤 = (歷史最高點 - 當天價值) / 歷史最高點
    drawdown = (peak - s) / peak
    # 找到最大的回撤值
    max_drawdown = float(drawdown.max()) if not drawdown.empty else np.nan

    # 回傳所有計算好的指標
    return {
        "total_return": float(total_return),
        "cagr": float(cagr) if pd.notna(cagr) else np.nan,
        "volatility": float(volatility_ann),
        "sharpe_ratio": float(sharpe_ratio) if pd.notna(sharpe_ratio) else np.nan,
        "max_drawdown": max_drawdown,
    }

# ------------- 主流程：嚴格年窗回測 + 一次性寫入資料庫（UPSERT: etf_id+start_date）-------------
@app.task() # 將此函式定義為一個可以非同步執行的背景任務
def backtest_windows_from_tri(
    etf_id: str,
    end_date: str,
    windows_years: Optional[Iterable[int]] = None,  # 回測的年期，預設用 config 的設定
    *,
    risk_free_rate_annual: float = 0.0,
    annualization: int = 252,
) -> Dict[str, object]:
    """
    對指定的 ETF 進行嚴格年窗回測。
    - 若 ETF 歷史資料長度不足指定年限，則該年限的計算會被**跳過**。
    - 將所有計算結果一次性寫入資料庫（使用 UPSERT 方式，避免重複）。
    - 執行狀態和樣本數等資訊只寫入 log，不存入資料庫。
    回傳：一個包含執行結果摘要的字典。
    """
    # 如果未提供 windows_years，則使用預設值
    windows_years = list(windows_years or BACKTEST_WINDOWS_YEARS)
    # 將結束日期字串轉換為 date 物件
    end_dt: date = datetime.strptime(end_date, DATE_FMT).date()

    # 初始化用於儲存結果的列表
    rows: List[Dict] = []  # 準備寫入資料庫的每一筆紀錄
    windows_done: List[str] = []  # 記錄成功完成的年期
    windows_skipped: List[str] = []  # 記錄因資料不足而跳過的年期

    # 為提高效率，一次性讀取該 ETF 從頭到尾的完整 TRI 資料
    payload_all = read_tris_range(etf_id, start=None, end=end_date)
    tri_all = _records_to_tri_series(payload_all)
    
    # 如果完全沒有 TRI 資料，則記錄日誌並直接返回
    if tri_all.empty:
        logger.info("[BACKTEST][%s] end=%s 無任何 TRI 資料，全部跳過。", etf_id, end_date)
        return {"etf_id": etf_id, "end_date": end_date, "inserted": 0, "windows_done": [], "windows_skipped": [f"{y}y" for y in windows_years]}

    # 取得實際資料的第一天和最後一天
    actual_first = tri_all.index[0].date()
    actual_last = tri_all.index[-1].date()
    
    # 如果資料的最後一天早於指定的結束日期，則以實際資料的最後一天為準
    if actual_last < end_dt:
        logger.info("[BACKTEST][%s] end=%s 但資料最後日為 %s，仍以資料最後日為基準計算。", etf_id, end_date, actual_last.strftime(DATE_FMT))
        end_dt = actual_last

    # 遍歷所有要計算的回測年期（例如 1, 3, 10 年）
    for y in windows_years:
        label = f"{y}y"  # 建立標籤，如 "1y", "3y"
        # 計算回測的目標起始日 = 結束日 - 年期
        target_start_dt = end_dt - relativedelta(years=y)

        # 嚴格視窗檢查：如果 ETF 實際的第一筆資料日期晚於目標起始日，代表歷史長度不足
        if actual_first > target_start_dt:
            logger.info("[BACKTEST][%s][%s] 年資不足（first=%s > target_start=%s），跳過寫入。", etf_id, label, actual_first.strftime(DATE_FMT), target_start_dt.strftime(DATE_FMT))
            windows_skipped.append(label)
            continue  # 繼續下一個年期的迴圈

        # 從完整的 TRI 資料中，切片出大於等於目標起始日的資料
        tri = tri_all[tri_all.index.date >= target_start_dt]
        # 如果切片後是空的，也跳過
        if tri.empty:
            logger.info("[BACKTEST][%s][%s] 視窗內無 TRI，跳過。", etf_id, label)
            windows_skipped.append(label)
            continue

        # 呼叫函式，計算這段時間窗口的績效指標
        metrics = _compute_metrics_from_tri(
            tri,
            risk_free_rate_annual=risk_free_rate_annual,
            annualization=annualization,
            use_calendar_years=True,
        )

        # 取得這個視窗實際的開始與結束日期
        win_start = tri.index[0].date()
        win_end = tri.index[-1].date()

        # 組合準備寫入資料庫的一筆紀錄
        rows.append({
            "etf_id": etf_id,
            "window_label": label,  # 視窗標籤，例如 "3y"
            "start_date": win_start.strftime(DATE_FMT),  # ✅ 主鍵之一
            "end_date": win_end.strftime(DATE_FMT),      # 僅為參考資訊
            "total_return": metrics["total_return"],
            "cagr": metrics["cagr"],
            "volatility": metrics["volatility"],
            "sharpe_ratio": metrics["sharpe_ratio"],
            "max_drawdown": metrics["max_drawdown"],
        })
        windows_done.append(label)

        # 記錄詳細的計算結果日誌
        logger.info("[BACKTEST][%s][%s] start=%s end=%s  TR=%.6f  CAGR=%.6f  VOL=%.6f  SR=%.6f  MDD=%.6f",
                      etf_id, label,
                      win_start.strftime(DATE_FMT), win_end.strftime(DATE_FMT),
                      metrics["total_return"], metrics["cagr"], metrics["volatility"],
                      metrics["sharpe_ratio"] if pd.notna(metrics["sharpe_ratio"]) else float('nan'),
                      metrics["max_drawdown"])

    # 將所有計算好的結果一次性寫入資料庫
    inserted = 0
    if rows:
        # 轉換為 DataFrame 並根據開始日期排序
        df_out = pd.DataFrame(rows).sort_values(["start_date"])
        try:
            # 嘗試使用 upsert_on 參數進行寫入，以 (etf_id, start_date) 作為唯一鍵
            write_etf_backtest_results_to_db(df_out, upsert_on=["etf_id", "start_date"])
        except TypeError:
            # 如果資料庫寫入函式不支援 upsert_on 參數，則執行普通寫入
            # (此時需要在資料庫層級設定 UNIQUE 約束來達到 upsert 效果)
            write_etf_backtest_results_to_db(df_out)
        inserted = len(df_out)

    # 記錄最終的執行摘要日誌
    logger.info("[BACKTEST][%s] end=%s 已寫入 %d 筆；完成: %s；跳過: %s",
                  etf_id, end_date, inserted, windows_done, windows_skipped)

    # 回傳執行的摘要結果
    return {"etf_id": etf_id, "end_date": end_date}
