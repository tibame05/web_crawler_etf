# crawler/tasks_backtest_utils_tw.py
import pandas as pd
import numpy as np
import pandas_ta as ta

from crawler.worker import app
from database.main import write_etf_daily_price_to_db, write_etf_backtest_results_to_db

# 🎯 任務 1：計算各項技術指標（RSI, MA, MACD, KD）
@app.task()
def calculate_indicators(df):
    """
    對傳入的股價資料 DataFrame 計算技術分析指標，並回傳含技術指標的 DataFrame。
    指標包含：
    - RSI（14日）
    - 移動平均線（MA5, MA20）
    - MACD（快線、慢線、柱狀圖）
    - KD 隨機指標（%K, %D）
    """
    # 基本防呆
    required_cols = {"close", "high", "low", "adj_close"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"缺少必要欄位：{required_cols - set(df.columns)}")
    
    # RSI (14) (相對強弱指標)
    df["rsi"] = ta.rsi(df["close"], length=14)

    # MA5、MA20（移動平均線）（也可以使用 df['close'].rolling(5).mean())）
    df["ma5"] = ta.sma(df["close"], length=5)
    df["ma20"] = ta.sma(df["close"], length=20)

    # MACD（移動平均收斂背離指標）
    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    df["macd_line"] = macd["MACD_12_26_9"]
    df["macd_signal"] = macd["MACDs_12_26_9"]
    df["macd_hist"] = macd["MACDh_12_26_9"]

    # KD 指標（STOCH: 隨機震盪指標）
    stoch = ta.stoch(df["high"], df["low"], df["close"], k=14, d=3, smooth_k=3)
    df["pct_k"] = stoch["STOCHk_14_3_3"]
    df["pct_d"] = stoch["STOCHd_14_3_3"]

    # 增加該日報酬率與累積報酬指數
    df['daily_return'] = df['adj_close'].pct_change()
    df['cumulative_return'] = (1 + df['daily_return']).cumprod()

    df["date"] = pd.to_datetime(df["date"])
    print(df[["adj_close", "daily_return", "cumulative_return"]].tail())

    # 刪除價格欄為 NaN 的資料（避免寫入全空 row）
    key_cols = [
        "adj_close",
        "close",
        "high",
        "low",
        "open",
        "volume",
        "daily_return",
        "cumulative_return",
    ]
    df.dropna(subset=key_cols, how="all", inplace=True)

    write_etf_daily_price_to_db(df)

    return df.to_dict(orient="records")

# 🎯 任務 2：計算策略績效評估指標
@app.task()
def evaluate_performance(df, etf_id):
    """
    根據含 Adj_Close 的股價資料，計算回測績效指標並以 dict 回傳：
    - 總報酬率（Total Return）
    - 年化報酬率（CAGR）
    - 最大回撤（Max Drawdown）
    - 夏普比率（Sharpe Ratio）
    """
            
    # 過濾無效資料
    df = df.dropna(subset=["adj_close", "daily_return", "cumulative_return"])
    if df.empty:
        print("⚠️ 有欄位但值幾乎全為 NaN")
        return

    # 基本防呆
    if df is None or df.empty or "adj_close" not in df.columns:
        return
    # 看是否有daily_return與cumulative_return
    if "daily_return" not in df.columns or "cumulative_return" not in df.columns:
        return
    # 若 cumulative_return 無有效數值，則跳過
    if df['cumulative_return'].isnull().all() or df['cumulative_return'].isna().iloc[-1]:
        return
    
    # 確保 date 欄位為 datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    # 回測期間
    backtest_start = df["date"].iloc[0].strftime("%Y-%m-%d")
    backtest_end = df["date"].iloc[-1].strftime("%Y-%m-%d")

    # 總報酬率（Total Return）
    total_return = df['cumulative_return'].iloc[-1] - 1

    # 年化報酬率（CAGR）
    days = (df["date"].iloc[-1] - df["date"].iloc[0]).days

    if days <= 0 or df['cumulative_return'].iloc[-1] <= 0:
        cagr = np.nan
    else:
        cagr = df['cumulative_return'].iloc[-1] ** (365 / days) - 1

    # 最大回撤（Max Drawdown）
    roll_max = df['cumulative_return'].cummax()
    drawdown = df['cumulative_return'] / roll_max - 1
    max_drawdown = drawdown.min()

    # 夏普比率（Sharpe Ratio）
    std_return = df['daily_return'].std()
    sharpe = np.sqrt(252) * df['daily_return'].mean() / std_return if std_return and std_return != 0 else np.nan

    # 清理暫存欄
    #df.drop(columns=["daily_return", "cumulative_return"], inplace=True)

    metrics = {
        "backtest_start": backtest_start,
        "backtest_end": backtest_end,
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe,
    }

    if metrics is not None:
        metrics["etf_id"] = etf_id

        desired_order = [
            "etf_id",
            "backtest_start",
            "backtest_end",
            "total_return",
            "cagr",
            "max_drawdown",
            "sharpe_ratio",
        ]
        df_metrics = pd.DataFrame([metrics])
        df_metrics = df_metrics[[col for col in desired_order if col in df_metrics]]
        write_etf_backtest_results_to_db(df_metrics)
        print(f"✅ {etf_id} 績效分析已儲存")
    else:
        print(f"⚠️ 無法進行績效分析：{etf_id}")

    # return metrics
