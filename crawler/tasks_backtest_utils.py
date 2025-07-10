import pandas as pd
import numpy as np
import pandas_ta as ta
import os

from crawler.worker import app

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

    df = df.copy()

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

    return df

# 🎯 任務 2：計算策略績效評估指標
@app.task()
def evaluate_performance(df):
    """
    根據含 Adj_Close 的股價資料，計算回測績效指標並以 dict 回傳：
    - 總報酬率（Total Return）
    - 年化報酬率（CAGR）
    - 最大回撤（Max Drawdown）
    - 夏普比率（Sharpe Ratio）
    """
    # 基本防呆
    if df is None or df.empty or "adj_close" not in df.columns:
        return None
    # 看是否有daily_return與cumulative_return
    if "daily_return" not in df.columns or "cumulative_return" not in df.columns:
        return None
    # 若 cumulative_return 無有效數值，則跳過
    if df['cumulative_return'].isnull().all() or df['cumulative_return'].isna().iloc[-1]:
        return None
    
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

    return {
        "backtest_start": backtest_start,
        "backtest_end": backtest_end,
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe
    }

