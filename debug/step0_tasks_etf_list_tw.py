# debug/step0_tasks_etf_list_tw.py✅
# 目的：用「真實網址」測試 crawler/tasks_etf_list_tw.fetch_tw_etf_list，
#      但避免真的寫 DB（攔截 write_etfs_to_db），並把輸入→輸出印出來，包含筆數統計。

import json
import time

# 匯入你的模組
import crawler.tasks_etf_list_tw as mod

URL = "https://tw.stock.yahoo.com/tw-etf"
REGION = "TW"
LIMIT = 0          # 0 = 不截斷（無上限，全部輸出）
SAVE_HTML = False  # 是否存原始 HTML（False = 完全不存）

# --- 包裝 requests.get：固定 UA、timeout、可選存 HTML（預設關閉） ---
_real_get = mod.requests.get
def _wrapped_get(url, *a, **k):
    k.setdefault("timeout", 15)
    resp = _real_get(url, *a, **k)

    if SAVE_HTML:
        try:
            with open("debug_step0_tw.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            print("已存檔：./debug_step0_tw.html")
        except Exception as e:
            print(f"⚠️ 存 HTML 失敗：{e}")

    time.sleep(0.3)
    return resp

mod.requests.get = _wrapped_get

# --- 攔截 DB 寫入 ---
_captured = {"rows": None}
def _fake_write_etfs_to_db(rows):
    _captured["rows"] = rows
mod.write_etfs_to_db = _fake_write_etfs_to_db

# --- 執行 ---
try:
    out = mod.fetch_tw_etf_list(crawler_url=URL, region=REGION)
except Exception as e:
    print(f"❌ 執行失敗：{e}")
    raise

# --- 筆數統計 ---
n_out = len(out) if isinstance(out, list) else 0
written = _captured.get("rows")
n_written = len(written) if isinstance(written, list) else 0

# --- 視圖（LIMIT=0 => 不截斷）
def _maybe_truncate(seq):
    if not isinstance(seq, list):
        return seq
    if isinstance(LIMIT, int) and LIMIT > 0:
        return seq[:LIMIT]
    return seq

view = _maybe_truncate(out)
written_view = _maybe_truncate(written)

# --- 輸出 ---
print("\n=== 解析後回傳 (list of dict) ===")
print(f"來源：{URL}")
print(f"區域：{REGION}")
print(f"筆數：{n_out}（LIMIT={'無上限' if not LIMIT else LIMIT}）")
print(json.dumps(view, ensure_ascii=False, indent=2))

print("\n=== 準備寫入 DB 的 payload（已攔截） ===")
print(f"筆數：{n_written}（LIMIT={'無上限' if not LIMIT else LIMIT}）")
print(json.dumps(written_view, ensure_ascii=False, indent=2))

if not out:
    print("\n⚠️ 沒抓到資料，可能：選擇器改了／被風控擋／頁面動態載入。", end="")
    if SAVE_HTML:
        print(" → 已存 ./debug_step0_tw.html 可檢查結構。")
    else:
        print(" → 目前未啟用存 HTML（SAVE_HTML=False），需要排查時再暫時打開。")
