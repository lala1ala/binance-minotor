#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 radar 扫出的「低位区」信号追加写入 Google Sheet 账本。

职责边界（很重要，别越界）
  - 只写「信号行」的前 10 列：日期 / Token / 信号来源 / OI分位 / OI变化% /
    Price变化% / 方向感知 / 共振确认 / 入场价 / BTC入场价，另加 X 列「扫描确认次数」
  - **结果列（10~22：24h/48h/7d 价格与 vs BTC 超额）一律留空**，
    由 GitHub Actions 的每日回填任务（backfill_ledger.py）计算补上
  - 同一天同一个币只记第一条（当天重复触发不重复记账）；当天再次命中时
    只把 X 列「扫描确认次数」+1 —— 这一个数就是「该币当天被 2h 扫描确认了几次」
  - X 列是唯一会被更新的列，其余既有单元格一概不动

凭证：环境变量 GSHEET_CREDENTIALS（service account JSON 的完整文本）

低位区口径（与 main.py 中 scan_and_collect 的 low_zone 严格一致）：
    杠杆位（OI 30 天存量分位） <= 25%
    且 OI 24h 变化 > 1%
    且 |24h 涨跌| <= 5%
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from tradfi_filter import excluded_symbols as non_crypto_symbols

SHEET_ID = "1HBE_HXUgm7Sj0FJknDWO8hBi1OH3zNPJhWVBMiRV25w"
TAB = "自动任务信号回测"
NCOL = 24          # 23 个既有列 + X 列「扫描确认次数」
X_COL = 23         # X 列的 0-based 下标
HDR_X = "扫描确认次数"
SRC = "低位区"

FAPI = "https://fapi.binance.com"


def _fmt_pct(v):
    if v is None:
        return ""
    try:
        return f"{float(v):+.2f}%"
    except (TypeError, ValueError):
        return ""


def _fmt_price(p):
    if p is None:
        return ""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return ""
    if p >= 1000:
        return f"${p:.0f}"
    if p >= 1:
        return f"${p:.2f}"
    return f"${p:.4f}"


def _btc_price(all_metrics=None):
    """优先用本轮已采集到的 BTCUSDT 价，取不到再补一次接口"""
    for d in (all_metrics or []):
        if d.get("symbol") == "BTCUSDT" and d.get("price"):
            try:
                return float(d["price"])
            except (TypeError, ValueError):
                pass
    try:
        import requests
        r = requests.get(
            f"{FAPI}/fapi/v1/ticker/price",
            params={"symbol": "BTCUSDT"},
            timeout=15,
        )
        return float(r.json()["price"])
    except Exception:  # noqa: BLE001
        return None


def build_rows(low_zone_rows, all_metrics=None, now=None):
    """把 low_zone 的原始 dict 列表转成 23 列的表格行"""
    now = now or datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    btc_px = _btc_price(all_metrics)

    rows = []
    skip = non_crypto_symbols()
    for d in (low_zone_rows or []):
        sym = str(d.get("symbol") or "")
        if not sym.endswith("USDT"):
            continue
        # 保险：上游已按扫描池过滤，这里再挡一道，避免股票/商品混进账本
        if sym in skip:
            continue
        tok = sym[:-4]
        if not tok:
            continue

        row = [""] * NCOL
        row[0] = date_str
        row[1] = tok
        row[2] = SRC
        oi_pos = d.get("oi_pos")
        row[3] = f"{oi_pos * 100:.2f}%" if oi_pos is not None else "N/A"
        row[4] = _fmt_pct(d.get("oi_chg_1d"))
        row[5] = _fmt_pct(d.get("price_chg"))
        row[6] = "🟢低位区"
        row[7] = "否"
        row[8] = _fmt_price(d.get("price"))
        row[9] = _fmt_price(btc_px)
        row[X_COL] = "1"          # 首次确认；当天再次命中时由 write_low_zone 累加
        # row[10:23] 留空 —— 结果列由每日回填任务补
        rows.append(row)
    return date_str, rows


def _sheet():
    import gspread
    from google.oauth2.service_account import Credentials

    raw = os.environ.get("GSHEET_CREDENTIALS") or ""
    if not raw.strip():
        raise RuntimeError("GSHEET_CREDENTIALS 未设置")
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SHEET_ID).worksheet(TAB)


def _seen_index(existing):
    """既有行的 {(日期, Token): 行号(1-based)} 映射，用于当天去重与确认次数累加"""
    idx = {}
    for i, r in enumerate(existing[1:], start=2):
        if len(r) < 2:
            continue
        d0 = str(r[0])[:10]
        t0 = str(r[1]).strip().upper()
        if d0 and t0:
            idx[(d0, t0)] = i
    return idx


def _ensure_header(wks, existing):
    """确保 X 列表头存在（首次运行时写一次）"""
    head = existing[0] if existing else []
    if len(head) > X_COL and str(head[X_COL]).strip():
        return
    try:
        wks.update_cell(1, X_COL + 1, HDR_X)
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] X 列表头写入失败: {e}")


def write_low_zone(low_zone_rows, all_metrics=None):
    """追加写入低位区信号。返回 (实际写入行数, 人类可读说明)"""
    date_str, rows = build_rows(low_zone_rows, all_metrics)
    if not rows:
        return 0, "本次低位区候选为 0，未写入"

    wks = _sheet()
    existing = wks.get_all_values()
    _ensure_header(wks, existing)
    idx = _seen_index(existing)

    fresh = [r for r in rows if (r[0], r[1].upper()) not in idx]
    dup = len(rows) - len(fresh)
    if fresh:
        wks.append_rows(fresh, value_input_option="RAW")

    # 当天已存在的币：X 列「扫描确认次数」+1。
    # 这就是「同一个币一天里出现多次」的量化口径 —— 每 2h 一次扫描，命中就加一。
    bumps = []
    for r in rows:
        rownum = idx.get((r[0], r[1].upper()))
        if not rownum:
            continue
        cur = ""
        if len(existing[rownum - 1]) > X_COL:
            cur = str(existing[rownum - 1][X_COL]).strip()
        n = int(cur) if cur.isdigit() else 1
        bumps.append({"range": f"X{rownum}", "values": [[str(n + 1)]]})
    if bumps:
        try:
            wks.batch_update(bumps, value_input_option="RAW")
        except Exception as e:  # noqa: BLE001
            print(f"  [warn] 确认次数累加失败: {e}")

    detail = f"低位区 {len(rows)} 个 → 写入 {len(fresh)} 行（{date_str}）"
    if dup:
        detail += f"，{dup} 个当天已有（确认次数已累加）"
    return len(fresh), detail


if __name__ == "__main__":
    # 便于本机单独验证：python ledger_writer.py '{"symbol":"BTCUSDT","price":60000,...}'
    import sys

    payload = json.loads(sys.argv[1]) if len(sys.argv) > 1 else []
    if isinstance(payload, dict):
        payload = [payload]
    n, msg = write_low_zone(payload)
    print(f"wrote={n} :: {msg}")
