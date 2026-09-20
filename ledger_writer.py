#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 radar 扫出的「低位区」信号追加写入 Google Sheet 账本。

职责边界（很重要，别越界）
  - 只写「信号行」的前 10 列：日期 / Token / 信号来源 / OI分位 / OI变化% /
    Price变化% / 方向感知 / 共振确认 / 入场价 / BTC入场价
  - **结果列（10~22：24h/48h/7d 价格与 vs BTC 超额）一律留空**，
    由本机每日 10:30 的回填任务（backfill_signals.py）计算补上
  - 只追加，**不修改任何既有行**；同一天同一个币只记第一条（当天重复触发不重复记账）

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

SHEET_ID = "1HBE_HXUgm7Sj0FJknDWO8hBi1OH3zNPJhWVBMiRV25w"
TAB = "自动任务信号回测"
NCOL = 23
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
    for d in (low_zone_rows or []):
        sym = str(d.get("symbol") or "")
        if not sym.endswith("USDT"):
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
        # row[10:] 留空 —— 结果列由每日回填任务补
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


def _seen_keys(existing):
    """既有行的 (日期, Token) 集合，用于当天去重"""
    seen = set()
    for r in existing[1:]:
        if len(r) < 2:
            continue
        d0 = str(r[0])[:10]
        t0 = str(r[1]).strip().upper()
        if d0 and t0:
            seen.add((d0, t0))
    return seen


def write_low_zone(low_zone_rows, all_metrics=None):
    """追加写入低位区信号。返回 (实际写入行数, 人类可读说明)"""
    date_str, rows = build_rows(low_zone_rows, all_metrics)
    if not rows:
        return 0, "本次低位区候选为 0，未写入"

    wks = _sheet()
    existing = wks.get_all_values()
    seen = _seen_keys(existing)

    fresh = [r for r in rows if (r[0], r[1].upper()) not in seen]
    dup = len(rows) - len(fresh)
    if not fresh:
        return 0, f"低位区 {len(rows)} 个，当天均已记账（跳过 {dup} 个）"

    wks.append_rows(fresh, value_input_option="RAW")
    detail = f"低位区 {len(rows)} 个 → 写入 {len(fresh)} 行（{date_str}）"
    if dup:
        detail += f"，{dup} 个当天已存在已跳过"
    return len(fresh), detail


if __name__ == "__main__":
    # 便于本机单独验证：python ledger_writer.py '{"symbol":"BTCUSDT","price":60000,...}'
    import sys

    payload = json.loads(sys.argv[1]) if len(sys.argv) > 1 else []
    if isinstance(payload, dict):
        payload = [payload]
    n, msg = write_low_zone(payload)
    print(f"wrote={n} :: {msg}")
