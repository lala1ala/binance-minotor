#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""①③ 两个新信号的落点：Google Sheet 里一张独立的观察表。

为什么单独一个 tab、单独一个文件
  · 不碰「自动任务信号回测」那张账本 —— 它有 26 列固定结构，
    列号被 ledger_writer.py 与 backfill_ledger.py 两边硬编码，
    多写一列就会静默错位（09-23 改列时已经吃过这个亏）。
  · 这张表只服务两件事：
        ① 负费率持续性 + 低位  → 每轮记录（不推送）
        ③ 小市值爆空扫描        → 首次命中记录 + 推送
  · 读它还能做 ③ 的去重：同一个币历史上出现过就不再推。

表结构（12 列，0-based 见 COL_* 常量；改列必须同步三个函数）
    0 日期 | 1 类型 | 2 Token | 3 价位分位% | 4 DD% |
    5 连续负费率h | 6 累积费率% | 7 当前费率% |
    8 OI名义额$ | 9 换手率 | 10 上市天数 | 11 备注

凭证：环境变量 GSHEET_CREDENTIALS（service account JSON 完整文本）
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

SHEET_ID = "1HBE_HXUgm7Sj0FJknDWO8hBi1OH3zNPJhWVBMiRV25w"
TAB = "爆空与负费率观察"

HEADERS = [
    "日期", "类型", "Token", "价位分位%", "DD%",
    "连续负费率h", "累积费率%", "当前费率%",
    "OI名义额$", "换手率", "上市天数", "备注",
]
NCOL = len(HEADERS)

TYPE_FUNDING = "负费率低位"
TYPE_SQUEEZE = "爆空候选"

_changed = []


def _get_sheet():
    """按需取 worksheet；没有就建一个并写表头。"""
    import gspread
    from google.oauth2.service_account import Credentials

    raw = os.environ.get("GSHEET_CREDENTIALS") or ""
    if not raw.strip():
        raise RuntimeError("GSHEET_CREDENTIALS 未设置")

    creds = Credentials.from_service_account_info(
        json.loads(raw), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sh = gspread.authorize(creds).open_by_key(SHEET_ID)
    try:
        return sh.worksheet(TAB)
    except gspread.exceptions.WorksheetNotFound:
        wks = sh.add_worksheet(title=TAB, rows=2000, cols=NCOL)
        wks.append_row(HEADERS, value_input_option="RAW")
        return wks


def read_rows():
    """返回 [(行号1-based, 行list)]，不含表头。"""
    wks = _get_sheet()
    vals = wks.get_all_values()
    out = []
    for i, r in enumerate(vals[1:], start=2):
        if not any(str(c).strip() for c in r):
            continue
        out.append((i, r))
    return out


def seen_tokens(kind=TYPE_SQUEEZE):
    """历史上出现过的 Token 集合（用于 ③ 只推首次）。"""
    got = set()
    for _, r in read_rows():
        if len(r) > 2 and str(r[1]).strip() == kind:
            t = str(r[2]).strip().upper()
            if t:
                got.add(t)
    return got


def funding_first_seen():
    """「负费率低位」里每个 Token 最早出现的日期（UTC，YYYY-MM-DD）。

    日报用它标 🆕：表里没有过 → 新进；最早日期 == 今天 → 今天新进。
    ⚠️ 必须在 upsert 之前调用，否则本轮刚写进去的行会让所有币都算「今天新进」。
    """
    out = {}
    for _, r in read_rows():
        if len(r) <= 2:
            continue
        if str(r[1]).strip() != TYPE_FUNDING:
            continue
        t = str(r[2]).strip().upper()
        d = str(r[0])[:10]
        if not t or not d:
            continue
        if t not in out or d < out[t]:
            out[t] = d
    return out


def upsert(rows):
    """按 (日期, 类型, Token) 去重：已存在就整行覆盖，否则追加。

    返回 (追加行数, 更新行数)。
    """
    rows = [r for r in rows if r]
    if not rows:
        return 0, 0

    wks = _get_sheet()
    vals = wks.get_all_values()
    idx = {}
    for i, r in enumerate(vals[1:], start=2):
        if len(r) >= 3:
            key = (str(r[0])[:10], str(r[1]).strip(), str(r[2]).strip().upper())
            idx[key] = i

    fresh, updates = [], []
    for r in rows:
        key = (str(r[0])[:10], str(r[1]).strip(), str(r[2]).strip().upper())
        rownum = idx.get(key)
        if rownum:
            updates.append({"range": f"A{rownum}:L{rownum}", "values": [r]})
        else:
            fresh.append(r)

    if fresh:
        wks.append_rows(fresh, value_input_option="RAW")
    if updates:
        wks.batch_update(updates, value_input_option="RAW")
    return len(fresh), len(updates)


def today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---- 行构造函数（两个信号共用，避免列序写两遍）----
def _num(v, nd=1):
    try:
        return round(float(v), nd)
    except (TypeError, ValueError):
        return ""


def funding_row(token, pos, dd, neg_hours, cum_rate, cur_rate, note=""):
    r = [""] * NCOL
    r[0] = today()
    r[1] = TYPE_FUNDING
    r[2] = token
    r[3] = _num(pos * 100, 1) if pos is not None else ""
    r[4] = _num(dd, 1) if dd is not None else ""
    r[5] = _num(neg_hours, 0)
    r[6] = _num(cum_rate, 3)
    r[7] = _num(cur_rate, 4)
    r[8] = ""
    r[9] = ""
    r[10] = ""
    r[11] = note
    return r


def squeeze_row(token, pos, dd, cur_rate, oi_usd, turnover, listed_days, note=""):
    r = [""] * NCOL
    r[0] = today()
    r[1] = TYPE_SQUEEZE
    r[2] = token
    r[3] = _num(pos * 100, 1) if pos is not None else ""
    r[4] = _num(dd, 1) if dd is not None else ""
    r[5] = ""
    r[6] = ""
    r[7] = _num(cur_rate, 4)
    r[8] = int(round(oi_usd)) if oi_usd is not None else ""
    r[9] = _num(turnover, 2) if turnover is not None else ""
    r[10] = int(listed_days) if listed_days is not None else ""
    r[11] = note
    return r
