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


# ==================== 日报发送状态（2026-11-03 新增）====================
# 为什么需要它：日报原来只挂在 workflow 的一条 cron（'7 1 * * *'）上。
# 2026-11-03 该班次被 GitHub 直接丢掉（连 00:37Z 一起丢），结果一整天没有日报，
# 而且没有任何告警 —— 定时日报从上线起一次都没成功发出过。
#
# 现在改成：北京 09:00 之后，**只要今天还没发过就发**。每 2 小时的班次都是重试机会。
# 状态存在同一工作簿的 opps_state 页：
#   · 跨机器（不依赖本机文件，符合「设备无关」原则）
#   · 不加新依赖（复用已有 service account 与 SHEET_ID）
STATE_TAB = "opps_state"
STATE_KEY = "last_digest_date"


def _get_state_sheet():
    """取状态页；没有就建。读失败由调用方处理，这里直接抛。"""
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
        return sh.worksheet(STATE_TAB)
    except gspread.exceptions.WorksheetNotFound:
        wks = sh.add_worksheet(title=STATE_TAB, rows=10, cols=2)
        wks.append_row(["键", "值"], value_input_option="RAW")
        return wks


def last_digest_date():
    """返回上次日报发送的北京日期（YYYY-MM-DD）。

    三态返回，调用方必须区分：
      · "YYYY-MM-DD" → 上次发送日
      · ""           → 状态页正常但没有记录（首次上线）
      · None         → **读失败**。此时应退回保守逻辑（只信 workflow 注入），
                       否则 Sheet 一挂就会每 2 小时刷一条日报。
    """
    try:
        vals = _get_state_sheet().get_all_values()
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 读状态失败: {type(e).__name__}: {e}")
        return None

    for r in vals[1:]:
        if len(r) >= 2 and str(r[0]).strip() == STATE_KEY:
            return str(r[1]).strip()[:10]
    return ""


def mark_digest_sent(day):
    """记录「今天已发」。写失败返回 False —— 下一班可能重发一次，但不漏发。"""
    try:
        wks = _get_state_sheet()
        vals = wks.get_all_values()
        for i, r in enumerate(vals[1:], start=2):
            if len(r) >= 2 and str(r[0]).strip() == STATE_KEY:
                wks.update(values=[[day]], range_name=f"B{i}", value_input_option="RAW")
                return True
        wks.append_row([STATE_KEY, day], value_input_option="RAW")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 写状态失败: {type(e).__name__}: {e}")
        return False


# ---- 主报告幂等：记录上次成功推送的时间戳 ----
# 为什么需要：GitHub schedule 会丢班（实测交付约 3.7/12），所以外部定时器
# （Cloudflare Worker Cron）会额外触发 workflow_dispatch 兜底。两边都触发时，
# 不加闸就会同一时段推两条，并把账本的「扫描确认次数」重复 +1。
# 与 last_digest_date 复用同一张 opps_state 页，只是 key 不同。
STATE_KEY_REPORT_TS = "last_report_ts"


def last_report_ts():
    """上次主报告成功推送的 Unix 时间戳。

    三态，调用方必须区分：
      · float > 0 → 上次推送时刻
      · 0         → 状态页正常但从未记录（首次上线）
      · None      → **读失败**。此时应继续跑：宁可重复一次，不可漏推。
    """
    try:
        vals = _get_state_sheet().get_all_values()
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 读主报告状态失败: {type(e).__name__}: {e}")
        return None

    for r in vals[1:]:
        if len(r) >= 2 and str(r[0]).strip() == STATE_KEY_REPORT_TS:
            try:
                return float(str(r[1]).strip())
            except (TypeError, ValueError):
                return 0
    return 0


def mark_report_sent(ts):
    """记录主报告推送时刻。写失败返回 False —— 下一轮可能重推一次，但不漏。"""
    val = f"{float(ts):.0f}"
    try:
        wks = _get_state_sheet()
        vals = wks.get_all_values()
        for i, r in enumerate(vals[1:], start=2):
            if len(r) >= 2 and str(r[0]).strip() == STATE_KEY_REPORT_TS:
                wks.update(values=[[val]], range_name=f"B{i}", value_input_option="RAW")
                return True
        wks.append_row([STATE_KEY_REPORT_TS, val], value_input_option="RAW")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 写主报告状态失败: {type(e).__name__}: {e}")
        return False


# ---- 升温报告幂等：当天只发一次 ----
# 为什么需要：warmup.yml 原本只有一条 '0 1 * * *'（UTC 01:00，北京 09:00），
# GitHub schedule 丢班就一整天没有升温报告。2026-11-03 给 Cloudflare Worker
# 加了第三条 Cron Trigger（'22 1 * * *'）兜底，于是同一天会有两次触发。
# 而 warmup 分支在 main.py 里跑在 90 分钟闸门**之前**（该分支自己 return），
# 那道闸管不到它，所以必须在这里单独拦一道，否则会推两条一模一样的报告。
# 与日报、主报告复用同一张 opps_state 页，只是 key 不同。
STATE_KEY_WARMUP_DATE = "last_warmup_date"


def _state_get(key):
    """读状态页某个 key 的原始字符串值。

    三态，调用方必须区分：
      · 非空字符串 → 上次记录值
      · ""         → 状态页正常但没有该 key（首次上线）
      · None       → **读失败**。调用方应继续跑：宁可重复一次，不可漏推。
    """
    try:
        vals = _get_state_sheet().get_all_values()
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 读 {key} 失败: {type(e).__name__}: {e}")
        return None

    for r in vals[1:]:
        if len(r) >= 2 and str(r[0]).strip() == key:
            return str(r[1]).strip()
    return ""


def _state_set(key, val):
    """写状态页某个 key。写失败返回 False —— 下一轮可能重发一次，但不漏。"""
    try:
        wks = _get_state_sheet()
        vals = wks.get_all_values()
        for i, r in enumerate(vals[1:], start=2):
            if len(r) >= 2 and str(r[0]).strip() == key:
                wks.update(values=[[val]], range_name=f"B{i}", value_input_option="RAW")
                return True
        wks.append_row([key, val], value_input_option="RAW")
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[opps_state] 写 {key} 失败: {type(e).__name__}: {e}")
        return False


def last_warmup_date():
    """上次升温报告发送的北京日期（YYYY-MM-DD）。三态语义同 _state_get。"""
    v = _state_get(STATE_KEY_WARMUP_DATE)
    if v is None:
        return None
    return v[:10]


def mark_warmup_sent(day):
    """记录「今天已发升温报告」。写失败返回 False。"""
    return _state_set(STATE_KEY_WARMUP_DATE, day)


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
