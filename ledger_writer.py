#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 radar 扫出的「低位区」信号追加写入 Google Sheet 账本。

职责边界（很重要，别越界）
  - 只写「信号行」的前 10 列：日期 / Token / 信号来源 / OI分位 / OI变化% /
    Price变化% / 方向感知 / 共振确认 / 入场价 / BTC入场价，
    另加 X「扫描确认次数」、Y「DD(距1年高点%)」、Z「价位分位%」
  - **结果列（10~22：24h/48h/7d 价格与 vs BTC 超额）一律留空**，
    由 GitHub Actions 的每日回填任务（backfill_ledger.py）计算补上
  - 同一天同一个币只记第一条（当天重复触发不重复记账）；当天再次命中时
    只把 X 列「扫描确认次数」+1 —— 这一个数就是「该币当天被 2h 扫描确认了几次」
  - X 列是唯一会被更新的列，其余既有单元格一概不动

★ 2026-09-22 新增 Y/Z 两列「位置」维度
    背景：这张表原先只有「杠杆贵不贵」（OI分位），没有「价格贵不贵」。
    radar 其实一直在算（get_year_position 同时返回 pos 和 dd），
    而且 DD 早就推到 Telegram 了，但落表时被丢在两处：
      ① main.py 的 low_zone_rows 字段白名单没有 year_dd（源头丢）
      ② 本文件 build_rows 只写了 oi_pos，连传过来的 year_pos 也没写（第二道丢）
    现在两道都补上。口径与 main.py 的 get_year_position 完全一致
    （高点取**已收盘**的 365 根日线，剔除当天未收盘那根）。
    Y/Z 写成**数字**（不是带 % 的文本），方便排序与统计。

凭证：环境变量 GSHEET_CREDENTIALS（service account JSON 的完整文本）

低位区口径（与 main.py 中 scan_and_collect 的 low_zone 严格一致）：
    杠杆位（OI 30 天存量分位） <= 25%
    且 OI 24h 变化 > 1%
    且 |24h 涨跌| <= 5%
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from tradfi_filter import excluded_symbols as non_crypto_symbols

SHEET_ID = "1HBE_HXUgm7Sj0FJknDWO8hBi1OH3zNPJhWVBMiRV25w"
TAB = "自动任务信号回测"
NCOL = 26          # 23 个既有列 + X「扫描确认次数」 + Y/Z 两列位置
X_COL = 23         # X 列的 0-based 下标
Y_COL = 24         # DD(距1年高点%)
Z_COL = 25         # 价位分位%
HDR_X = "扫描确认次数"
HDR_Y = "DD(距1年高点%)"
HDR_Z = "价位分位%"
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
    """把 low_zone 的原始 dict 列表转成 26 列的表格行"""
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
        # Y/Z：位置两维。写数字不写文本，方便排序与统计。
        ydd, ypos = d.get("year_dd"), d.get("year_pos")
        row[Y_COL] = round(float(ydd), 1) if ydd is not None else "N/A"
        row[Z_COL] = round(float(ypos) * 100, 1) if ypos is not None else "N/A"
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


def read_marks(now=None):
    """只读账本，供推送时给低位区那些币打「重复标注」。

    返回 (date_str, today_counts, prev_symbols)
      today_counts : {TOKEN: 今天账本里已经记到的「扫描确认次数」}
      prev_symbols : 昨天出现在账本里的 TOKEN 集合

    为什么要有它：推送发生在落表之前（推送不该被写表拖住，写表失败也不该
    影响推送）。所以推送时看不到「本次是今天第几次」——只能先读一遍现状，
    再推算出「本次确认后 = 第 n+1 次」。这里的 n 与 write_low_zone 累加 X 列
    的口径完全一致。

    只读，不写任何单元格。失败会抛异常，由调用方决定是否降级为无标注。
    """
    now = now or datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    prev_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    wks = _sheet()
    existing = wks.get_all_values()

    today_counts: dict = {}
    prev_symbols: set = set()
    for r in existing[1:]:
        if len(r) < 2:
            continue
        d0 = str(r[0])[:10]
        t0 = str(r[1]).strip().upper()
        if not t0:
            continue
        if d0 == date_str:
            cur = str(r[X_COL]).strip() if len(r) > X_COL else ""
            n = int(cur) if cur.isdigit() else 0
            today_counts[t0] = max(today_counts.get(t0, 0), n)
        elif d0 == prev_str:
            prev_symbols.add(t0)
    return date_str, today_counts, prev_symbols


def _ensure_header(wks, existing):
    """确保 X / Y / Z 列表头存在（缺哪个补哪个，一次批更新）"""
    import gspread

    head = existing[0] if existing else []
    want = ((X_COL, HDR_X), (Y_COL, HDR_Y), (Z_COL, HDR_Z))
    ops = []
    for col, name in want:
        if len(head) > col and str(head[col]).strip():
            continue
        ops.append({"range": gspread.utils.rowcol_to_a1(1, col + 1),
                    "values": [[name]]})
    if not ops:
        return
    try:
        wks.batch_update(ops, value_input_option="RAW")
    except Exception as e:  # noqa: BLE001
        print(f"  [warn] 表头写入失败: {e}")


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
