#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""③ 小市值爆空扫描（来源：daidaibtc 2025-02-25 / 02-26）

他的原话把条件说得很死：币安合约有、市值一两千万到几千万、跌得特别多、
新 List、没下架风险、活跃度高。

落成机器判据（2026-11-02 定，含两处实测修正）：
  · 币安 USDT 永续，status=TRADING（天然排除已下架/待下架）
  · 上市 7~90 天（exchangeInfo.onboardDate）
  · 距「上市以来最高价」回撤 ≤ −60%（新币没有一年历史，用上市以来高点）
  · OI 名义额 ≤ $1,500 万（**替代市值做粗筛**，因为 rada 没有市值源；
    这只是代理指标，不是市值本身）
  · 当前资金费率 ≤ 0（空头在付钱 = 空头拥挤）
  · 24h 成交额 / OI 名义额 ∈ [1.5, 15]
      —— 原来写「≥3」，实测修正：币安 top40 的换手率
         P25=0.95 / P50=1.61 / P75=3.76 / P90=11.64，`≥3` 会砍掉六成；
         而 ESPORTS 那种 52.57 是薄 OI 的妖币，必须加上限。
  · 24h 成交额 ≥ $30 万（死币不要）
  · 排除股票/商品上链（tradfi_filter，用户明确「我不关注股」）

噪音控制：
  · 同一个币**只推第一次**（历史记录在 Google Sheet「爆空与负费率观察」里）。
  · 每条消息最多 5 个，按换手率降序。
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import opps_writer
from tradfi_filter import from_exchange_info as _tradfi_from_info

logger = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"

MIN_LIST_DAYS = 7
MAX_LIST_DAYS = 90
MIN_DD = -60.0            # 距上市以来高点回撤上限（负数）
MAX_OI_USD = 15_000_000   # OI 名义额上限
MAX_FUNDING = 0.0         # 当前费率必须 ≤ 0
TURNOVER_MIN = 1.5
TURNOVER_MAX = 15.0
MIN_QUOTE_VOL = 300_000
MIN_BARS = 20             # 少于 20 根日线不下结论
MAX_CANDIDATES = 80
BUDGET_SECONDS = 150
MAX_PUSH = 5


def _exchange_info(monitor):
    info = monitor.request_with_retry(f"{FAPI}/fapi/v1/exchangeInfo", timeout=20)
    if not isinstance(info, dict) or not info.get("symbols"):
        logger.error("[squeeze] exchangeInfo 取数失败")
        return None
    return info


def _year_stats(klines):
    """上市以来的高点回撤与价位分位（新币没有 365 天，就用全部可得日线）。"""
    if not isinstance(klines, list) or len(klines) < MIN_BARS:
        return None
    try:
        highs = [float(k[2]) for k in klines]
        lows = [float(k[3]) for k in klines]
        close = float(klines[-1][4])
    except (IndexError, TypeError, ValueError):
        return None
    hi, lo = max(highs), min(lows)
    if hi <= lo or close <= 0:
        return None
    return {
        "pos": (close - lo) / (hi - lo),
        "dd": (close - hi) / hi * 100.0,
        "bars": len(klines),
    }


def _candidate(monitor, symbol, listed_days, quote_vol, funding_pct):
    """单个候选：日线位置 + OI 名义额 + 换手率。不满足返回 None。"""
    kl = monitor.request_with_retry(
        f"{FAPI}/fapi/v1/klines?symbol={symbol}&interval=1d&limit=365", timeout=15
    )
    st = _year_stats(kl)
    if not st or st["dd"] > MIN_DD:
        return None

    oi = monitor.request_with_retry(
        f"{FAPI}/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=1",
        timeout=12,
    )
    oi_usd = None
    if isinstance(oi, list) and oi:
        try:
            oi_usd = float(oi[-1]["sumOpenInterestValue"])
        except (KeyError, TypeError, ValueError):
            oi_usd = None
    if not oi_usd or oi_usd <= 0 or oi_usd > MAX_OI_USD:
        return None

    turnover = quote_vol / oi_usd
    if not (TURNOVER_MIN <= turnover <= TURNOVER_MAX):
        return None
    if funding_pct > MAX_FUNDING:
        return None

    return {
        "symbol": symbol,
        "token": symbol[:-4],
        "pos": st["pos"],
        "dd": st["dd"],
        "bars": st["bars"],
        "listed_days": listed_days,
        "oi_usd": oi_usd,
        "turnover": turnover,
        "funding_pct": funding_pct,
    }


def scan(monitor, deadline=None):
    started = time.monotonic()
    deadline = deadline or (started + BUDGET_SECONDS)
    now_ms = time.time() * 1000

    info = _exchange_info(monitor)
    if not info:
        return []
    excluded = _tradfi_from_info(info)

    tick = monitor.request_with_retry(f"{FAPI}/fapi/v1/ticker/24hr", timeout=20)
    prem = monitor.request_with_retry(f"{FAPI}/fapi/v1/premiumIndex", timeout=15)
    if not isinstance(tick, list) or not isinstance(prem, list):
        logger.error("[squeeze] ticker/premiumIndex 取数失败")
        return []
    prem_map = {p["symbol"]: p for p in prem if p.get("symbol")}
    tick_map = {x["symbol"]: x for x in tick if x.get("symbol")}

    cands = []
    for s in info["symbols"]:
        sym = s.get("symbol") or ""
        if not sym.endswith("USDT") or sym in excluded:
            continue
        if s.get("contractType") != "PERPETUAL" or s.get("status") != "TRADING":
            continue
        onboard = s.get("onboardDate")
        if not onboard:
            continue
        days = (now_ms - float(onboard)) / 86400000.0
        if not (MIN_LIST_DAYS <= days <= MAX_LIST_DAYS):
            continue
        t = tick_map.get(sym)
        if not t:
            continue
        try:
            qv = float(t["quoteVolume"])
            fp = float(prem_map[sym]["lastFundingRate"]) * 100.0
        except (KeyError, TypeError, ValueError):
            continue
        # 费率在这里先挡一道：省掉后面每个候选的 2 次请求
        if qv < MIN_QUOTE_VOL or fp > MAX_FUNDING:
            continue
        cands.append((sym, days, qv, fp))

    logger.info(
        f"[squeeze] 上市 {MIN_LIST_DAYS}~{MAX_LIST_DAYS} 天的 USDT 永续共 {len(cands)} 个"
        f"（已排除传统资产 {len(excluded)} 个）"
    )
    if not cands:
        return []
    cands = cands[:MAX_CANDIDATES]

    results = []
    pool = ThreadPoolExecutor(max_workers=6)
    try:
        futs = {
            pool.submit(_candidate, monitor, sym, days, qv, fp): sym
            for sym, days, qv, fp in cands
        }
        for done, fut in enumerate(as_completed(futs), 1):
            try:
                row = fut.result()
            except Exception as e:  # noqa: BLE001
                logger.debug("[squeeze] 候选失败: %s", e)
                row = None
            if row:
                results.append(row)
            if done % 20 == 0:
                logger.info(f"[squeeze] 进度 {done}/{len(cands)}，命中 {len(results)}")
            if time.monotonic() > deadline:
                logger.warning(f"[squeeze] 预算用尽，停止等待剩余 {len(cands) - done} 个")
                break
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    results.sort(key=lambda r: r["turnover"], reverse=True)
    logger.info(f"[squeeze] 命中 {len(results)} 个（耗时 {time.monotonic() - started:.1f}s）")
    return results


def _fmt_usd(v):
    if v >= 100_000_000:
        return f"${v / 100_000_000:.2f}亿"
    if v >= 10_000:
        return f"${v / 10_000:.0f}万"
    return f"${v:.0f}"


def format_message(rows):
    if not rows:
        return ""
    lines = ["💥 **小市值爆空候选（首次命中）**"]
    for r in rows[:MAX_PUSH]:
        lines.append(
            f"• `{r['token']}`: 上市 {r['listed_days']:.0f} 天 | "
            f"DD {r['dd']:+.0f}% | 价位 {r['pos'] * 100:.0f}% | "
            f"OI {_fmt_usd(r['oi_usd'])} | 费率 {r['funding_pct']:+.4f}% | "
            f"换手 {r['turnover']:.1f}"
        )
    lines.append("")
    lines.append(
        "口径：上市 7–90 天 · 距上市以来高点 ≤−60% · OI ≤$1,500万 · "
        "费率≤0 · 换手 1.5–15（OI 是市值代理，不是市值本身）"
    )
    lines.append("同一币只推首次；来源 daidaibtc 2025-02-25 的做空条件")
    return "\n".join(lines)


def run(monitor):
    """入口：扫描 → 过滤历史上推过的币 → 写表 → 推送。返回本轮新命中数。"""
    started = time.monotonic()
    rows = scan(monitor)
    if not rows:
        logger.info("[squeeze] 本轮无符合项")
        return 0

    try:
        seen = opps_writer.seen_tokens()
    except Exception as e:  # noqa: BLE001
        logger.error(f"[squeeze] 读观察表失败，无法去重: {type(e).__name__}: {e}")
        seen = set()

    fresh = [r for r in rows if r["token"].upper() not in seen]
    logger.info(f"[squeeze] 本轮命中 {len(rows)} 个，其中首次 {len(fresh)} 个")

    if fresh:
        try:
            opps_writer.upsert([
                opps_writer.squeeze_row(
                    r["token"], r["pos"], r["dd"], r["funding_pct"],
                    r["oi_usd"], r["turnover"], r["listed_days"],
                    f"日线{r['bars']}根",
                )
                for r in fresh
            ])
        except Exception as e:  # noqa: BLE001
            # 写表失败仍然推送：漏掉一次信号比重复推一次更糟。
            # 代价是下一轮可能再推一次，日志里记下来便于事后核。
            logger.error(f"[squeeze] 写观察表失败（下轮可能重复推）: {type(e).__name__}: {e}")

        msg = format_message(fresh)
        if msg:
            monitor.send_telegram(msg)
            logger.info(f"[squeeze] 已推送 {min(len(fresh), MAX_PUSH)} 个")

    logger.info(f"[squeeze] 完成（总耗时 {time.monotonic() - started:.1f}s）")
    return len(fresh)
