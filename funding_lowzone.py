#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""① 负费率持续性 + 低位（来源：daidaibtc 2025-03-31）

他的原话大意：跨所套利时，是观察到「底部的负费率长期维持得很不对劲」才抓到那笔
裸多 1M 的。所以这条信号要同时满足 **负费率 + 位置在底部 + 持续够久**。

★★ 2026-11-02 回测后定下的口径（推翻了此前两版说法）
  1) **位置是主筛，费率只是加成**。129 个币 / 2,090 个抽样点（每 7 天取一点，
     2026-05-19~11-02）：
         低位(≤20%) + 负费率   121 样本  +2.93% / 61.2%
         低位(≤20%) + 正费率   539 样本  +1.50% / 54.9%
         中位      + 负费率             −1.57% / 44.1%
         高位      + 负费率             −1.88% / 37.9%
     → 负费率放在高位是**负 alpha**，必须卡位置。
  2) **可比量是累积费率，不是时长**。LSK 每小时 −0.2% × 26h = −5.2%，
     而 8h 币 −0.01% × 72h = −0.09%，同样叫「72 小时」代价差 58 倍。
     → 时长只做**最低门槛**（72h），排序用累积费率。
  3) **不设累积费率硬阈值**。`≤ −0.5%` 在低位下只剩 17 个样本，
     会把观察池清空；改为按累积费率排序，保证一直有 5~10 个对象。
  4) 24–72h 档 83 样本 −0.03% / 49.4%（等于随机），所以 72h 以下不记。

推送节奏（2026-11-02 用户要求改，原方案「只记录不推送」已废弃）：
  · 每 2 小时照旧写 Google Sheet「爆空与负费率观察」，**不推**（避免同一批币反复刷）。
  · **每天北京时间 09:00 发一条日报**到 Telegram：本轮命中数 + 榜上前 8 名。
  · **0 命中也发**。用户原话「我怎么知道呢 这个难道是为了跑而跑吗」——
    不发声就等于无法区分「没信号」和「任务挂了」。日报同时是存活证明。

局限（写清楚，别当成已验证结论）：
  · 回测只有 5.5 个月、单一下跌环境；
  · 样本非独立（同一币多期重叠）；
  · 未扣手续费与滑点。
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import opps_writer

logger = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"

MIN_NEG_HOURS = 72.0      # 连续负费率最低时长（按小时，不按「期数」）
MAX_POS = 0.20            # 一年低点分位上限（0=最低）
TOP_N = 60                # 最多查多少个「当前费率最负」的币（控请求量）
BUDGET_SECONDS = 420      # 本模块自己的时间预算。2026-11-02 实测：代理慢时
                          # 150s 只跑完 12/59 个，命中从 4 个掉到 1 个 —— 结果不可复现。
                          # 单请求最坏 48s（直连 3×8s + 代理 4×6s），
                          # 59 个 / 10 并发 × 48s ≈ 283s，420s 有足够余量。


def _neg_streak(hist, now_ts):
    """从已结算的费率历史里，取「最近这一段连续为负」的时长与累积值。

    hist: [{"fundingTime": ms, "fundingRate": "0.0001"}, ...]
    返回 (neg_hours, cum_pct, periods, truncated)

    · 时长按**结算时间戳**算：now − 连续段最早那条的结算时间。
      费率在 t 时刻结算、覆盖 (t_prev, t]，所以这个算法少算最多一个结算周期，
      对 72h 这种量级的门槛没有影响（8h 币误差 ≤ 11%，1h 币 ≤ 1.4%）。
    · truncated=True 表示 500 期全是负的 → neg_hours 只是**下界**。
    """
    rows = []
    for h in hist:
        try:
            rows.append((int(h["fundingTime"]) / 1000.0, float(h["fundingRate"])))
        except (KeyError, TypeError, ValueError):
            continue
    if not rows:
        return 0.0, 0.0, 0, False
    rows.sort()

    cum, periods, first_ts = 0.0, 0, None
    for ts, r in reversed(rows):
        if r < 0:
            cum += r
            periods += 1
            first_ts = ts
        else:
            break
    if periods == 0:
        return 0.0, 0.0, 0, False

    neg_hours = max(0.0, (now_ts - first_ts) / 3600.0)
    return neg_hours, cum * 100.0, periods, periods >= len(rows)


def _fetch_neg_pool(monitor):
    """当前费率为负的 USDT 合约，按费率从最负到最不负排序。"""
    resp = None
    for i in range(3):
        resp = monitor.request_with_retry(f"{FAPI}/fapi/v1/premiumIndex", timeout=15)
        if isinstance(resp, list):
            break
        if i < 2:
            logger.warning(f"[funding] premiumIndex 第 {i + 1} 次取数失败，重试")
            time.sleep(2)
    if not isinstance(resp, list):
        logger.error("[funding] premiumIndex 取数失败")
        return []
    out = []
    for p in resp:
        sym = p.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        try:
            rate = float(p["lastFundingRate"])
        except (KeyError, TypeError, ValueError):
            continue
        if rate < 0:
            out.append((sym, rate))
    out.sort(key=lambda x: x[1])
    return out[:TOP_N]


def _candidate(monitor, symbol, now_ts, cur_rate):
    """单个币：连续负费率 + 位置。不满足门槛返回 None。"""
    hist = monitor.request_with_retry(
        f"{FAPI}/fapi/v1/fundingRate?symbol={symbol}&limit=500", timeout=12
    )
    if not isinstance(hist, list) or not hist:
        # 代理抖动下重试一次：一个币漏掉会让「本轮命中数」随网络波动，
        # 看起来像信号时有时无。
        hist = monitor.request_with_retry(
            f"{FAPI}/fapi/v1/fundingRate?symbol={symbol}&limit=500", timeout=12
        )
    if not isinstance(hist, list) or not hist:
        return None

    neg_hours, cum_pct, periods, truncated = _neg_streak(hist, now_ts)
    if neg_hours < MIN_NEG_HOURS:
        return None

    yp = monitor.get_year_position(symbol)
    if not yp or yp.get("pos") is None:
        return None
    if yp["pos"] > MAX_POS:
        return None

    return {
        "symbol": symbol,
        "token": symbol[:-4],
        "pos": yp["pos"],
        "dd": yp.get("dd"),
        "neg_hours": neg_hours,
        "cum_pct": cum_pct,
        "cur_pct": cur_rate * 100.0,
        "periods": periods,
        "truncated": truncated,
    }


def scan(monitor, deadline=None):
    started = time.monotonic()
    deadline = deadline or (started + BUDGET_SECONDS)
    now_ts = time.time()

    pool = _fetch_neg_pool(monitor)
    logger.info(f"[funding] 当前负费率合约 {len(pool)} 个进入候选（上限 {TOP_N}）")
    if not pool:
        return [], 0

    results = {}
    pool_exec = ThreadPoolExecutor(max_workers=10)
    try:
        futs = {
            pool_exec.submit(_candidate, monitor, sym, now_ts, rate): sym
            for sym, rate in pool
        }
        for done, fut in enumerate(as_completed(futs), 1):
            sym = futs[fut]
            try:
                row = fut.result()
            except Exception as e:  # noqa: BLE001
                logger.debug("[funding] %s 失败: %s", sym, e)
                row = None
            if row:
                results[sym] = row
            if done % 20 == 0:
                logger.info(f"[funding] 进度 {done}/{len(pool)}，命中 {len(results)}")
            if time.monotonic() > deadline:
                logger.warning(f"[funding] 预算用尽，停止等待剩余 {len(pool) - done} 个")
                break
    finally:
        pool_exec.shutdown(wait=False, cancel_futures=True)

    rows = sorted(results.values(), key=lambda r: r["cum_pct"])
    logger.info(
        f"[funding] 连续负费率≥{MIN_NEG_HOURS:.0f}h 且价位≤{MAX_POS*100:.0f}% 的 "
        f"{len(rows)} 个（耗时 {time.monotonic() - started:.1f}s）"
    )
    return rows, len(pool)


def build_digest(rows, pool_n, first_seen=None, now=None):
    """日报文案。纯函数，方便本地拿假数据验排版。

    first_seen: {TOKEN: 最早出现日期 YYYY-MM-DD}，用于标 🆕。
                取不到就传 {}，此时不标新进（宁可不标，也不乱标）。
    """
    first_seen = first_seen or {}
    now = now or datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    head = f"📉 负费率低位 · 每日观察 {now.strftime('%m-%d')}"

    if pool_n <= 0:
        return (
            head + "\n\n"
            "⚠️ 负费率合约取数失败（或当前确实一个都没有），本轮没有扫描结果。\n"
            "这是取数问题，不是「没信号」。"
        )

    lines = [
        head,
        "",
        f"负费率合约 {pool_n} 个 → 连续≥{MIN_NEG_HOURS:.0f}h 且价位≤{MAX_POS*100:.0f}% "
        f"的 {len(rows)} 个",
    ]
    if not rows:
        lines += [
            "",
            "榜上是空的。这条每天都会发，空着说明任务在跑、只是今天没信号。",
        ]
        return "\n".join(lines)

    lines.append("")
    for i, r in enumerate(rows[:8], 1):
        tok = r["token"]
        is_new = first_seen.get(tok.upper()) in (None, today)
        dd = "—" if r.get("dd") is None else f"{r['dd']:.1f}%"
        hrs = f"{r['neg_hours']:.0f}h"
        if r.get("truncated"):
            hrs = "≥" + hrs
        lines.append(
            f"{i}. {tok} 累积 {r['cum_pct']:.2f}%｜{hrs}｜价位 {r['pos'] * 100:.1f}%｜DD {dd}"
            + (" 🆕" if is_new else "")
        )
    if len(rows) > 8:
        lines.append(f"…另有 {len(rows) - 8} 个，见表格")
    lines += ["", "🆕 = 第一次进这张表 ｜ 只观察，不构成买入信号"]
    return "\n".join(lines)


def run(monitor, notify=False):
    """入口：扫描 → 写观察表 → （仅日报轮）推送。

    notify=True 只在每天北京 09:00 那一轮由 main.py 传入，
    其余 2 小时轮次只写表不推，避免同一批币反复刷屏。
    """
    started = time.monotonic()
    rows, pool_n = scan(monitor)

    # ⚠️ 必须在上写入之前读历史，否则本轮刚写的行会把「🆕」抹掉
    first_seen = {}
    if notify and rows:
        try:
            first_seen = opps_writer.funding_first_seen()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[funding] 读历史失败，日报不标新进: {e}")

    if not rows:
        logger.info("[funding] 本轮无符合项，未写入")
    else:
        sheet_rows = []
        for r in rows:
            note = "500期全负，时长是下界" if r["truncated"] else ""
            sheet_rows.append(
                opps_writer.funding_row(
                    r["token"], r["pos"], r["dd"], r["neg_hours"],
                    r["cum_pct"], r["cur_pct"], note,
                )
            )
        n_new, n_upd = opps_writer.upsert(sheet_rows)
        logger.info(f"[funding] 观察表 写入 {n_new} 新行 / 更新 {n_upd} 行")

    if notify:
        try:
            monitor.send_telegram(build_digest(rows, pool_n, first_seen))
            logger.info("[funding] 日报已发送")
        except Exception as e:  # noqa: BLE001
            logger.error(f"[funding] 日报发送失败: {e}")

    logger.info(f"[funding] 总耗时 {time.monotonic() - started:.1f}s")
    return len(rows)
