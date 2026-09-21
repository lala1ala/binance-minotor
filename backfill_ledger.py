#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回填「自动任务信号回测」的结果列（24h/48h/7d 价格、涨跌%、BTC 基准、vs BTC 超额）。

★ 运行环境：GitHub Actions（仓库 binance-minotor）。**不依赖任何本机路径**。
  - 凭证：环境变量 GSHEET_CREDENTIALS（服务账号 JSON 全文），读不到才退回本地文件
  - 币安访问：直连 → 代理降级（GitHub runner 在美区，币安直连会 451/超时）
  - 数据源：Google Sheet 线上表（不依赖本地快照，本地快照不会自动刷新）

口径（已用 08-19 的 17 条历史行标定，最优拟合）：
  锚点 = 信号日 D 的 00:00 UTC
  取值 = 该 00:00 UTC 小时线的**收盘价**（≈ D 01:00 UTC 的价）
  +24h/+48h/+7d = 锚点 + 24/48/168 小时，同法取值
  vs BTC = 该币区间涨跌% − BTC 同期涨跌%
  市场 = 优先币安 U 本位永续(fapi)，无则用现货(spot)

只填空单元格，不覆盖已有值。默认 dry-run，加 --write 才写回 Sheet。
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

FAPI = "https://fapi.binance.com"
SPOT = "https://api.binance.com"
HOUR_MS = 3600 * 1000
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SYM_RE = re.compile(r"^[A-Z0-9]{2,15}$")

SHEET_ID_LEDGER = "1HBE_HXUgm7Sj0FJknDWO8hBi1OH3zNPJhWVBMiRV25w"
SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
PROXY_LIST_URL = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"

# 结果列（0-indexed）
COLS = {
    "p24": 10, "r24": 11, "br24": 12, "brp24": 13, "v24": 14,
    "p48": 15, "r48": 16, "brp48": 17, "v48": 18,
    "p7d": 19, "r7d": 20, "brp7d": 21, "v7d": 22,
}
HORIZONS = (("24", 24, ("p24", "r24", "br24", "brp24", "v24")),
            ("48", 48, ("p48", "r48", "brp48", "v48")),
            ("7d", 168, ("p7d", "r7d", "brp7d", "v7d")))


# ==================== 网络层：直连 → 代理降级 ====================
class Net:
    """带降级的 HTTP 客户端（与 radar main.py 同策略）。

    按成本从低到高：直连 → 复用上次成功的代理 → 试 N 个候选代理。
    直连连续失败 3 次后，本次运行不再尝试直连 —— 否则每个请求都要白等一次超时。
    单请求最坏耗时 8 + 4×8 = 40s，实测命中复用代理后 ~1s。
    """

    DIRECT_TIMEOUT = 8
    PROXY_TIMEOUT = 8
    MAX_PROXY_TRIES = 4

    def __init__(self):
        self.proxies = []
        self._good = None
        self._direct_fail = 0
        self._direct_off = False
        self._lock = threading.Lock()
        self.stats = {"direct": 0, "proxy": 0, "fail": 0}

    @staticmethod
    def _try(url, timeout, proxy=None):
        """单次请求：成功返回 JSON，失败返回 None（含币安错误体 {code,msg}）。"""
        try:
            r = requests.get(url, proxies=proxy, timeout=timeout)
            if r.status_code != 200:
                return None
            d = r.json()
            if isinstance(d, dict) and "code" in d:
                return None
            return d
        except Exception:  # noqa: BLE001
            return None

    def _load_proxies(self):
        with self._lock:
            if self.proxies:
                return
        try:
            r = requests.get(PROXY_LIST_URL, timeout=10)
            if r.status_code == 200:
                lst = [p.strip() for p in r.text.splitlines() if p.strip()][:50]
                with self._lock:
                    self.proxies = [{"http": "http://" + p, "https": "http://" + p}
                                    for p in lst]
                print("  [net] 获取代理 %d 个" % len(lst))
        except Exception as e:  # noqa: BLE001
            print("  [net] 代理列表获取失败: %s" % e)

    def get(self, url, timeout=None):
        to = timeout or self.DIRECT_TIMEOUT

        # --- 1. 直连 ---
        if not self._direct_off:
            d = self._try(url, to)
            if d is not None:
                with self._lock:
                    self._direct_fail = 0
                    self.stats["direct"] += 1
                return d
            with self._lock:
                self._direct_fail += 1
                if self._direct_fail >= 3 and not self._direct_off:
                    self._direct_off = True
                    print("  [net] 直连连续失败 3 次 → 本次运行改为全部走代理")

        # --- 2. 复用上次成功的代理 ---
        with self._lock:
            good = self._good
        if good is not None:
            d = self._try(url, to, good)
            if d is not None:
                with self._lock:
                    self.stats["proxy"] += 1
                return d
            with self._lock:
                self._good = None

        # --- 3. 候选代理 ---
        self._load_proxies()
        with self._lock:
            cands = list(self.proxies[:self.MAX_PROXY_TRIES])
        for p in cands:
            d = self._try(url, self.PROXY_TIMEOUT, p)
            if d is not None:
                with self._lock:
                    self._good = p
                    self.stats["proxy"] += 1
                return d

        with self._lock:
            self.stats["fail"] += 1
        return None


NET = Net()


def http_json(url, timeout=40):
    """兼容旧签名：统一走 NET（内部已含重试与降级）。"""
    return NET.get(url, timeout=timeout)


# ==================== 币安数据 ====================
def load_tradable():
    """返回 (fapi_all, spot_all, fapi_trading, spot_trading)

    all = exchangeInfo 里出现过的全部交易对（含已下线的），
    因为回填只需要**历史** K 线，已下线币种（ICX/SCRT/STORJ 之类）仍可取到。
    """
    fx_all, sp_all, fx_ok, sp_ok = set(), set(), set(), set()
    d = http_json(FAPI + "/fapi/v1/exchangeInfo", timeout=90)
    if d:
        for s in d.get("symbols", []):
            fx_all.add(s["symbol"])
            if s.get("status") == "TRADING":
                fx_ok.add(s["symbol"])
    d = http_json(SPOT + "/api/v3/exchangeInfo", timeout=90)
    if d:
        for s in d.get("symbols", []):
            sp_all.add(s["symbol"])
            if s.get("status") == "TRADING":
                sp_ok.add(s["symbol"])
    return fx_all, sp_all, fx_ok, sp_ok


def _page_klines(symbol, market, start_ms, end_ms, out):
    """拉取 [start_ms, end_ms) 的 1h 线写入 out（原地改），遵守 limit=1500 分页。"""
    base = FAPI if market == "fapi" else SPOT
    path = "/fapi/v1/klines" if market == "fapi" else "/api/v3/klines"
    cur = start_ms
    guard = 0
    while cur < end_ms and guard < 20:
        guard += 1
        url = "%s%s?symbol=%s&interval=1h&startTime=%d&limit=1500" % (
            base, path, symbol, cur)
        d = http_json(url)
        if not d:
            return
        for k in d:
            out[int(k[0])] = (float(k[1]), float(k[4]))
        nxt = int(d[-1][0]) + HOUR_MS
        if nxt <= cur:
            return
        cur = nxt
        if len(d) < 1500:
            return
        time.sleep(0.05)


def fetch_1h(symbol, start_ms, end_ms, market, cached=None):
    """取 symbol 的 1h 线，返回 {openTime_ms: (open, close)}。

    ★ 增量缓存：cached 是上次留下的 {open_ts: [o, c]}。只有当**请求范围超出**
      已有覆盖时，才补拉缺失的那一段。历史 K 线不会变，所以尾部增量即可。
      （2026-09-20 的坑：旧版缓存命中就直接整包返回，导致新写入的信号行
       永远取不到锚点价 —— 用「范围校验」修掉后，这里进一步做成双向补拉。）
    """
    out = {}
    if cached:
        try:
            out = {int(k): tuple(v) for k, v in cached.items()}
        except Exception:  # noqa: BLE001
            out = {}

    if out:
        have_min, have_max = min(out), max(out)
        need = end_ms - HOUR_MS
        if have_min <= start_ms and have_max >= need:
            return out
        if have_min > start_ms:
            _page_klines(symbol, market, start_ms, have_min, out)
        if have_max < need:
            _page_klines(symbol, market, have_max + HOUR_MS, end_ms, out)
    else:
        _page_klines(symbol, market, start_ms, end_ms, out)
    return out


def px(series, ts):
    """取 ts 小时线的收盘价；缺失则往前后各找 1~2 小时兜底"""
    for off in (0, -HOUR_MS, HOUR_MS, -2 * HOUR_MS, 2 * HOUR_MS):
        v = series.get(ts + off)
        if v is not None:
            return v[1]
    return None


def pct(a, b):
    return (b / a - 1.0) * 100.0 if (a and b) else None


def fmt_pct(v):
    return "%+.2f%%" % v if v is not None else None


# ==================== Google Sheet ====================
def _credentials():
    """凭证优先级：环境变量 GSHEET_CREDENTIALS（云端） → 本地文件（本机调试）。"""
    from google.oauth2.service_account import Credentials

    raw = os.environ.get("GSHEET_CREDENTIALS", "").strip()
    if raw:
        info = json.loads(raw)
        return Credentials.from_service_account_info(info, scopes=SCOPE)

    local = os.path.join(os.path.expanduser("~"), ".workbuddy", "gsheet_credentials.json")
    if os.path.exists(local):
        return Credentials.from_service_account_file(local, scopes=SCOPE)

    raise SystemExit("缺少凭证：请设置环境变量 GSHEET_CREDENTIALS（服务账号 JSON 全文）")


def _open_sheet():
    import gspread

    gc = gspread.authorize(_credentials())
    return gc.open_by_key(SHEET_ID_LEDGER)


def read_from_sheet(tab):
    return _open_sheet().worksheet(tab).get_all_values()


# ==================== 主流程 ====================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tab", default="自动任务信号回测")
    ap.add_argument("--out", default="_backfill_out")
    ap.add_argument("--xlsx", default="", help="仅本机调试用：读本地 Excel 快照")
    ap.add_argument("--write", action="store_true", help="写回 Google Sheet（默认只预览）")
    ap.add_argument("--workers", type=int, default=6, help="并发拉 K 线的线程数")
    args = ap.parse_args()

    print("=== 信号回填（GitHub Actions 版）===")
    print("时间: %s UTC" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))

    if args.xlsx:
        import openpyxl
        wb = openpyxl.load_workbook(args.xlsx, read_only=True, data_only=True)
        ws = wb[args.tab]
        all_rows = [list(r) for r in ws.iter_rows(values_only=True)]
        wb.close()
    else:
        all_rows = read_from_sheet(args.tab)

    header = all_rows[0]
    ncol = len(header)
    body = [list(r) + [None] * (ncol - len(r)) for r in all_rows[1:] if r and r[0] is not None]
    print("表 %s: 表头 %d 列, 数据 %d 行" % (args.tab, ncol, len(body)))

    # 解析行。日期列存在脏前缀（实测 '663 2026-08-21'），用正则把日期捞出来
    recs, skipped = [], []
    for i, r in enumerate(body):
        sheet_row = i + 2  # 1-based，含表头
        raw_date = str(r[0] or "")
        token = str(r[1] or "").strip()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", raw_date)
        if not m or not SYM_RE.match(token):
            skipped.append((sheet_row, raw_date, token))
            continue
        d = m.group(1)
        if not DATE_RE.match(d):
            skipped.append((sheet_row, raw_date, token))
            continue
        recs.append({
            "row": sheet_row, "date": d, "token": token,
            "cells": {k: r[v] for k, v in COLS.items()},
        })
    print("可解析 %d 行；跳过 %d 行（日期/币种异常）" % (len(recs), len(skipped)))
    if skipped:
        print("  跳过示例: %s" % (skipped[:6],))

    dates = sorted({x["date"] for x in recs})
    print("日期范围 %s ~ %s" % (dates[0], dates[-1]))

    # 交易对解析：优先在交易的永续 → 在交易的现货 → 已下线的永续 → 已下线的现货
    print("拉取币安全量交易对...")
    fx_all, sp_all, fx_ok, sp_ok = load_tradable()
    print("  fapi %d(在交易 %d) / spot %d(在交易 %d)"
          % (len(fx_all), len(fx_ok), len(sp_all), len(sp_ok)))
    need = sorted({x["token"] for x in recs})
    resolved, unresolved = {}, []
    for t in need:
        s = t + "USDT"
        if s in fx_ok:
            resolved[t] = (s, "fapi")
        elif s in sp_ok:
            resolved[t] = (s, "spot")
        elif s in fx_all:
            resolved[t] = (s, "fapi")
        elif s in sp_all:
            resolved[t] = (s, "spot")
        else:
            unresolved.append(t)
    print("  可解析 %d / %d；无法解析 %d: %s"
          % (len(resolved), len(need), len(unresolved), unresolved))

    # 时间窗口：最早 D-1d，最晚 max(D)+8d
    t0 = int(datetime.strptime(dates[0], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000) - 24 * HOUR_MS
    t1 = int(datetime.strptime(dates[-1], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000) + 8 * 24 * HOUR_MS

    os.makedirs(args.out, exist_ok=True)
    cache_path = os.path.join(args.out, "klines_cache.json")
    cache = {}
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                blob = json.load(f)
            if isinstance(blob, dict) and isinstance(blob.get("data"), dict):
                cache = blob["data"]
                print("  命中 K 线缓存 %d 个币种（上次范围 %s）"
                      % (len(cache), blob.get("_scope", "?")))
        except Exception:  # noqa: BLE001
            cache = {}

    print("拉取 BTC 基准...")
    ck_btc = "BTC|fapi"
    btc = fetch_1h("BTCUSDT", t0, t1, "fapi", cache.get(ck_btc))
    cache[ck_btc] = {str(k): list(v) for k, v in btc.items()}

    print("拉取各币种 1h 线（%d 线程）..." % args.workers)
    series = {}

    def work(token, sym, mk):
        return fetch_1h(sym, t0, t1, mk, cache.get("%s|%s" % (token, mk)))

    todo = sorted(resolved.items())
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(work, t, sym, mk): (t, sym, mk) for t, (sym, mk) in todo}
        done = 0
        for fut in as_completed(futs):
            t, sym, mk = futs[fut]
            try:
                series[t] = fut.result()
            except Exception as e:  # noqa: BLE001
                print("  [warn] %s 拉取异常: %s" % (t, e))
                series[t] = {}
            cache["%s|%s" % (t, mk)] = {str(k): list(v) for k, v in series[t].items()}
            done += 1
            if done % 50 == 0:
                print("  %d/%d" % (done, len(todo)))

    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({"_scope": "%s~%s" % (dates[0], dates[-1]), "data": cache}, f)
        print("  K 线缓存已写 %s（%d 币）" % (cache_path, len(cache)))
    except Exception as e:  # noqa: BLE001
        print("  [warn] 缓存写入失败: %s" % e)

    print("网络统计: 直连 %d / 代理 %d / 全败 %d"
          % (NET.stats["direct"], NET.stats["proxy"], NET.stats["fail"]))

    # ---- 计算 ----
    updates, stats = [], {"filled": 0, "already": 0, "no_price": 0, "no_future": 0}
    no_price_rows = []
    for x in recs:
        anchor = int(datetime.strptime(x["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        s = series.get(x["token"]) or {}
        p0 = px(s, anchor)
        b0 = px(btc, anchor)
        rowset = {}
        if p0 is None or b0 is None:
            stats["no_price"] += 1
            no_price_rows.append((x["row"], x["date"], x["token"],
                                  "有K线" if s else "无K线"))
            continue
        for label, hrs, keys in HORIZONS:
            ts = anchor + hrs * HOUR_MS
            p1, b1 = px(s, ts), px(btc, ts)
            if p1 is None or b1 is None:
                stats["no_future"] += 1
                continue
            r, br = pct(p0, p1), pct(b0, b1)
            vals = {
                "p24": p1, "r24": fmt_pct(r), "br24": b1, "brp24": fmt_pct(br),
                "v24": fmt_pct(r - br if (r is not None and br is not None) else None),
                "p48": p1, "r48": fmt_pct(r), "brp48": fmt_pct(br),
                "v48": fmt_pct(r - br if (r is not None and br is not None) else None),
                "p7d": p1, "r7d": fmt_pct(r), "brp7d": fmt_pct(br),
                "v7d": fmt_pct(r - br if (r is not None and br is not None) else None),
            }
            for k in keys:
                if x["cells"].get(k) is None or str(x["cells"].get(k)).strip() == "":
                    rowset[COLS[k]] = round(vals[k], 8) if isinstance(vals[k], float) else vals[k]
        if rowset:
            orig = {c: x["cells"][k] for k, c in COLS.items()}
            updates.append({"row": x["row"], "date": x["date"], "token": x["token"],
                            "cells": rowset, "_orig": orig})
            stats["filled"] += len(rowset)
        else:
            stats["already"] += 1

    updates_path = os.path.join(args.out, "updates.json")
    with open(updates_path, "w", encoding="utf-8") as f:
        json.dump(updates, f, ensure_ascii=False, indent=1)

    print("")
    print("=== 结果 ===")
    print("  待写单元格 %d 个，涉及 %d 行" % (stats["filled"], len(updates)))
    print("  已填跳过 %d 行 | 无锚点价 %d 行 | 缺未来价 %d 次"
          % (stats["already"], stats["no_price"], stats["no_future"]))
    if no_price_rows:
        print("  无锚点价明细:")
        for row, d, t, why in no_price_rows[:12]:
            print("    r%s %s %-9s (%s)" % (row, d, t, why))
    print("  预览已存 %s" % updates_path)
    print("")
    print("前 5 行示例:")
    for u in updates[:5]:
        cells = {header[k]: v for k, v in sorted(u["cells"].items())}
        print("  r%s %s %-8s %s" % (u["row"], u["date"], u["token"], cells))

    if not args.write:
        print("")
        print("[dry-run] 未写入。确认无误后加 --write")
        return

    # ---- 写回 ----
    sh = _open_sheet()
    wks = sh.worksheet(args.tab)

    # 写入前先备份整表（可回滚）
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(args.out, "backup_%s.json" % stamp)
    live = wks.get_all_values()
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(live, f, ensure_ascii=False)
    print("已备份到 %s" % backup_path)

    # 线上真值兜底：只在线上确实为空时才写，避免盲写覆盖
    dropped, kept = 0, []
    for u in updates:
        rv = live[u["row"] - 1] if u["row"] - 1 < len(live) else []
        cells = {}
        for c, v in u["cells"].items():
            ci = int(c)
            cur = rv[ci].strip() if len(rv) > ci else ""
            if cur:
                dropped += 1
            else:
                cells[c] = v
        if cells:
            u["cells"] = cells
            kept.append(u)
    print("线上复查：已有值 → 跳过不写 %d 个单元格；线上确为空 → 实际待写 %d 个 / %d 行"
          % (dropped, sum(len(u["cells"]) for u in kept), len(kept)))
    updates = kept
    if not updates:
        print("线上结果列已全部有值，无需写入。")
        return

    # 按行合并成连续区间写入（逐单元格会到 5000+ 次）
    print("")
    print("写回 %d 行..." % len(updates))
    import gspread

    payload = []
    for u in updates:
        cols = sorted(u["cells"])
        lo, hi = cols[0], cols[-1]
        rv = live[u["row"] - 1] if u["row"] - 1 < len(live) else []
        span = []
        for c in range(lo, hi + 1):
            if c in u["cells"]:
                span.append(u["cells"][c])
            else:
                span.append(rv[c] if len(rv) > c else None)
        rng = (gspread.utils.rowcol_to_a1(u["row"], lo + 1) + ":"
               + gspread.utils.rowcol_to_a1(u["row"], hi + 1))
        payload.append({"range": rng, "values": [span]})

    for i in range(0, len(payload), 100):
        # 用 RAW 而不是 USER_ENTERED：表内既有的 `-3.37%` 等是**文本**，
        # 若用 USER_ENTERED 会被 Sheets 解析成百分比数值（底层值从文本变成小数），
        # 与其他行的类型不一致。RAW 能保持原有类型。
        wks.batch_update(payload[i:i + 100], value_input_option="RAW")
        print("  已写 %d/%d 行" % (min(i + 100, len(payload)), len(payload)))
    print("完成，共写入 %d 个行区间。" % len(payload))


if __name__ == "__main__":
    main()
