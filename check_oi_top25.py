#!/usr/bin/env python3
"""
核对 Binance 永续 OI Top25 排名 + 目标币种 5 日 OI/价格趋势。

用法:
    python check_oi_top25.py [SYMBOL...]
    不传参数默认检查 LINKUSDT JTOUSDT ETHFIUSDT

说明:
    - Top25 是在「24h 成交额前 N 个 USDT 永续对」这个可交易宇宙内、按 OI 降序排名的前 25 名。
    - 用于核对某币种是否属于 OI Top25，以及它近 5 日的 OI/价格是否同向温和上涨。
"""
import sys
import time
from datetime import datetime

import requests

FAPI = "https://fapi.binance.com"
UNIVERSE_SIZE = 100  # 按 24h 成交额取前 N 个 USDT 永续对做 OI 排名


def get_public_proxies():
    try:
        url = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return [{"http": f"http://{p}", "https": f"http://{p}"} for p in resp.text.splitlines()[:30]]
    except Exception:
        pass
    return []


def request_json(path, params=None):
    url = FAPI + path
    try:
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and ('code' in data or 'msg' in data):
                if 'restricted' in str(data.get('msg', '')):
                    raise ValueError('IP Restricted')
            return data
    except Exception:
        pass

    proxies = get_public_proxies()
    for p in proxies[:10]:
        try:
            r = requests.get(url, params=params, proxies=p, timeout=8)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and 'code' in data:
                    continue
                return data
        except Exception:
            continue
    return None


def get_usdt_symbols():
    tickers = request_json("/fapi/v1/ticker/24hr")
    if not tickers or not isinstance(tickers, list):
        return []
    usdt = [t for t in tickers if t.get('symbol', '').endswith('USDT')]
    usdt.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
    return usdt[:UNIVERSE_SIZE]


def get_oi(symbol):
    data = request_json("/fapi/v1/openInterest", {"symbol": symbol})
    if data and 'openInterest' in data:
        return float(data['openInterest'])
    return 0.0


def get_daily_history(symbol, days=6):
    oi_hist = request_json("/futures/data/openInterestHist", {"symbol": symbol, "period": "1d", "limit": days})
    klines = request_json("/fapi/v1/klines", {"symbol": symbol, "interval": "1d", "limit": days})

    oi_rows = []
    if isinstance(oi_hist, list):
        for r in oi_hist:
            ts = datetime.fromtimestamp(r['timestamp'] / 1000).strftime('%m-%d')
            oi_rows.append((ts, float(r['sumOpenInterest'])))

    k_rows = []
    if isinstance(klines, list):
        for k in klines:
            ts = datetime.fromtimestamp(k[0] / 1000).strftime('%m-%d')
            k_rows.append((ts, float(k[4])))  # close

    return oi_rows, k_rows


def main():
    targets = [s.upper() for s in sys.argv[1:]] or ['LINKUSDT', 'JTOUSDT', 'ETHFIUSDT']
    print(f"扫描前 {UNIVERSE_SIZE} 个 USDT 永续对的 OI ...")

    symbols = get_usdt_symbols()
    if not symbols:
        print("无法获取 ticker 列表（可能 IP 受限）")
        return

    oi_map = {}
    for i, s in enumerate(symbols):
        oi = get_oi(s['symbol'])
        price = float(s.get('lastPrice', 0))
        oi_map[s['symbol']] = oi * price  # 用 USD 名义价值排名，避免低价币按张数霸榜
        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{len(symbols)} ...")
        time.sleep(0.05)

    ranked = sorted(oi_map.items(), key=lambda x: x[1], reverse=True)

    print("\n===== 当前 OI Top 25 (按 USD 名义价值) =====")
    for i, (sym, oi) in enumerate(ranked[:25], 1):
        mark = "  <<< 目标" if sym in targets else ""
        print(f"{i:>2}. {sym:<16} OI=${oi:,.0f}{mark}")

    print("\n===== 目标币种 OI 排名 + 5日趋势 =====")
    for t in targets:
        rank = next((i for i, (sym, _) in enumerate(ranked, 1) if sym == t), None)
        oi_rows, k_rows = get_daily_history(t)

        print(f"\n[{t}] 当前 OI 排名: #{rank if rank else '未进前'}{UNIVERSE_SIZE}")
        if oi_rows:
            print("  每日 OI:")
            for ts, v in oi_rows:
                print(f"    {ts}: {v:,.0f}")
        if k_rows:
            print("  每日收盘:")
            for ts, v in k_rows:
                print(f"    {ts}: {v:,}")


if __name__ == '__main__':
    main()
