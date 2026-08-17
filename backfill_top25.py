#!/usr/bin/env python3
"""
一次性回填每日 OI/价格历史到 Firebase。

用途：direction B 的「OI持续升温（按增长）」维度需要过去几天的每日 OI/价格数据。
本脚本把最近 5 天的 data（每币每日 oi 合约数 + 收盘价）回填进 Firebase，
让检测能立刻有历史数据可用，而不是再等 3 天。

（早期版本叫 backfill_top25，回填的是 OI Top25 排名；现已改为回填每日 data。）

运行方式（在 GitHub Actions 里用 workflow_dispatch 手动触发）：
    python backfill_top25.py

依赖环境变量：
    FIREBASE_CREDENTIALS: service account JSON 字符串
"""
import os
import json
import time
from datetime import datetime, timedelta, timezone

import requests
import firebase_admin
from firebase_admin import credentials, firestore

FAPI = "https://fapi.binance.com"
VOLUME_THRESHOLD = 5_000_000   # 与 warmup 模式一致：24h成交额 > $5M
BACKFILL_DAYS = 5    # 回填最近 5 天
COLLECTION = "oi_warmup_tracker"
DOC_ID = "daily_snapshots"


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


def get_top_symbols():
    tickers = request_json("/fapi/v1/ticker/24hr")
    if not tickers or not isinstance(tickers, list):
        return []
    usdt = [
        t for t in tickers
        if t.get('symbol', '').endswith('USDT') and float(t.get('quoteVolume', 0)) > VOLUME_THRESHOLD
    ]
    usdt.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
    return [t['symbol'] for t in usdt]


def build_backfill_days():
    """返回 {beijing_date: {symbol: {'oi': contracts, 'price': close, 'ls':..., 'cvd':..., 'fr':...}}}"""
    symbols = get_top_symbols()
    if not symbols:
        raise RuntimeError("无法获取 ticker 列表（可能 IP 受限）")

    # beijing_date -> {symbol: {'oi': contracts, 'price': close}}
    day_map = {}

    for i, sym in enumerate(symbols):
        oi_hist = request_json("/futures/data/openInterestHist", {"symbol": sym, "period": "1d", "limit": BACKFILL_DAYS})
        klines = request_json("/fapi/v1/klines", {"symbol": sym, "interval": "1d", "limit": BACKFILL_DAYS})

        oi_by_ts = {}
        if isinstance(oi_hist, list):
            for r in oi_hist:
                oi_by_ts[r['timestamp']] = r

        k_by_ts = {}
        if isinstance(klines, list):
            for k in klines:
                k_by_ts[k[0]] = k

        for ts, r in oi_by_ts.items():
            bj = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) + timedelta(hours=8)
            bj_date = bj.strftime('%Y-%m-%d')
            oi_contracts = float(r.get('sumOpenInterest') or 0)
            price = float(k_by_ts[ts][4]) if ts in k_by_ts else 0.0
            day_map.setdefault(bj_date, {})[sym] = {'oi': oi_contracts, 'price': price}

        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{len(symbols)} ...")
        time.sleep(0.03)

    result = {}
    for bj_date, sym_data in day_map.items():
        data = {
            sym: {
                'oi': v['oi'],
                'price': v['price'],
                'ls': 1.0,
                'cvd': 0,
                'fr': 0,
            }
            for sym, v in sym_data.items()
        }
        result[bj_date] = data

    return result


def main():
    creds_json = os.environ.get('FIREBASE_CREDENTIALS')
    if not creds_json:
        raise ValueError('缺少环境变量 FIREBASE_CREDENTIALS')

    cred_dict = json.loads(creds_json)
    cred = credentials.Certificate(cred_dict)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    doc_ref = db.collection(COLLECTION).document(DOC_ID)

    print("回填每日 OI/价格历史 ...")
    backfill_days = build_backfill_days()
    print(f"回填到 {len(backfill_days)} 个自然日")

    doc = doc_ref.get()
    snapshots = doc.to_dict().get('snapshots', []) if doc.exists else []
    by_date = {s.get('date'): s for s in snapshots}

    for bj_date, data in backfill_days.items():
        if bj_date in by_date:
            s = by_date[bj_date]
            if not s.get('data'):
                s['data'] = data      # 若缺 data 则补
        else:
            by_date[bj_date] = {'date': bj_date, 'data': data}

    snapshots = sorted(by_date.values(), key=lambda x: x.get('date', ''))
    snapshots = snapshots[-BACKFILL_DAYS:]
    doc_ref.set({'snapshots': snapshots})

    print(f"完成。当前共 {len(snapshots)} 个快照：")
    for s in snapshots:
        n = len(s.get('data', {}))
        print(f"  {s.get('date')}: data {n} 个币")


if __name__ == '__main__':
    main()
