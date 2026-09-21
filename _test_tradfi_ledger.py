# -*- coding: utf-8 -*-
"""不碰线上表的单元测试：在线取数路径 + 确认次数累加逻辑"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests
import tradfi_filter as tf
import ledger_writer as lw

print("=== 1. 在线取数路径（模拟 radar 的 request_with_retry）===")


def fake_fetcher(url, timeout=20):
    return requests.get(url, timeout=timeout).json()


s = tf.excluded_symbols(fake_fetcher, refresh=True)
print("  在线命中 %d 个；XAGUSDT=%s BTCUSDT=%s" % (len(s), "XAGUSDT" in s, "BTCUSDT" in s))

print()
print("=== 2. 确认次数累加（假表，不写线上）===")


class FakeWS:
    def __init__(self, existing):
        self.existing = existing
        self.appended = []
        self.updates = []

    def get_all_values(self):
        return self.existing

    def update_cell(self, r, c, v):
        self.updates.append(("cell", "R%dC%d" % (r, c), v))

    def append_rows(self, rows, **kw):
        self.appended.extend(rows)

    def batch_update(self, data, **kw):
        for d in data:
            self.updates.append(("batch", d["range"], d["values"][0][0]))


# 表头只有 23 列 → 应触发 X1 表头补写
hdr = ["日期", "Token", "信号来源", "OI分位", "OI变化%", "Price变化%", "方向感知",
       "共振确认", "入场价", "BTC入场价", "24h价格", "24h%", "BTC 24h价", "BTC 24h%",
       "vs BTC 24h", "48h价格", "48h%", "BTC 48h%", "vs BTC 48h", "7d价格", "7d%",
       "BTC 7d%", "vs BTC 7d"]
row_btc = ["2026-09-21", "BTC"] + [""] * 21 + ["1"]      # 当天已存在，已确认 1 次（第 24 格=X 列）
row_eth = ["2026-09-21", "ETH"] + [""] * 21 + ["3"]      # 当天已存在，已确认 3 次

fake = FakeWS([hdr, row_btc, row_eth])
lw._sheet = lambda: fake          # 打桩，绝不碰线上

rows_in = [
    {"symbol": "BTCUSDT", "price": 81184, "price_chg": -1.2, "oi_chg_1d": 1.8, "oi_pos": 0.20},
    {"symbol": "ETHUSDT", "price": 3120, "price_chg": 0.9, "oi_chg_1d": 3.3, "oi_pos": 0.05},
    {"symbol": "SOLUSDT", "price": 190, "price_chg": 0.3, "oi_chg_1d": 2.2, "oi_pos": 0.11},   # 新
    {"symbol": "XAGUSDT", "price": 48, "price_chg": 0.4, "oi_chg_1d": 2.1, "oi_pos": 0.12},   # 传统资产，应被挡
]
n, detail = lw.write_low_zone(rows_in, [{"symbol": "BTCUSDT", "price": 81184}])
print("  返回值:", n, "|", detail)
print("  追加行:", [(r[1], r[lw.X_COL]) for r in fake.appended])
print("  更新项:", fake.updates)
print()
ok = True
ok &= (n == 1)
ok &= ([r[1] for r in fake.appended] == ["SOL"])
ok &= ((("cell", "R1C24", "扫描确认次数") in fake.updates))
ok &= ((("batch", "X2", "2") in fake.updates))    # BTC 1 → 2
ok &= ((("batch", "X3", "4") in fake.updates))    # ETH 3 → 4
ok &= all(r[1] != "XAG" for r in fake.appended)
print("  断言结果:", "全部通过" if ok else "**有失败**")
