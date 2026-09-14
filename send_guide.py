# -*- coding: utf-8 -*-
"""一次性工具：把 radar 指标说明书发到 Telegram（供临时 workflow 调用）。

发完即删——这不是常驻功能，只是为了让用户能置顶一份说明。
"""
import json
import os
import sys
import urllib.error
import urllib.request

TEXT = """📖 <b>radar 指标说明书</b>（置顶版）
<i>更新 2026-09-14 · 版本 40c6c33</i>

雷达每 2 小时整点自动推送一次（北京时间偶数点）。标题里的时间是扫描时刻；「数据完整度 X/Y」= 本次取到数据的币数 ÷ 有效 USDT 永续总数，低于 90% 会附警告。

━━━━━━━━━━━━
<b>一、一行数字怎么读</b>
<code>杠杆位:8%🟢 价位:84% | DD-37% | OI:24h +3.2% (2h +0.4%) | 价:+1.1% | LS:1.24</code>

<b>杠杆位</b>：当前 OI 存量在最近 <b>30 天（日线）</b>里的分位。0% = 该窗口最低（杠杆已被挤干），100% = 最高（杠杆堆满）。≤25% 打 🟢。日线不足 14 根显示 <code>杠杆位:?</code>。
<b>价位</b>：(现价 − 365日最低) ÷ (365日最高 − 365日最低)。0% = 一年最低，100% = 一年最高，≤25% 打 🟢。用 <b>365 根日线</b>。只做展示，不当筛选条件。
<b>DD</b>：距一年高点的回撤%（负数）。DD-37% = 比一年最高点低 37%。
<b>OI 24h / (2h)</b>：未平仓合约量的 <b>24 小时</b> / <b>2 小时</b>变化率（底层是 1 小时 K 线取 25 个点）。正 = 有新资金进场堆杠杆。
<b>价</b>：滚动 <b>24 小时</b>价格涨跌。
<b>LS</b>：大户多空持仓比，取最近一根 <b>2h</b>。&gt;1 = 大户净多头，&lt;1 = 净空头。

━━━━━━━━━━━━
<b>二、四个栏目分别怎么筛出来的</b>

🟢 <b>低位区 · OI日级累积</b>
<b>怎么筛</b>：杠杆位≤25% <b>且</b> OI 24h增&gt;1% <b>且</b> |24h涨跌|≤5%
<b>什么逻辑</b>：杠杆水位在近月低位 + 钱在进来 + 价格还没动 = 最接近「有人悄悄建仓」的形状。
<b>怎么用</b>：优先级最高的一栏。⚠️ 但证据偏弱（子样本仅 n=17），别当铁律。

💎 <b>横盘 + 大户多</b>
<b>怎么筛</b>：−2% &lt; 24h涨跌 &lt; +5% <b>且</b> OI 24h增&gt;1.5% <b>且</b> LS&gt;1.2
<b>什么逻辑</b>：价格横着、OI 在涨、大户偏多。
<b>⚠️ 关键</b>：这条<b>不含杠杆位条件</b>，所以会出现杠杆位 90%+ 的标的。它是「横盘」信号，不是「低位」信号。
（2026-09-14 由「低位埋伏」改名，原名会与底部「不属于低位」的说法自相矛盾。）

📈 <b>2h OI 爆增榜</b>（脉冲，不是累积）
<b>怎么筛</b>：全市场 OI <b>2 小时</b>变化 Top 5，不限价格。
<b>什么逻辑</b>：短时脉冲。回测显示<b>单独看是负 alpha</b>（+0.48%，低于基线 +0.88%）。
<b>怎么用</b>：看到就好，别追。它不参与选币、不写库。

☢️ <b>极端费率</b>
<b>怎么筛</b>：资金费率最低 3 名（负）+ 最高 3 名（正）。
<b>什么逻辑</b>：负得越极端 = 空头越拥挤（存在轧空可能）；正得越极端 = 多头越拥挤（追高成本高）。

━━━━━━━━━━━━
<b>三、底部两行提示</b>
⚠️ <b>爆增榜里有 N 个杠杆位不在低位</b>（会列出币种和分位）→ 这些是右侧追高，别当低位机会。
📉 <b>本次算过杠杆位的候选币中 N 个 ≥90%</b> → 只是反向提示。两个前提：① 该计数只覆盖「算过杠杆位的候选币」，不是全市场；② 实测 90~100% 那档反而 +2.38%，所以「高分位一定差」不成立。

━━━━━━━━━━━━
<b>四、一句话总结</b>
低位区看「便宜 + 钱进来」；横盘+大户多看「不涨 + 大户多」；爆增榜当噪音滤镜；费率看拥挤度。所有分位都是<b>相对位置</b>不是绝对值，「越低越好」只是方向性偏好，不是已验证结论。

<b>数据源</b>：币安合约公开 API（免 key）。"""


def main() -> None:
    tok = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        sys.exit("缺少 TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")

    payload = json.dumps({
        "chat_id": chat,
        "text": TEXT,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{tok}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", "replace")
        print("SEND_OK", body[:400])
    except urllib.error.HTTPError as e:
        print("SEND_FAIL_HTTP", e.code, e.read().decode("utf-8", "replace")[:900])
        raise
    except Exception as e:
        print("SEND_FAIL", repr(e))
        raise


if __name__ == "__main__":
    main()
