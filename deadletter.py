# -*- coding: utf-8 -*-
"""死信告警：检查 radar 最近 N 小时是否有成功运行，没有就发 Telegram 告警。

背景：GitHub 的 cron 本身会丢 run（实测约 48%），且 radar 跑在 Azure 美国 runner 上、
直连币安常被 451/429 拦下掉进公共代理。这两件事都可能导致「时间过去了、没推送」。
本脚本独立于 radar 本体，专门盯这件事：只要最近 DEADLETTER_WINDOW_HOURS 小时内
「Binance Monitor Task」没有任何一次 success，就发一条告警。

依赖（全部由 GitHub Actions 注入，无需额外 secrets）：
- GITHUB_TOKEN / GITHUB_REPOSITORY：查 runs 历史
- TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID：发告警
"""
import os
import sys
from datetime import datetime, timedelta, timezone

import requests

TOKEN = os.environ.get("GITHUB_TOKEN")
REPO = os.environ.get("GITHUB_REPOSITORY")
BOT = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT = os.environ.get("TELEGRAM_CHAT_ID")
HOURS = int(os.environ.get("DEADLETTER_WINDOW_HOURS", "6"))
WORKFLOW_NAME = "Binance Monitor Task"


def fetch_runs():
    url = f"https://api.github.com/repos/{REPO}/actions/runs?per_page=30"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.json()["workflow_runs"]


def parse_ts(s):
    # GitHub 返回形如 "2026-09-13T12:34:56Z"，Python 3.9 的 fromisoformat 不支持 Z
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def send_alert(text):
    url = f"https://api.telegram.org/bot{BOT}/sendMessage"
    r = requests.post(url, json={"chat_id": CHAT, "text": text}, timeout=20)
    print(f"alert sent: {r.status_code}")


def main():
    runs = fetch_runs()
    monitor = [x for x in runs if x.get("name") == WORKFLOW_NAME]

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=HOURS)
    recent = [x for x in monitor if parse_ts(x["created_at"]) >= cutoff]

    success = [x for x in recent if x.get("conclusion") == "success"]

    if success:
        print(f"OK: {len(success)} success runs in last {HOURS}h")
        return

    # 没有成功 → 告警，区分「完全没跑」和「跑了但全失败」
    latest = monitor[0] if monitor else None
    if not recent:
        msg = (
            f"[radar 死信告警] 最近 {HOURS} 小时没有任何一次 monitor 运行，"
            f"GitHub cron 可能把调度全丢了。"
        )
    else:
        states = ", ".join(x.get("conclusion") or "?" for x in recent)
        msg = (
            f"[radar 死信告警] 最近 {HOURS} 小时有 {len(recent)} 次运行但全部失败/取消"
            f"（{states}），可能是代理全死或代码异常。"
        )
    if latest:
        msg += f"\n最近一次: {latest['created_at']} ({latest.get('conclusion')})"
    msg += "\n(本条来自独立死信检查，不代表 radar 本体恢复)"
    send_alert(msg)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"deadletter check failed: {e}", file=sys.stderr)
        sys.exit(1)
