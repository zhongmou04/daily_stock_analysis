#!/usr/bin/env python3
"""重大经济数据发布提醒（第一层）。

每次运行：拉取本周经济日历 -> 找出 20~35 分钟后发布的「高影响·美国」事件
-> 通过 Telegram 推送提醒，并提示发布前后暂不开仓。

只用 Python 标准库，无需 pip 安装。
需要环境变量：TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID
可选环境变量：
  EVENT_ALERT_CURRENCIES   关注的国家/货币，默认 USD（多个用逗号分隔）
  EVENT_ALERT_IMPACTS      关注的影响级别，默认 High（可写 High,Medium）
  EVENT_ALERT_LEAD_MIN     提前多少分钟开始提醒，默认 35
  EVENT_ALERT_MIN_MIN      最晚提前多少分钟，默认 20
  EVENT_ALERT_DRY_RUN      设为 1 时只打印，不发 Telegram
"""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
BEIJING = timezone(timedelta(hours=8))


def fetch_calendar(url: str = CALENDAR_URL) -> list[dict]:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _csv_env(name: str, default: str) -> set[str]:
    return {x.strip().lower() for x in os.getenv(name, default).split(",") if x.strip()}


def pick_events(events: list[dict], now: datetime) -> list[dict]:
    countries = _csv_env("EVENT_ALERT_CURRENCIES", "USD")
    impacts = _csv_env("EVENT_ALERT_IMPACTS", "High")
    lead = int(os.getenv("EVENT_ALERT_LEAD_MIN", "35"))
    least = int(os.getenv("EVENT_ALERT_MIN_MIN", "20"))
    picked = []
    for ev in events:
        try:
            when = datetime.fromisoformat(ev["date"])
        except (KeyError, ValueError, TypeError):
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        if str(ev.get("country", "")).lower() not in countries:
            continue
        if str(ev.get("impact", "")).lower() not in impacts:
            continue
        minutes = (when - now).total_seconds() / 60
        if least <= minutes < lead:
            picked.append({**ev, "_when": when, "_minutes": round(minutes)})
    return sorted(picked, key=lambda e: e["_when"])


def format_message(events: list[dict]) -> str:
    lines = ["⚠️ 重大数据即将发布"]
    for ev in events:
        t = ev["_when"].astimezone(BEIJING).strftime("%H:%M")
        lines.append(f"\n• 北京时间 {t}（约 {ev['_minutes']} 分钟后）")
        lines.append(f"  {ev.get('title', '未知事件')}")
        extra = []
        if ev.get("forecast"):
            extra.append(f"预期 {ev['forecast']}")
        if ev.get("previous"):
            extra.append(f"前值 {ev['previous']}")
        if extra:
            lines.append("  " + "，".join(extra))
    lines.append("\n🚫 发布前后约 30 分钟波动剧烈、容易被扫损，建议暂不开新仓。")
    return "\n".join(lines)


def send_telegram(text: str) -> None:
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=20) as r:
        r.read()


def main() -> int:
    now = datetime.now(timezone.utc)
    try:
        events = fetch_calendar()
    except Exception as exc:  # 网络问题不应让任务报红
        print(f"获取经济日历失败：{exc}")
        return 0
    picked = pick_events(events, now)
    if not picked:
        print("当前没有需要提醒的事件")
        return 0
    text = format_message(picked)
    print(text)
    if os.getenv("EVENT_ALERT_DRY_RUN") == "1":
        return 0
    send_telegram(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())