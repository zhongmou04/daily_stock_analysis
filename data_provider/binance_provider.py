"""
Binance 数据源（drop-in）——给每日股票分析 workflow 用

解决的问题：关注列表里的 BTC-USD 这类币代码，baostock 不认
（只认 9 位 A 股代码如 sh.600000），直接走币安公开行情接口拿 K 线，
再转成和现有分析代码一样的 DataFrame 格式，后面的分析逻辑不用改。

用法（不用发仓库也能先用）：
    1. 把本文件放到仓库里 data_provider 旁边，例如 data_provider/binance_provider.py
    2. 在原来调 baostock 拿数据的地方加一个分流：

        from binance_provider import get_history, is_crypto

        if is_crypto(code):
            df = get_history(code, start_date="2026-01-01", end_date="2026-11-10")
        else:
            df = ...  # 原来的 baostock 逻辑不变

    3. 代码映射：BTC-USD -> BTCUSDT，ETH-USD -> ETHUSDT，
       BTC/USDT、btcusdt、BTC 也都认；A 股 sh./sz. 开头的不走这里。

关键点（2026-11-11 实测）：
    - 不需要 API 密钥，公开行情接口。
    - 默认用 https://data-api.binance.vision（公开行情专用域名）。
      注意：本机实测 https://api.binance.com 返回 451（地区限制），
      所以不要把 api.binance.com 写死成唯一域名；本文件默认 vision，
      失败时才自动切到 api.binance.com。
    - GitHub Actions 上同样建议默认 vision 域名，最稳。

返回的 DataFrame 列（和 baostock query_history 常用列对齐）：
    date, code, open, high, low, close, preclose, volume, amount
    其中 date 是 'YYYY-MM-DD'，code 是你传进来的原始代码（如 BTC-USD），
    preclose 是上一日收盘价（首日为空），volume 是币的数量，
    amount 是计价货币成交额（quote volume）。
    如需中文列名，用 df.rename(columns=COLUMN_MAP_ZH) 即可，
    COLUMN_MAP_ZH 在下面已给出。

依赖：pandas、requests（GitHub Actions 的 Python 3.11 环境一般都有；
没有 requests 时会退回标准库 urllib，一样能跑）。
"""

from __future__ import annotations

import time

import pandas as pd

try:
    import requests
except ImportError:  # pragma: no cover - 极简环境退路
    requests = None

# 公开行情域名：vision 优先（无地区限制），api.binance.com 备用
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]

COLUMN_MAP_ZH = {
    "date": "日期",
    "code": "代码",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "preclose": "前收盘价",
    "volume": "成交量",
    "amount": "成交额",
}

# 常见币种简称 -> 币安交易对；不在表里的按“去掉 -USD/_USD// 后 + USDT”处理
_KNOWN = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
    "DOGE": "DOGEUSDT",
    "ADA": "ADAUSDT",
    "LTC": "LTCUSDT",
    "LINK": "LINKUSDT",
    "AVAX": "AVAXUSDT",
    "DOT": "DOTUSDT",
    "TRX": "TRXUSDT",
    "TON": "TONUSDT",
    "INIT": "INITUSDT",
}


def normalize_symbol(code: str) -> str:
    """BTC-USD / BTC/USDT / btcusdt / BTC -> BTCUSDT。"""
    s = str(code).strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("USDT"):
        return s
    if s.endswith("USD"):
        s = s[:-3]
    if s.endswith("USDC"):
        return s[:-4] + "USDT"
    return _KNOWN.get(s, s + "USDT")


def is_crypto(code: str) -> bool:
    """是不是币代码：不是 sh./sz. 开头的 A 股，且像个币种，就当币处理。"""
    s = str(code).strip().lower()
    if s.startswith(("sh.", "sz.", "bj.")):
        return False
    return True


def _get_json(url: str, params: dict, timeout: int = 15):
    if requests is not None:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    import json
    import urllib.parse
    import urllib.request

    full = url + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(full, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_binance_klines(
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
    interval: str = "1d",
    limit: int = 1000,
) -> pd.DataFrame:
    """拉币安 K 线，返回上面约定的列。start/end 用 'YYYY-MM-DD'。"""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_date:
        params["startTime"] = int(pd.Timestamp(start_date).timestamp() * 1000)
    if end_date:
        # 含 end_date 当天：取到次日 00:00 之前
        params["endTime"] = int(
            (pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp() * 1000
        ) - 1

    last_err: Exception | None = None
    klines = None
    for base in BASE_URLS:
        try:
            klines = _get_json(f"{base}/api/v3/klines", params)
            break
        except Exception as e:  # noqa: BLE001 - 换域名重试
            last_err = e
            time.sleep(0.5)
    if klines is None:
        raise RuntimeError(f"币安行情接口都连不上：{last_err}")

    if not klines:
        return pd.DataFrame(
            columns=["date", "code", "open", "high", "low", "close",
                     "preclose", "volume", "amount"]
        )

    df = pd.DataFrame(
        klines,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "amount", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore",
        ],
    )
    df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["code"] = symbol
    df["preclose"] = df["close"].shift(1)
    return df[["date", "code", "open", "high", "low", "close",
               "preclose", "volume", "amount"]]


def get_history(code: str, start_date: str | None = None,
                end_date: str | None = None) -> pd.DataFrame:
    """分流入口：传 BTC-USD 就走币安，返回的 code 列保留原始代码。"""
    symbol = normalize_symbol(code)
    df = fetch_binance_klines(symbol, start_date=start_date, end_date=end_date)
    df["code"] = str(code).strip().upper()
    return df


if __name__ == "__main__":
    # 自测：BTC-USD 最近 5 天
    for c in ["BTC-USD", "ETH-USD", "btcusdt"]:
        print(c, "->", normalize_symbol(c))
    df = get_history("BTC-USD", start_date="2026-11-05", end_date="2026-11-10")
    print(df.to_string(index=False))
    print("rows:", len(df), "| columns:", list(df.columns))