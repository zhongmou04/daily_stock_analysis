# -*- coding: utf-8 -*-
"""
===================================
BinanceFetcher - 加密货币数据源 (Priority 1)
===================================

数据来源：币安公开行情接口（Binance Public Market Data）
特点：免费、无需 API Key、现货日 K 线
优点：A 股数据源都不认 BTC-USD 这类币代码时，由它接手

关键策略：
1. 只接币代码：BTC-USD / BTCUSDT / BTC 等；非币代码直接抛
   DataFetchError，让 DataFetcherManager 切换到其他数据源
2. 默认走 https://data-api.binance.vision（公开行情专用域名，
   无地区限制）；失败时自动切 https://api.binance.com
   注意：api.binance.com 在部分地区返回 451，不要写死成唯一域名
3. 返回的 DataFrame 复用 BaseFetcher 的标准列，后面的分析逻辑不用改
"""

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from typing import Optional

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import (
    BaseFetcher,
    DataFetchError,
    STANDARD_COLUMNS,
)

try:
    import requests
except ImportError:  # pragma: no cover - 极简环境退路
    requests = None

logger = logging.getLogger(__name__)

# 公开行情域名：vision 优先（无地区限制），api.binance.com 备用
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]

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

# 币种中文名（用于分析报告展示）
_CN_NAMES = {
    "BTC": "比特币",
    "ETH": "以太坊",
    "SOL": "索拉纳",
    "BNB": "币安币",
    "XRP": "瑞波币",
    "DOGE": "狗狗币",
    "ADA": "艾达币",
    "LTC": "莱特币",
    "LINK": "Chainlink",
    "AVAX": "雪崩币",
    "DOT": "波卡币",
    "TRX": "波场币",
    "TON": "Toncoin",
    "INIT": "Initia",
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
    """
    判断代码是否为币代码。

    认三种形式：
    - 带币后缀：BTC-USD、BTC/USDT、BTCUSDT、BTCUSDC
    - 常见币简称：BTC、ETH、SOL...
    其他一律不当币处理（A 股 6 位数字、美股 1-5 字母都返回 False，
    让其他数据源去接）。
    """
    s = str(code).strip().upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith(("USDT", "USDC", "USD")):
        return True
    return s in _KNOWN


class BinanceFetcher(BaseFetcher):
    """
    Binance 数据源实现

    优先级：1（币代码只此一家能接；非币代码不发请求直接让路）
    数据来源：币安公开 K 线接口 /api/v3/klines

    关键策略：
    - 先判断 is_crypto，非币代码直接抛 DataFetchError，不产生网络请求
    - 双域名自动切换，失败后指数退避重试
    - pct_chg 由收盘价算出（Binance K 线本身不带涨跌幅）
    """

    name = "BinanceFetcher"
    priority = int(os.getenv("BINANCE_PRIORITY", "1"))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(OSError),
        before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get_json(self, url: str, params: dict, timeout: int = 15):
        """发一次 GET 请求并返回 JSON（requests 不可用时退回 urllib）。"""
        if requests is not None:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        full = url + "?" + urllib.parse.urlencode(params)
        with urllib.request.urlopen(full, timeout=timeout) as resp:
            return json.loads(resp.read())

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Binance 获取原始 K 线数据

        流程：
        1. 检查是否为币代码（不是就抛异常，让管理器切换其他数据源）
        2. 转换代码为币安交易对格式
        3. 双域名依次请求 K 线接口
        4. 将结果转换为 DataFrame
        """
        # 非币代码不支持，抛出异常让 DataFetcherManager 切换到其他数据源
        if not is_crypto(stock_code):
            raise DataFetchError(f"BinanceFetcher 不支持非币代码 {stock_code}")

        # 转换代码格式
        symbol = normalize_symbol(stock_code)

        params = {"symbol": symbol, "interval": "1d", "limit": 1000}
        if start_date:
            params["startTime"] = int(pd.Timestamp(start_date).timestamp() * 1000)
        if end_date:
            # 含 end_date 当天：取到次日 00:00 之前
            params["endTime"] = int(
                (pd.Timestamp(end_date) + pd.Timedelta(days=1)).timestamp() * 1000
            ) - 1

        logger.debug(f"调用 Binance klines({symbol}, {start_date}, {end_date})")

        last_err: Exception | None = None
        klines = None
        for base in BASE_URLS:
            try:
                klines = self._get_json(f"{base}/api/v3/klines", params)
                break
            except Exception as e:  # noqa: BLE001 - 换域名重试
                last_err = e
                logger.warning(f"币安 {base} 请求失败，尝试备用域名: {e}")
                time.sleep(0.5)

        if klines is None:
            raise DataFetchError(f"Binance 获取数据失败: {last_err}")

        if not klines:
            raise DataFetchError(f"Binance 未查询到 {stock_code} 的数据")

        df = pd.DataFrame(
            klines,
            columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "amount", "trades", "taker_buy_base",
                "taker_buy_quote", "ignore",
            ],
        )
        df["date"] = pd.to_datetime(df["open_time"], unit="ms").dt.strftime("%Y-%m-%d")

        return df

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        """
        标准化 Binance 数据

        Binance K 线原始列：
        open_time, open, high, low, close, volume, close_time,
        amount（计价货币成交额）, trades, ...

        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg
        """
        df = df.copy()

        # 数值类型转换（Binance 返回的都是字符串）
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 涨跌幅由收盘价算出（Binance K 线本身不带 pctChg）
        df['pct_chg'] = df['close'].pct_change() * 100

        # 添加股票代码列（保留用户写进 STOCK_LIST 的原始代码，如 BTC-USD）
        df['code'] = stock_code

        # 只保留需要的列
        keep_cols = ['code'] + STANDARD_COLUMNS
        existing_cols = [col for col in keep_cols if col in df.columns]
        df = df[existing_cols]

        return df

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        获取币种名称

        本地映射，无需联网。

        Args:
            stock_code: 币代码，如 'BTC-USD'

        Returns:
            币种名称，如 '比特币 (BTC/USDT)'
        """
        symbol = normalize_symbol(stock_code)
        base = symbol[:-4] if symbol.endswith("USDT") else symbol
        cn = _CN_NAMES.get(base)
        if cn:
            return f"{cn} ({base}/USDT)"
        return f"{base}/USDT"


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)

    fetcher = BinanceFetcher()

    try:
        # 测试历史数据
        df = fetcher.get_daily_data('BTC-USD', start_date='2026-11-05', end_date='2026-11-10')
        print(f"获取成功，共 {len(df)} 条数据")
        print(df.tail())

        # 测试币种名称
        name = fetcher.get_stock_name('BTC-USD')
        print(f"币种名称: {name}")

    except Exception as e:
        print(f"获取失败: {e}")