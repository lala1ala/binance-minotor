# -*- coding: utf-8 -*-
"""排除「传统资产」合约：股票上链 / 商品 / 外汇 / 盘前。

判据取自币安 fapi exchangeInfo 的元数据，不维护人工清单：
  · underlyingSubType 含 'TradFi'
      → EQUITY(美股) / HK_EQUITY / KR_EQUITY / CN_EQUITY / COMMODITY / FX / PREMARKET
      实测 905 个合约里 197 个命中；当前 >$10M 扫描池 182 个里有 34 个
  · baseAsset 属 GOLD_TOKENS → PAXG / XAUT
      黄金代币。币安把它们标成 COIN+RWA 而不是 TradFi，但同属传统资产
      （与已被 TradFi 覆盖的 XAU 并列），故一并排除。

★ 为什么不按 'RWA' 一刀切：该标签还包含加密原生项目（实测 CFGUSDT = RWA+Crypto），
  一刀切会误伤。
★ 为什么带静态兜底：exchangeInfo 取数失败时若退化成「不过滤」，股票会重新混进
  榜单并写进账本 —— 宁可漏扫，也不要脏数据。兜底清单见同目录 tradfi_symbols.json。
  币安上新传统资产合约后，在本目录跑 `python tradfi_filter.py` 重新生成即可。
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TRADFI_TAG = "TradFi"
GOLD_TOKENS = {"PAXG", "XAUT"}
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
FALLBACK_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "tradfi_symbols.json"
)

_cache = None


def _pick(item):
    """该合约是否属于要排除的传统资产"""
    sym = item.get("symbol") or ""
    if not sym.endswith("USDT"):
        return False
    if TRADFI_TAG in (item.get("underlyingSubType") or []):
        return True
    return (item.get("baseAsset") or "").upper() in GOLD_TOKENS


def from_exchange_info(info):
    """从 exchangeInfo 响应里挑出要排除的 USDT 合约"""
    if not isinstance(info, dict):
        return set()
    return {s["symbol"] for s in (info.get("symbols") or []) if _pick(s)}


def _load_fallback():
    try:
        with open(FALLBACK_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f).get("symbols") or [])
    except Exception as e:  # noqa: BLE001
        logger.warning("传统资产兜底清单读取失败(%s)，本次不做排除", e)
        return set()


def excluded_symbols(fetcher=None, refresh=False):
    """返回需排除的 USDT 合约集合（模块级缓存，一次运行只取一次）。

    fetcher(url, timeout=...) -> 解析好的 JSON，通常传请求器的 request_with_retry
    """
    global _cache
    if _cache is not None and not refresh:
        return _cache

    got = set()
    if fetcher is not None:
        try:
            got = from_exchange_info(fetcher(EXCHANGE_INFO_URL, timeout=20))
        except Exception as e:  # noqa: BLE001
            logger.warning("传统资产在线判据获取异常: %s", e)

    if got:
        _cache = got
        logger.info("传统资产排除表：在线获取 %d 个合约", len(got))
    else:
        _cache = _load_fallback()
        logger.warning("传统资产排除表：在线获取为空 → 改用静态兜底 %d 个合约", len(_cache))
    return _cache


def is_excluded(symbol, fetcher=None):
    return (symbol or "") in excluded_symbols(fetcher)


def _dump():
    """重新生成 tradfi_symbols.json（币安上新传统资产合约后手动跑一次）"""
    import requests

    r = requests.get(EXCHANGE_INFO_URL, timeout=30)
    r.raise_for_status()
    syms = sorted(from_exchange_info(r.json()))
    with open(FALLBACK_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": EXCHANGE_INFO_URL,
                "count": len(syms),
                "symbols": syms,
            },
            f,
            ensure_ascii=False,
            indent=1,
        )
    print("已写入 %s：%d 个合约" % (FALLBACK_PATH, len(syms)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _dump()
