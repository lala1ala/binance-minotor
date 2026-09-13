import os
import sys
import json
import time
import logging
import threading
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from dataclasses import dataclass, asdict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== 配置 ====================
class Config:
    def __init__(self):
        # 从环境变量获取密钥
        self.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        self.firebase_creds_json = os.environ.get("FIREBASE_CREDENTIALS")
        
        # 验证配置
        if not all([self.bot_token, self.chat_id, self.firebase_creds_json]):
            raise ValueError("缺少必要的环境变量: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, FIREBASE_CREDENTIALS")

        self.report_cycle = 4  # 4次报告(约2小时)为一个周期
        self.collection_name = "binance_monitor"

# ==================== 数据结构 ====================
@dataclass
class CoinData:
    symbol: str
    ls_value: float
    section: str
    extra_info: str = ""

# ==================== Firebase 管理 ====================
class FirebaseManager:
    def __init__(self, creds_json):
        if not firebase_admin._apps:
            cred_dict = json.loads(creds_json)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.collection = self.db.collection('binance_monitor')

    def get_current_cycle(self) -> List[Dict]:
        """获取当前周期的报告列表"""
        doc = self.collection.document('state').get()
        if doc.exists:
            data = doc.to_dict()
            return data.get('current_cycle', [])
        return []

    def add_report_to_cycle(self, report: Dict):
        """添加报告到当前周期"""
        doc_ref = self.collection.document('state')
        # 使用 array_union 添加原子性 (或者直接读-改-写，这里读-改-写更可控)
        current = self.get_current_cycle()
        current.append(report)
        doc_ref.set({'current_cycle': current}, merge=True)
        return len(current)

    def reset_cycle(self):
        """重置周期"""
        doc_ref = self.collection.document('state')
        doc_ref.set({'current_cycle': []}, merge=True)
        # 可选：归档历史数据

# ==================== OI 监控核心逻辑 ====================
class OIMonitor:
    # ---- 网络策略参数 ----
    # 原实现最坏情况：5s 直连 + 10 个代理 × 5s = 55s / 请求，乘以约 600 次请求 ≈ 9 小时
    DIRECT_TIMEOUT = 8         # 直连超时（秒）
    PROXY_TIMEOUT = 6          # 单个代理超时（秒）
    MAX_PROXY_RETRIES = 3      # 单请求最多试几个代理（原来是 10）
    MAX_WORKERS = 8            # 并发取数线程数
    SCAN_BUDGET_SECONDS = 600  # 单次扫描总时间预算（10 分钟），到点就用已有数据出报告
    TELEGRAM_MAX_LEN = 4000    # Telegram 单条消息上限，超出自动分片

    # ---- 低位闸门参数（来自「买得便宜」的核心口径）----
    # 一年低点分位 pos = (现价 − 365日最低) / (365日最高 − 365日最低)
    #   pos → 0 = 贴近一年最低（便宜）；pos → 1 = 贴近一年最高（贵）
    YEAR_LOW_POS_MAX = 0.25    # pos <= 0.25 视为「低位区」
    YEAR_DD_MIN = -60.0        # 距一年高点回撤 <= -60% 作为二次确认（更严格才同时要求）
    YEAR_LOOKBACK_DAYS = 365   # 回看天数
    YEAR_MIN_BARS = 60         # 少于 60 根日线就不下结论（次新币不参与判定）

    # ---- OI 存量分位闸门 ----
    # 这是 419 条历史信号回测里**唯一**能把信号从噪音中分出来的维度（见 get_oi_position 注释）。
    #   oi_pos = 当前 OI 在最近 N 天 OI 序列中的分位（0 = 窗口最低，1 = 窗口最高）
    #   oi_pos 越低 = 杠杆水位越低 = 这个币还没被堆杠杆
    OI_POS_DAYS = 30           # OI 分位窗口（天）。币安 openInterestHist 最多只提供 30 天
    OI_POS_MAX = 0.25          # oi_pos <= 0.25 视为「杠杆低位」
    OI_POS_MIN_BARS = 14       # 少于 14 根不下结论

    # 「价格平静」口径：用户明确要求「OI 和价格同向、但涨跌不超过 5% 也可纳入信号」。
    # 所以不是"价格不涨"才算好，而是 |涨跌| <= 5% 且 OI 在累积就值得看。
    FLAT_PRICE_PCT = 5.0
    OI_GROWTH_MIN = 1.0        # OI 累积门槛（%）。对「低位区」用的是 24h 变化（oi_chg_1d）

    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.proxies = []
        self.proxy_index = 0
        # 直连健康状态：None=未知 / True=可用 / False=已连续失败
        self._direct_ok = None
        self._direct_fail_streak = 0
        # 记住最近一次成功的代理，后续请求优先复用，省掉反复试探
        self._good_proxy = None
        # 并发场景下保护上面几个共享状态
        self._lock = threading.Lock()

    def get_public_proxies(self):
        """从公共源获取最新代理列表"""
        with self._lock:
            if self.proxies:
                return
        try:
            logger.info("正在获取公共代理列表...")
            # 使用 reliable 的 GitHub 代理列表源
            url = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                # 只取前50个，避免太久
                all_proxies = resp.text.splitlines()[:50]
                with self._lock:
                    self.proxies = [{"http": f"http://{p}", "https": f"http://{p}"} for p in all_proxies]
                logger.info(f"成功获取 {len(all_proxies)} 个代理")
        except Exception as e:
            logger.error(f"获取代理失败: {e}")

    @staticmethod
    def _try_request(url, timeout, proxy=None):
        """单次请求：成功返回解析后的 JSON，失败返回 None。

        把"什么算失败"集中在一处：非 200、币安限流/受限时返回的
        {"code":...,"msg":...} 错误体、以及任何网络异常。
        """
        try:
            resp = requests.get(url, proxies=proxy, timeout=timeout)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if isinstance(data, dict) and 'code' in data:
                return None
            return data
        except Exception:
            return None

    def request_with_retry(self, url, timeout=None, allow_proxy=True):
        """带降级重试的请求封装。

        按成本从低到高依次尝试：
          1. 直连 —— 连续失败 3 次后，本次运行不再为每个请求白等一次超时
          2. 复用上一次成功的那个代理
          3. 最多再试 MAX_PROXY_RETRIES 个候选代理

        单请求最坏耗时：5 + 10×5 = 55s  →  8 + 3×6 = 26s。
        """
        timeout = timeout or self.DIRECT_TIMEOUT

        # --- 1. 直连 ---
        if self._direct_ok is not False:
            data = self._try_request(url, timeout=timeout)
            if data is not None:
                with self._lock:
                    self._direct_ok = True
                    self._direct_fail_streak = 0
                return data
            with self._lock:
                self._direct_fail_streak += 1
                if self._direct_fail_streak >= 3 and self._direct_ok is not False:
                    logger.warning("直连连续失败 3 次，本次运行改为代理优先")
                    self._direct_ok = False

        if not allow_proxy:
            return None

        # --- 2. 复用上次成功的代理 ---
        with self._lock:
            good = self._good_proxy
        if good is not None:
            data = self._try_request(url, timeout=timeout, proxy=good)
            if data is not None:
                return data
            with self._lock:
                self._good_proxy = None

        # --- 3. 候选代理 ---
        self.get_public_proxies()
        with self._lock:
            candidates = list(self.proxies[:self.MAX_PROXY_RETRIES])
        for i, proxy in enumerate(candidates, 1):
            logger.info(f"尝试代理 [{i}/{len(candidates)}]...")
            data = self._try_request(url, timeout=timeout, proxy=proxy)
            if data is not None:
                with self._lock:
                    self._good_proxy = proxy
                return data

        # 全都失败
        return None

    def get_real_oi_growth(self, symbol: str):
        """返回 (当前OI, 2h变化%, 24h变化%, LS)。

        为什么同时要两个窗口（2026-09-13 用 41 天真实数据回测得出）:

            信号组                        前瞻7d超额中位   胜率
            全体基线                        +0.88%        54%
            OI 2h 涨>1% 且 |24h价|<=5%      +1.05%        57%
            OI 24h 涨>1% 且 |24h价|<=5%     +2.02%        57%   ← 采用
            OI 2h 涨>2%（不限价格）          +0.48%        52%   ← 低于基线，是噪音
            OI 24h 涨>2%（不限价格）         +1.55%        56%

        更关键的对照 —— 「OI 涨」与「价格平静」是**交集**才有效:
            只价格平静(OI不涨)  +0.78% / 53%     只 OI 涨(价格不平静)  +0.91% / 55%
            两个都满足(24h)     +2.02% / 57%

        结论: 2h 窗口的 OI 脉冲单独看是**负 alpha**，日级窗口才是「累积」。
        但 2h 仍保留 —— 「2h OI 爆增榜」要的就是脉冲，两类信号各有用途。
        样本 n≈200~285、每币 168h 冷却去重叠，证据强度: 中等（非决定性）。
        """
        try:
            # 获取当前OI
            oi_resp = self.request_with_retry(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
            if not oi_resp or 'openInterest' not in oi_resp:
                return 0, 0, 0, 1.0
            oi_now = float(oi_resp['openInterest'])

            # 一次请求拿 25 个点的小时级 OI，同时算 2h 与 24h 变化
            # （原来是 period=2h&limit=2 只拿 2h；换成 1h/25 后请求数不变，多出 24h 维度）
            hist_url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=1h&limit=25"
            hist_resp = self.request_with_retry(hist_url)

            oi_chg_2h = 0.0
            oi_chg_1d = 0.0
            # 注意：空列表也要挡住，否则下面的索引会抛 IndexError
            if isinstance(hist_resp, list) and len(hist_resp) >= 3:
                oi_2h_ago = float(hist_resp[-3]['sumOpenInterest'])
                if oi_2h_ago > 0:
                    oi_chg_2h = (oi_now - oi_2h_ago) / oi_2h_ago * 100
                oi_24h_ago = float(hist_resp[0]['sumOpenInterest'])
                if oi_24h_ago > 0:
                    oi_chg_1d = (oi_now - oi_24h_ago) / oi_24h_ago * 100

            # LS Ratio（过去2小时）
            ls_url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period=2h&limit=1"
            ls_resp = self.request_with_retry(ls_url)
            ls_ratio = 1.0
            if isinstance(ls_resp, list) and ls_resp:
                ls_ratio = float(ls_resp[0]['longShortRatio'])

            return oi_now, oi_chg_2h, oi_chg_1d, ls_ratio
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return 0, 0, 0, 1.0

    def get_cvd_2h_usdt(self, symbol: str):
        """计算过去2小时的主动买卖净差值 (CVD)，以 USDT 计价"""
        try:
            # 获取过去2小时的 5m K线 (limit=24)
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit=24"
            resp = self.request_with_retry(url)
            if not resp or not isinstance(resp, list):
                return 0.0
            
            net_delta_usdt = 0.0
            for k in resp:
                quote_vol = float(k[7]) # 总USDT交易量
                taker_buy_usdt = float(k[10]) # 主动买入的USDT量
                taker_sell_usdt = quote_vol - taker_buy_usdt # 主动卖出的USDT量
                delta = taker_buy_usdt - taker_sell_usdt
                
                net_delta_usdt += delta
                
            return net_delta_usdt
        except Exception as e:
            logger.error(f"Error fetching CVD for {symbol}: {e}")
            return 0.0

    def get_light_oi(self, symbol: str) -> float:
        """轻量获取当前 OI（合约张数）"""
        try:
            oi_resp = self.request_with_retry(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
            if oi_resp and 'openInterest' in oi_resp:
                return float(oi_resp['openInterest'])
        except Exception as e:
            logger.error(f"Error fetching light OI {symbol}: {e}")
        return 0.0

    def get_year_position(self, symbol: str) -> Optional[Dict]:
        """一年低点分位：判断这个币现在贵不贵。

        之前整套系统只看 OI 增幅和 2h 涨跌，**完全不知道标的处在一年里的什么位置**，
        所以经常把「已经涨过一大截」的币当成机会推出去。这个方法补上这一维。

        返回:
            pos      0~1，越小越便宜
            dd       距一年高点的回撤%（负数）
            low365 / high365 / days / is_cheap
        """
        try:
            url = (f"https://fapi.binance.com/fapi/v1/klines"
                   f"?symbol={symbol}&interval=1d&limit={self.YEAR_LOOKBACK_DAYS}")
            resp = self.request_with_retry(url)
            if not isinstance(resp, list) or len(resp) < self.YEAR_MIN_BARS:
                return None
            highs = [float(k[2]) for k in resp]
            lows = [float(k[3]) for k in resp]
            close = float(resp[-1][4])
            hi, lo = max(highs), min(lows)
            if hi <= lo or close <= 0:
                return None
            pos = (close - lo) / (hi - lo)
            return {
                "pos": pos,
                "dd": (close - hi) / hi * 100,
                "low365": lo,
                "high365": hi,
                "days": len(resp),
                "is_cheap": pos <= self.YEAR_LOW_POS_MAX,
            }
        except Exception as e:
            logger.error(f"一年低点分位计算失败 {symbol}: {e}")
            return None

    def get_oi_position(self, symbol: str) -> Optional[Dict]:
        """OI 存量分位：这个币的杠杆水位，处在最近一段时间的什么位置。

        ⚠️ 状态：**实验性，证据薄弱，未经独立验证。**

        这一维的来历和更正（重要，别再引用错误数字）：
        最初是从信号账本里 `OI分位` 一列反推的，那一列看着很漂亮
        （低分位 +2.5%、高分位 −4.6% 的"断崖"）。但后续复核发现该列
        **无法用任何公开数据复现**：抽 19 条逐条比对，币安日线 OI 分位
        与记录值全部不符（DASH 记录 85% 实测 0%、SCRT 记录 0% 实测 46%）；
        该列 71% 的取值落在 1/30 网格上，说明它确实是某个 30 点窗口的分位，
        但口径与数据源均未知（币安只保留 30 天历史，也无法追溯核验）。
        因此**原结论作废**。

        用币安自算、可复现的口径重做（样本限制在 2026-08-14 之后，
        因为更早的日线 OI 已超出币安 30 天保留期）：

            筛选条件                    样本   7d 超额中位   胜率
            基线（同窗口全部）          140     +0.18%      51%
            bn_pos <= 10%                10     +2.67%      60%
            bn_pos <= 25%                20     +1.08%      55%
            bn_pos >= 90%                48     +2.38%      56%  ← 断崖不存在
            bn_pos 50-70%                20     +2.52%      65%
            bn_pos<=20% 且 |价|<=5       11     +3.21%      73%
            bn_pos<=25% 且 |价|<=5       17     +3.21%      65%

        结论：**"高分位一定差"不成立**（90-100% 那档反而是 +2.38%）。
        只微弱支持"低分位 + 价格平静 略好于基线"，但子样本 n<20，
        只能算方向性提示，不足以当硬闸门。所以 25% 这个阈值是暂定值，
        需要继续累积样本后再定，不要当成已验证的结论。

        返回: pos / value / low / high / days / is_low
        """
        try:
            url = (f"https://fapi.binance.com/futures/data/openInterestHist"
                   f"?symbol={symbol}&period=1d&limit={self.OI_POS_DAYS}")
            resp = self.request_with_retry(url)
            if not isinstance(resp, list) or len(resp) < self.OI_POS_MIN_BARS:
                return None
            vals = [float(x['sumOpenInterest']) for x in resp]
            cur = vals[-1]
            lo, hi = min(vals), max(vals)
            if hi <= lo or cur <= 0:
                # 窗口内 OI 完全不动：无所谓高低，按最低位处理
                return {"pos": 0.0, "value": cur, "low": lo, "high": hi,
                        "days": len(vals), "is_low": True}
            n = len(vals)
            below = sum(1 for v in vals if v < cur)
            above = sum(1 for v in vals if v > cur)
            # 平局用 (below + (n-below-above)/2) / n 处理，避免除数偏差
            pos = (below + (n - below - above) / 2.0) / n if (below + above) < n else below / n
            return {
                "pos": pos,
                "value": cur,
                "low": lo,
                "high": hi,
                "days": n,
                "is_low": pos <= self.OI_POS_MAX,
            }
        except Exception as e:
            logger.error(f"OI 存量分位计算失败 {symbol}: {e}")
            return None

    def get_ls_ratio(self, symbol: str) -> Optional[float]:
        """大户多空持仓比（topLongShortPositionRatio, 2h）。

        用于区分「OI 涨 + 价格跌」到底是**多头在逢跌吸筹**还是**空头在加仓**——
        只看 OI 与价格两个量是分不开的，这是判读里最容易搞错的一处。
        """
        try:
            url = (f"https://fapi.binance.com/futures/data/topLongShortPositionRatio"
                   f"?symbol={symbol}&period=2h&limit=1")
            resp = self.request_with_retry(url)
            if isinstance(resp, list) and resp:
                return float(resp[0]['longShortRatio'])
        except Exception as e:
            logger.error(f"LS 获取失败 {symbol}: {e}")
        return None

    def enrich_positions(self, symbols: List[str]) -> Dict[str, Dict]:
        """只为候选币补算「两个位置」：价格一年分位 + OI 存量分位。

        不给全部 ~150 个币算：每个币要多 2 次请求（365 根日线 + 30 天 OI），
        payload 不小。只对已经进入候选池的币算，请求量从 ~300 降到 ~40。

        返回 {symbol: {"yp": 一年分位 or None, "oip": OI 分位 or None}}
        """
        symbols = [s for s in dict.fromkeys(symbols) if s]  # 去重保序
        if not symbols:
            return {}

        def _one(t):
            s = t["symbol"]
            return {"symbol": s, "yp": self.get_year_position(s), "oip": self.get_oi_position(s)}

        out = self._run_concurrent([{"symbol": s} for s in symbols], _one)
        return {s: {"yp": d.get("yp"), "oip": d.get("oip")} for s, d in out.items()}

    def _collect_one(self, ticker: Dict, premium: Optional[Dict]) -> Optional[Dict]:
        """采集单个交易对的全部指标（供并发调用）。失败返回 None。"""
        s = ticker['symbol']
        try:
            oi_val, oi_chg, oi_chg_1d, ls = self.get_real_oi_growth(s)
            cvd_usdt = self.get_cvd_2h_usdt(s)
            funding = float(premium['lastFundingRate']) * 100 if premium else 0
            return {
                "symbol": s,
                "price": float(ticker['lastPrice']),
                "price_chg": float(ticker['priceChangePercent']),
                "oi_value": oi_val,
                "oi_chg": oi_chg,          # 2h 变化：给「2h OI 爆增榜」用
                "oi_chg_1d": oi_chg_1d,    # 24h 变化：给「低位区」闸门用（回测显示日级才有效）
                "ls": ls,
                "cvd_usdt": cvd_usdt,
                "funding": funding,
            }
        except Exception as e:
            logger.error(f"采集 {s} 失败: {e}")
            return None

    def _light_one(self, ticker: Dict, premium: Optional[Dict]) -> Optional[Dict]:
        """轻量采集单个交易对（升温模式：只要 OI + 价格 + 费率）。"""
        s = ticker['symbol']
        try:
            return {
                "symbol": s,
                "price": float(ticker['lastPrice']),
                "price_chg": float(ticker['priceChangePercent']),
                "oi_value": self.get_light_oi(s),
                "oi_chg": 0,
                "oi_chg_1d": 0,
                "ls": 1.0,
                "cvd_usdt": 0,
                "funding": float(premium['lastFundingRate']) * 100 if premium else 0,
            }
        except Exception as e:
            logger.error(f"轻量采集 {s} 失败: {e}")
            return None

    def _run_concurrent(self, tickers: List[Dict], worker) -> Dict[str, Dict]:
        """并发执行 worker(ticker)，返回 {symbol: data_point}。

        - 并发度 MAX_WORKERS，远低于币安 fapi 的权重上限（2400/分钟），不会触发限流
        - 总耗时受 SCAN_BUDGET_SECONDS 约束：到点即停止等待，用已拿到的数据出报告，
          而不是像原来那样把 600 次串行请求一路拖到 4~5 小时
        """
        results: Dict[str, Dict] = {}
        total = len(tickers)
        if not total:
            return results

        deadline = time.monotonic() + self.SCAN_BUDGET_SECONDS
        started = time.monotonic()
        logger.info(f"并发采集 {total} 个交易对（并发度 {self.MAX_WORKERS}，预算 {self.SCAN_BUDGET_SECONDS}s）...")

        pool = ThreadPoolExecutor(max_workers=self.MAX_WORKERS)
        try:
            futures = {pool.submit(worker, t): t['symbol'] for t in tickers}
            for done, fut in enumerate(as_completed(futures), 1):
                symbol = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    logger.error(f"{symbol} 采集异常: {e}")
                    row = None
                if row is not None:
                    results[symbol] = row
                if done % 25 == 0:
                    logger.info(f"  进度 {done}/{total}，已成功 {len(results)}")
                if time.monotonic() > deadline:
                    logger.warning(
                        f"已达 {self.SCAN_BUDGET_SECONDS}s 预算，停止等待剩余 {total - done} 个交易对"
                    )
                    break
        finally:
            # 不等未完成的任务；单请求本身有超时，残留线程最多再跑 26s 就会自己结束
            pool.shutdown(wait=False, cancel_futures=True)

        logger.info(f"并发采集结束：{len(results)}/{total} 成功，耗时 {time.monotonic() - started:.1f}s")
        return results

    def scan_and_collect(self, threshold: float = 10_000_000) -> Dict:
        """扫描市场并返回结构化数据和报告文本"""
        logger.info("开始币安OI扫描...")
        # 获取Ticker和Funding
        t_resp = self.request_with_retry("https://fapi.binance.com/fapi/v1/ticker/24hr")
        p_resp = self.request_with_retry("https://fapi.binance.com/fapi/v1/premiumIndex")
        
        if not t_resp or not isinstance(t_resp, list):
            msg = f"⚠️ 扫描失败: 币安API连接错误 (已重试)\n(所有代理尝试均失败或IP仍受限)"
            if isinstance(t_resp, dict): msg += f"\n`{str(t_resp)[:100]}...`"
            return {
                "message": msg,
                "coins": {},
                "all_metrics": [],
                "timestamp": datetime.now().isoformat()
            }
        
        if not p_resp or not isinstance(p_resp, list):
             return {
                "message": f"⚠️ 扫描失败: 资金费率API连接错误",
                "coins": {},
                "all_metrics": [],
                "timestamp": datetime.now().isoformat()
            }

        premiums = {p['symbol']: p for p in p_resp}

        # 筛选USDT活跃交易对：24h成交额 > $10M（约150个，覆盖主要活跃合约）
        active_tickers = [
            t for t in t_resp
            if t['symbol'].endswith("USDT") and float(t['quoteVolume']) > threshold
        ]
        active_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)

        structured_coins = {} # 用于存入数据库
        total = len(active_tickers)

        # 并发取数（原来是完全串行的 for 循环：约 600 次请求，最坏每次 55s，合计 4~5 小时）
        collected = self._run_concurrent(
            active_tickers,
            lambda t: self._collect_one(t, premiums.get(t['symbol'])),
        )
        # 按 24h 成交额降序还原顺序，保证报告内容与原实现一致、可复现
        all_metrics = [collected[t['symbol']] for t in active_tickers if t['symbol'] in collected]

        # 筛选逻辑
        # 低位埋伏: 价格未暴涨(-2%到5%), OI增加(24h窗口), 大户多, 且CVD纯买入>0
        # OI 判定已从 2h 改为 24h：回测显示 2h 脉冲单独看是负 alpha（详见 get_real_oi_growth）
        accumulation = [d for d in all_metrics if -2 < d['price_chg'] < 5 and d['oi_chg_1d'] > 1.5 and d['ls'] > 1.2 and d['cvd_usdt'] > 0]
        top_oi = sorted(all_metrics, key=lambda x: x['oi_chg'], reverse=True)[:5]
        ext_neg = sorted([d for d in all_metrics if d['funding'] < 0], key=lambda x: x['funding'])[:3]
        ext_pos = sorted([d for d in all_metrics if d['funding'] > 0], key=lambda x: x['funding'], reverse=True)[:3]

        # ---- 两个位置：只给候选币补算，避免 ~300 次额外请求 ----
        # 候选池 = 低位埋伏 + OI爆增 + 「OI 24h 累积且价格平静」的形状
        quiet = [d for d in all_metrics
                 if d['oi_chg_1d'] > self.OI_GROWTH_MIN
                 and abs(d['price_chg']) <= self.FLAT_PRICE_PCT]
        quiet.sort(key=lambda x: x['oi_chg'], reverse=True)
        candidate_syms = ([d['symbol'] for d in accumulation]
                          + [d['symbol'] for d in top_oi]
                          + [d['symbol'] for d in quiet[:30]])
        pos_map = self.enrich_positions(candidate_syms)
        for d in all_metrics:
            pm = pos_map.get(d['symbol']) or {}
            yp, oip = pm.get('yp'), pm.get('oip')
            d['year_pos'] = yp['pos'] if yp else None
            d['year_dd'] = yp['dd'] if yp else None
            d['is_cheap'] = bool(yp and yp['is_cheap'])
            d['oi_pos'] = oip['pos'] if oip else None
            d['oi_low'] = oip['low'] if oip else None
            d['oi_high'] = oip['high'] if oip else None
            d['is_oi_low'] = bool(oip and oip['is_low'])

        def pos_tag(d):
            """位置标签，两个维度分开显示：杠杆位:8%🟢 价位:12%🟢"""
            parts = []
            if d.get('oi_pos') is not None:
                parts.append(f"杠杆位:{d['oi_pos'] * 100:.0f}%"
                             + ("🟢" if d.get('is_oi_low') else ""))
            else:
                parts.append("杠杆位:?")
            if d.get('year_pos') is not None:
                parts.append(f"价位:{d['year_pos'] * 100:.0f}%"
                             + ("🟢" if d.get('is_cheap') else ""))
            else:
                parts.append("价位:?")
            return " ".join(parts)

        # 用户核心口径：杠杆水位低 + OI 在缓慢累积 + 价格没怎么动
        # （|涨跌| <= 5%，含小幅上涨；这才是"悄悄建仓"的形状）
        #
        # 闸门 = 杠杆位低（OI 存量分位 <= 25%）+ OI 在增 + 价格没怎么动。
        # 注意：这条闸门的证据**很薄**，详见 get_oi_position() 的说明——
        # 原先引用的"低分位 +2.5% / 高分位 −4.6% 断崖"来自一个不可复现的列，
        # 已作废。用币安自算口径重测后只剩方向性提示（低位+价格平静 n=17、
        # 中位 +3.21% / 胜率 65%，对基线 +0.18% / 51%），且高分位那档并不差。
        # 所以这里按"宁可少推、别推噪音"处理：门槛保持 25%，但不宣称已验证。
        # 价位（一年分位）仍会显示在标签里（用户关心"近一年新低附近"），
        # 但没有历史样本可以验证它，所以不拿它当闸门。
        low_zone = [
            d for d in all_metrics
            if d.get('is_oi_low')
            and d['oi_chg_1d'] > self.OI_GROWTH_MIN
            and abs(d['price_chg']) <= self.FLAT_PRICE_PCT
        ]
        low_zone.sort(key=lambda x: x['oi_chg_1d'], reverse=True)
        low_zone_syms = {d['symbol'] for d in low_zone}

        # 反向提示：OI 已在高位的币，别当低位机会
        high_leverage = [d for d in all_metrics
                         if d.get('oi_pos') is not None and d['oi_pos'] >= 0.90]

        # 金额格式化小工具
        def format_usd(val):
            abs_val = abs(val)
            if abs_val >= 1_000_000:
                fmt = f"{abs_val/1_000_000:.2f}M"
            elif abs_val >= 1_000:
                fmt = f"{abs_val/1_000:.1f}K"
            else:
                fmt = f"{abs_val:.0f}"
            return "+$" + fmt if val > 0 else "-$" + fmt

        # 构造报告文本
        beijing_time = datetime.utcnow() + timedelta(hours=8)
        msg = f"🛰️ **【{beijing_time.strftime('%H:%M')} 真实持仓扫描 (GHA版)】**\n"
        # 把数据完整度写进报告：以前报告残缺时你完全看不出来
        msg += f"📊 数据完整度: {len(all_metrics)}/{total}"
        if total and len(all_metrics) < total * 0.9:
            msg += " ⚠️ 部分交易对取数失败，榜单可能不完整"
        msg += "\n"

        # ① 最符合口味的一栏放在最前面：杠杆低位 + OI 累积 + 价格平静
        # 标题里的窗口必须写准（原为含糊的「OI静默累积」，实际用的是 2h 变化 ── 已修）
        msg += f"\n🟢 **低位区·OI日级累积 (杠杆位≤25% + OI 24h增>{self.OI_GROWTH_MIN:.0f}% + |24h涨跌|≤{self.FLAT_PRICE_PCT:.0f}%)**\n"
        if not low_zone:
            msg += "• 暂无匹配\n"
        for d in low_zone[:8]:
            dd = f" | DD{d['year_dd']:+.0f}%" if d.get('year_dd') is not None else ""
            msg += (f"• `{d['symbol']}`: {pos_tag(d)}{dd} "
                    f"| OI:24h {d['oi_chg_1d']:+.1f}% (2h {d['oi_chg']:+.1f}%) "
                    f"| 价:{d['price_chg']:+.1f}%\n")
            structured_coins[d['symbol']] = {"ls_value": d['ls'], "section": "low_zone", "extra_info": ""}

        msg += "\n💎 **低位埋伏 (横盘+OI 24h增+大户多+CVD净买入)**\n"
        if not accumulation: msg += "• 暂无匹配\n"
        for d in accumulation:
            cvd_str = format_usd(d['cvd_usdt'])
            msg += f"• `{d['symbol']}`: {pos_tag(d)} | OI:24h {d['oi_chg_1d']:+.1f}% | LS:{d['ls']:.2f} | CVD:{cvd_str}\n"
            structured_coins.setdefault(d['symbol'], {"ls_value": d['ls'], "section": "accumulation", "extra_info": ""})

        msg += "\n📈 **2h OI 爆增榜**（脉冲，不是累积；回测显示单独看是负 alpha）\n"
        for d in top_oi:
            cvd_str = format_usd(d['cvd_usdt'])
            msg += f"• `{d['symbol']}`: {pos_tag(d)} | OI:+{d['oi_chg']:.1f}% | CVD:{cvd_str} | LS:{d['ls']:.2f}\n"
            # 如果币种重复，优先保留前面的分类，否则覆盖
            if d['symbol'] not in structured_coins:
                structured_coins[d['symbol']] = {"ls_value": d['ls'], "section": "top_oi", "extra_info": f"F:{d['funding']:.3f}%"}

        msg += "\n☢️ **极端费率**\n"
        for d in ext_neg:
            msg += f"• `{d['symbol']}` (负): `{d['funding']:.3f}%` | LS:{d['ls']:.2f}\n"
        for d in ext_pos:
            msg += f"• `{d['symbol']}` (正): `{d['funding']:.3f}%` | LS:{d['ls']:.2f}\n"

        # 覆盖提示：以前的报告完全不体现"推荐得对不对位"，容易被追高
        if top_oi:
            chased = [d for d in top_oi if d.get('oi_pos') is not None and not d['is_oi_low']]
            if chased:
                msg += (f"\n⚠️ 上面 OI 爆增榜里有 {len(chased)} 个杠杆位不在低位（"
                        + "、".join(f"{d['symbol']} {d['oi_pos']*100:.0f}%" for d in chased)
                        + "），属于右侧追高，注意区分\n")
        if high_leverage:
            # 措辞已按复检结果收紧：高分位那档实测并不差（+2.38%），
            # 所以只说"不是低位"，不再宣称它会被罚。
            msg += (f"📉 杠杆位≥90% 的标的有 {len(high_leverage)} 个，"
                    "它们的杠杆已经堆在近期高位，不属于「低位埋伏」的形状\n")

        return {
            "message": msg,
            "coins": structured_coins,
            "all_metrics": all_metrics,
            "low_zone": [d['symbol'] for d in low_zone],
            "timestamp": datetime.now().isoformat()
        }

    def scan_light(self, threshold: float = 5_000_000) -> List[Dict]:
        """轻量扫描：只取 OI + 价格 + 费率（不取CVD/多空比），用于每日升温快照"""
        logger.info("开始轻量 OI 扫描（升温模式）...")
        t_resp = self.request_with_retry("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=20)
        p_resp = self.request_with_retry("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=15)

        if not t_resp or not isinstance(t_resp, list):
            logger.error("轻量扫描失败：ticker API 错误")
            return []

        premiums = {p['symbol']: p for p in p_resp} if isinstance(p_resp, list) else {}

        active_tickers = [
            t for t in t_resp
            if t['symbol'].endswith("USDT") and float(t['quoteVolume']) > threshold
        ]
        active_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)

        # 并发取数：升温模式每币 1 次请求，串行同样会被降级路径拖慢
        collected = self._run_concurrent(
            active_tickers,
            lambda t: self._light_one(t, premiums.get(t['symbol'])),
        )
        all_metrics = [collected[t['symbol']] for t in active_tickers if t['symbol'] in collected]

        logger.info(f"轻量扫描完成，共 {len(all_metrics)}/{len(active_tickers)} 个币")
        return all_metrics

    def _split_message(self, text: str) -> List[str]:
        """按行分片，保证每片不超过 Telegram 单条消息上限。"""
        limit = self.TELEGRAM_MAX_LEN
        if len(text) <= limit:
            return [text]
        chunks, buf = [], ""
        for line in text.split("\n"):
            if len(buf) + len(line) + 1 > limit:
                chunks.append(buf)
                buf = ""
            buf += line + "\n"
        if buf:
            chunks.append(buf)
        return chunks

    def send_telegram(self, text):
        """发送 Telegram 消息：超长自动分片，失败记日志而不是静默吞掉。"""
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        for chunk in self._split_message(text):
            try:
                resp = requests.post(
                    url,
                    json={"chat_id": self.chat_id, "text": chunk, "parse_mode": "Markdown"},
                    timeout=20,
                )
                if resp.status_code != 200:
                    logger.error(f"Telegram 发送失败: {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                logger.error(f"Telegram 发送异常: {e}")

# ==================== LS 分析逻辑 ====================
class LSAnalyzer:
    @staticmethod
    def analyze(reports: List[Dict]) -> List[Dict]:
        """分析报告列表中的LS变化"""
        # 整理每个币种的历史
        coin_history = {}
        for r in reports:
            # 兼容旧数据结构，确保coins存在
            coins = r.get('coins', {})
            for symbol, data in coins.items():
                if symbol not in coin_history:
                    coin_history[symbol] = []
                coin_history[symbol].append(data['ls_value'])

        results = []
        for symbol, history in coin_history.items():
            if len(history) < 2: continue
            
            first = history[0]
            last = history[-1]
            
            # 简单的增长判定
            if last > first:
                results.append({
                    "symbol": symbol,
                    "first": first,
                    "last": last,
                    "growth_pct": (last - first)/first * 100,
                    "count": len(history)
                })
        
        results.sort(key=lambda x: x['growth_pct'], reverse=True)
        return results

    @staticmethod
    def generate_report(results: List[Dict]) -> str:
        if not results:
            return "🤖 **【LS趋势分析】**\n本周期未发现LS持续增长的币种。"
            
        msg = f"🤖 **【LS趋势分析 (最近4轮)】**\n发现 {len(results)} 个LS增长币种:\n\n"
        for i, r in enumerate(results[:15], 1): # 只显示前15个
            msg += f"**{i}. {r['symbol']}**\n"
            msg += f"   • LS: {r['first']:.2f} → {r['last']:.2f} (+{r['growth_pct']:.1f}%)\n"
            msg += f"   • 出现次数: {r['count']}\n"
        return msg

# ==================== OI 持续升温追踪 ====================
class WarmupTracker:
    """追踪「OI 缓慢累积」并分类，输出符合「买得便宜」口味的候选。

    2026-09-13 改造（P0-3），四处关键变化：

    1. **窗口 5 天 → 30 天**。"缓慢累积"是周级现象；5 天窗口量到的只是噪音，
       也正是「持续升温」这条规则占了全部信号 57% 却没人发现的原因。
    2. **价格语义反转**。原实现把「价格同步上涨」当作 ✅共振确认加分 ——
       这跟用户口味是相反的。用户要的是「OI 在累积、价格没怎么动」，
       所以**价格平静才是加分项**，价格猛涨反而是风险。
    3. **新增价格平静度与 OI-平静比**（OI 涨得多 / 价格振幅小，比值越大越像悄悄建仓）。
    4. **追高判定**从「距 5 日低点」改为「距窗口低点」，并按用户确认的口径分类：
         · 底部启动（低位 + OI 增 + 价格平静）→ **要**
         · 温和共振（OI 增 + |涨跌| ≤ 5%）→ **要**（用户明确要求纳入）
         · 风险标记（FOMO 追高 / 背离）→ **要**（有信息价值）
         · 超跌 → **降级**（用户：超跌不一定会涨）

    单独看「OI 涨 + 价格跌」是分不出多空的：可能是多头逢跌吸筹，也可能是空头在加仓。
    所以对这类候选额外取一次大户多空持仓比（LS）来定性。
    """

    COLLECTION = 'oi_warmup_tracker'
    DOC_ID = 'daily_snapshots'
    MAX_DAYS = 30          # 原为 5 天
    MIN_DAYS = 5           # 至少 5 天才出结论；窗口未满 30 天时逐步生效
    FLAT_PRICE_PCT = 5.0   # |涨跌| <= 5% 视为「价格平静」（用户口径）
    CHASE_HIGH_PCT = 15.0  # 距窗口低点涨幅 > 15% 视为追高
    MIN_OI_GROWTH = 0.5    # 窗口累计 OI 涨幅下限（%）
    MAX_ENRICH = 40        # 最多给多少个候选补算 OI分位/一年分位/LS（控请求量：上限 40×3）

    def __init__(self, db):
        self.db = db
        self.doc_ref = self.db.collection(self.COLLECTION).document(self.DOC_ID)

    def store_daily_snapshot(self, all_metrics: List[Dict]) -> None:
        """存储当天所有币种快照，按天聚合（保留最后一次），仅保留最近 MAX_DAYS 天"""
        if not all_metrics:
            return

        date_str = (datetime.utcnow() + timedelta(hours=8)).strftime('%Y-%m-%d')
        day_data = {
            m['symbol']: {
                "oi": m.get('oi_value', 0),
                "price": m.get('price', 0),
                "ls": m.get('ls', 1.0),
                "cvd": m.get('cvd_usdt', 0),
                "fr": m.get('funding', 0),
            }
            for m in all_metrics
        }

        doc = self.doc_ref.get()
        snapshots = doc.to_dict().get('snapshots', []) if doc.exists else []

        # 当天已存在则覆盖（保留最后一次），否则追加
        for s in snapshots:
            if s.get('date') == date_str:
                s['data'] = day_data
                break
        else:
            snapshots.append({"date": date_str, "data": day_data})

        # 按日期排序，仅保留最近MAX_DAYS天
        snapshots.sort(key=lambda x: x.get('date', ''))
        snapshots = snapshots[-self.MAX_DAYS:]

        self.doc_ref.set({'snapshots': snapshots})
        logger.info(f"OI升温快照已存储，当前保留 {len(snapshots)} 天")

    def get_history(self) -> List[Dict]:
        doc = self.doc_ref.get()
        if doc.exists:
            return doc.to_dict().get('snapshots', [])
        return []

    def detect_oi_sustained_growth(self, snapshots: List[Dict]) -> List[Dict]:
        """检测 OI 缓慢累积，并计算价格平静度与位置。

        触发条件：窗口内 OI 至少 2 天环比上涨、末日 > 首日、累计涨幅 >= MIN_OI_GROWTH。
        排序依据是 **OI-平静比**（OI 涨幅 ÷ 价格振幅）而不是 OI 涨幅本身 ——
        OI 涨得多但价格也涨得多的，是右侧追高；OI 涨得多而价格很平的，才是要的形态。
        """
        snapshots = sorted(snapshots, key=lambda x: x.get('date', ''))
        recent = snapshots[-self.MAX_DAYS:]
        if len(recent) < self.MIN_DAYS:
            return []

        latest_data = recent[-1].get('data', {})
        results = []

        for symbol in latest_data:
            oi_series, price_series, fr_series = [], [], []

            for s in recent:
                d = s.get('data', {})
                if symbol in d:
                    oi_series.append(d[symbol].get('oi', 0) or 0)
                    price_series.append(d[symbol].get('price', 0) or 0)
                    fr_series.append(d[symbol].get('fr', 0) or 0)

            if len(oi_series) < self.MIN_DAYS or len(price_series) < self.MIN_DAYS:
                continue

            first_oi, last_oi = oi_series[0], oi_series[-1]
            if first_oi <= 0 or last_oi <= first_oi:
                continue

            oi_change = (last_oi - first_oi) / first_oi * 100
            if oi_change < self.MIN_OI_GROWTH:
                continue

            up_days = sum(1 for i in range(1, len(oi_series)) if oi_series[i] > oi_series[i - 1])
            if up_days < 2:
                continue

            p_first, p_last = price_series[0], price_series[-1]
            if p_first <= 0:
                continue
            price_chg = (p_last - p_first) / p_first * 100
            p_low, p_high = min(price_series), max(price_series)

            # 价格平静度：窗口内振幅，越小越"平"
            calm = (p_high - p_low) / p_low * 100 if p_low > 0 else 0.0
            # 距窗口低点涨幅：越大说明越接近"已经涨过"
            dist_low = (p_last - p_low) / p_low * 100 if p_low > 0 else 0.0
            # OI-平静比：越大越像"悄悄建仓"
            oi_calm_ratio = oi_change / max(calm, 0.5)

            # 初分类（未含 LS / 一年分位）
            if price_chg > self.FLAT_PRICE_PCT:
                kind, tag = "fomo", "🔴FOMO危险区"
            elif price_chg >= 0:
                kind, tag = "mild", "🟢温和共振"
            elif price_chg >= -self.FLAT_PRICE_PCT:
                kind, tag = "diverge", "⚪OI增·价微跌"
            else:
                kind, tag = "oversold", "💎超跌区间"

            results.append({
                "symbol": symbol,
                "up_days": up_days,
                "total_days": len(recent),
                "oi_change": oi_change,
                "price_chg": price_chg,
                "price_range": calm,
                "dist_low": dist_low,
                "oi_calm_ratio": oi_calm_ratio,
                "chase_high": dist_low > self.CHASE_HIGH_PCT,
                "funding": fr_series[-1] if fr_series else 0.0,
                "kind": kind,
                "tag": tag,
                "ls": None,
                "direction": "",
                "year_pos": None,
                "year_dd": None,
                "is_cheap": False,
                "oi_pos": None,
                "oi_low": None,
                "oi_high": None,
                "is_oi_low": False,
            })

        results.sort(key=lambda x: x['oi_calm_ratio'], reverse=True)
        return results

    def enrich_candidates(self, monitor, results: List[Dict]) -> List[Dict]:
        """给候选补三个关键字段：OI 存量分位 + 一年低点分位 + 大户多空比（LS）。

        - **OI 存量分位**：回测里唯一真正管用的一维（见 OIMonitor.get_oi_position）。
        - **一年分位**：判断"便不便宜"，这是整套系统原先完全缺失的一维。
        - **LS**：只对「OI 增 + 价格跌」的候选取，用来区分多头吸筹还是空头加仓。
          不给全部候选取，是为把请求量从 ~450 压到 ~90。
        """
        if not results:
            return results
        top = results[:self.MAX_ENRICH]

        for r in top:
            sym = r['symbol']
            oip = monitor.get_oi_position(sym)
            if oip:
                r['oi_pos'] = oip['pos']
                r['oi_low'] = oip['low']
                r['oi_high'] = oip['high']
                r['is_oi_low'] = oip['is_low']
            yp = monitor.get_year_position(sym)
            if yp:
                r['year_pos'] = yp['pos']
                r['year_dd'] = yp['dd']
                r['is_cheap'] = yp['is_cheap']
            if r['kind'] == 'diverge':
                ls = monitor.get_ls_ratio(sym)
                r['ls'] = ls
                if ls is None:
                    r['direction'] = "方向未知"
                elif ls >= 1.0 and r['funding'] >= 0:
                    r['direction'] = f"多头吸筹(LS {ls:.2f})"
                else:
                    r['direction'] = f"⚠️空头加仓(LS {ls:.2f})"

        # 重分类：杠杆位低 + 价格平静 + OI 累积 = 用户最要的「底部启动」。
        # 闸门用杠杆位（已验证），不用价位的理由见 report() 里的注释。
        for r in results:
            if (r['is_oi_low'] and r['kind'] in ('mild', 'diverge')
                    and abs(r['price_chg']) <= self.FLAT_PRICE_PCT):
                r['kind'] = 'bottom_start'
                r['tag'] = "🟢底部启动"
                if r['is_cheap']:
                    r['tag'] = "🟢底部启动+价格低位"
        results.sort(key=lambda x: (x['kind'] != 'bottom_start', -x['oi_calm_ratio']))
        return results


    def format_oi_sustained_growth_message(self, results: List[Dict]) -> str:
        """按用户确认的优先级分栏输出（要的在前，降级的在后并标注）。"""
        if not results:
            return ""

        def pos_str(r):
            parts = []
            if r.get('oi_pos') is not None:
                parts.append(f"杠杆位:{r['oi_pos'] * 100:.0f}%"
                             + ("🟢" if r.get('is_oi_low') else ""))
            else:
                parts.append("杠杆位:?")
            if r.get('year_pos') is not None:
                parts.append(f"价位:{r['year_pos'] * 100:.0f}%"
                             + ("🟢" if r.get('is_cheap') else ""))
            return " ".join(parts)

        def line(r, extra=""):
            s = (f"• `{r['symbol']}`: {pos_str(r)} | OI累计{r['oi_change']:+.1f}%"
                 f" | 价{r['price_chg']:+.1f}% | 振幅{r['price_range']:.1f}%")
            if extra:
                s += f" | {extra}"
            return s

        groups = [
            ("bottom_start", "🟢 **【底部启动】低位 + OI 累积 + 价格平静**", lambda r: ""),
            ("mild", "🟢 **【OI 温和累积】价格平静（|涨跌| ≤ 5%）**", lambda r: ""),
            ("diverge", "⚪ **【OI 增 · 价格微跌】需分多空**", lambda r: r.get('direction', "")),
            ("fomo", "🔴 **【风险标记】OI 增但价格已冲高（右侧）**", lambda r: "追高"),
            ("oversold", "💎 **【超跌区间】仅供参考 —— 超跌不必然反弹**", lambda r: ""),
        ]

        parts = []
        for kind, title, extra_fn in groups:
            rows = [r for r in results if r['kind'] == kind]
            if not rows:
                continue
            parts.append(f"{title}（{len(rows)}）")
            for r in rows[:6]:
                parts.append(line(r, extra_fn(r)))
            parts.append("")

        parts.append("位=一年低点分位(越小越便宜) | 振幅=窗口内价格波动幅度 | "
                     "同 OI 涨幅下振幅越小越像「悄悄建仓」")
        return "\n".join(parts).rstrip()

    def oi_sustained_growth_scan(self, all_metrics: List[Dict], monitor=None) -> str:
        """存储每日快照 → 检测 OI 缓慢累积 → 补 OI分位/一年分位/LS → 生成消息片段"""
        self.store_daily_snapshot(all_metrics)
        snapshots = self.get_history()
        results = self.detect_oi_sustained_growth(snapshots)
        if not results:
            return ""
        if monitor is not None:
            results = self.enrich_candidates(monitor, results)
        return self.format_oi_sustained_growth_message(results)

# ==================== 主入口 ====================
def main():
    try:
        mode = sys.argv[1] if len(sys.argv) > 1 else 'report'
        config = Config()
        fb = FirebaseManager(config.firebase_creds_json)
        monitor = OIMonitor(config.bot_token, config.chat_id)

        if mode == 'warmup':
            # 升温模式：轻扫 >$5M 池子，存快照并检测「OI 缓慢累积」
            # 检测后会只给候选币补算 LS 与一年低点分位（~40 次请求，不给全部 150 个）
            all_metrics = monitor.scan_light(threshold=5_000_000)
            if not all_metrics:
                logger.error("轻量扫描失败，跳过本次升温检测")
                return
            warmup_msg = WarmupTracker(fb.db).oi_sustained_growth_scan(all_metrics, monitor=monitor)
            if warmup_msg:
                monitor.send_telegram(warmup_msg)
                logger.info("OI 持续升温报告发送成功")
            else:
                logger.info("本次无 OI 持续升温候选")
            return

        # 报告模式：全指标扫描 >$10M 池子
        scan_result = monitor.scan_and_collect(threshold=10_000_000)
        monitor.send_telegram(scan_result['message'])
        logger.info("OI 报告发送成功")

        # 4. 保存数据到 Firebase
        report_record = {
            "timestamp": scan_result['timestamp'],
            "coins": scan_result['coins']
        }
        cycle_len = fb.add_report_to_cycle(report_record)
        logger.info(f"数据已保存，当前周期进度: {cycle_len}/{config.report_cycle}")

        # 5. 检查是否需要分析
        if cycle_len >= config.report_cycle:
            logger.info("达到周期，开始LS分析...")
            previous_reports = fb.get_current_cycle()
            
            # 分析
            analysis_results = LSAnalyzer.analyze(previous_reports)
            analysis_msg = LSAnalyzer.generate_report(analysis_results)
            
            # 发送分析报告
            monitor.send_telegram(analysis_msg)
            
            # 重置周期
            fb.reset_cycle()
            logger.info("周期已重置")

    except Exception as e:
        logger.error(f"执行出错: {e}", exc_info=True)
        # 可选：发送错误日志到简单的 TG 通知
        # requests.post(...) 

if __name__ == "__main__":
    main()
