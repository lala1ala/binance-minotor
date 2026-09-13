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
        try:
            # 获取当前OI
            oi_resp = self.request_with_retry(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
            if not oi_resp or 'openInterest' not in oi_resp:
                return 0, 0, 1.0
            oi_now = float(oi_resp['openInterest'])
            
            # 获取历史OI（过去2小时）
            hist_url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=2h&limit=2"
            hist_resp = self.request_with_retry(hist_url)
            
            # 注意：空列表也要挡住，否则下面的 hist_resp[0] 会抛 IndexError
            if not isinstance(hist_resp, list) or not hist_resp:
                return oi_now, 0, 1.0

            oi_2h_ago = float(hist_resp[0]['sumOpenInterest'])
            oi_growth = ((oi_now - oi_2h_ago) / oi_2h_ago) * 100 if oi_2h_ago > 0 else 0

            # LS Ratio（过去2小时）
            ls_url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period=2h&limit=1"
            ls_resp = self.request_with_retry(ls_url)
            ls_ratio = 1.0
            if isinstance(ls_resp, list) and ls_resp:
                ls_ratio = float(ls_resp[0]['longShortRatio'])

            return oi_now, oi_growth, ls_ratio
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return 0, 0, 1.0

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

    def _collect_one(self, ticker: Dict, premium: Optional[Dict]) -> Optional[Dict]:
        """采集单个交易对的全部指标（供并发调用）。失败返回 None。"""
        s = ticker['symbol']
        try:
            oi_val, oi_chg, ls = self.get_real_oi_growth(s)
            cvd_usdt = self.get_cvd_2h_usdt(s)
            funding = float(premium['lastFundingRate']) * 100 if premium else 0
            return {
                "symbol": s,
                "price": float(ticker['lastPrice']),
                "price_chg": float(ticker['priceChangePercent']),
                "oi_value": oi_val,
                "oi_chg": oi_chg,
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
        # 低位埋伏: 价格未暴涨(-2%到5%), OI增加, 大户多, 且CVD纯买入>0
        accumulation = [d for d in all_metrics if -2 < d['price_chg'] < 5 and d['oi_chg'] > 1.5 and d['ls'] > 1.2 and d['cvd_usdt'] > 0]
        top_oi = sorted(all_metrics, key=lambda x: x['oi_chg'], reverse=True)[:5]
        ext_neg = sorted([d for d in all_metrics if d['funding'] < 0], key=lambda x: x['funding'])[:3]
        ext_pos = sorted([d for d in all_metrics if d['funding'] > 0], key=lambda x: x['funding'], reverse=True)[:3]

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
        msg += "\n\n"
        
        msg += "💎 **低位埋伏 (横盘+OI增+大户多+CVD净买入)**\n"
        if not accumulation: msg += "• 暂无匹配\n"
        for d in accumulation:
            cvd_str = format_usd(d['cvd_usdt'])
            msg += f"• `{d['symbol']}`: OI:+{d['oi_chg']:.1f}% | LS:{d['ls']:.2f} | CVD:{cvd_str}\n"
            structured_coins[d['symbol']] = {"ls_value": d['ls'], "section": "accumulation", "extra_info": ""}

        msg += "\n📈 **2h OI 爆增榜**\n"
        for d in top_oi:
            cvd_str = format_usd(d['cvd_usdt'])
            msg += f"• `{d['symbol']}`: OI:+{d['oi_chg']:.1f}% | CVD:{cvd_str} | LS:{d['ls']:.2f}\n"
            # 如果币种重复，优先保留accumulation的分类，否则覆盖
            if d['symbol'] not in structured_coins:
                structured_coins[d['symbol']] = {"ls_value": d['ls'], "section": "top_oi", "extra_info": f"F:{d['funding']:.3f}%"}

        msg += "\n☢️ **极端费率**\n"
        for d in ext_neg:
            msg += f"• `{d['symbol']}` (负): `{d['funding']:.3f}%` | LS:{d['ls']:.2f}\n"
        for d in ext_pos:
            msg += f"• `{d['symbol']}` (正): `{d['funding']:.3f}%` | LS:{d['ls']:.2f}\n"

        return {
            "message": msg,
            "coins": structured_coins,
            "all_metrics": all_metrics,
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
    """OI持续升温追踪器：按天聚合存储快照，并检测左侧慢牛候选"""

    COLLECTION = 'oi_warmup_tracker'
    DOC_ID = 'daily_snapshots'
    MAX_DAYS = 5
    MIN_DAYS = 3

    def __init__(self, db):
        self.db = db
        self.doc_ref = self.db.collection(self.COLLECTION).document(self.DOC_ID)

    def store_daily_snapshot(self, all_metrics: List[Dict]) -> None:
        """存储当天所有币种快照，按天聚合（保留最后一次），仅保留最近5天"""
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
        """检测 OI 持续升温：过去5天 OI 环比上涨天数>=2（按增长，不看绝对规模），且总趋势向上"""
        snapshots = sorted(snapshots, key=lambda x: x.get('date', ''))
        recent = snapshots[-self.MAX_DAYS:]
        if len(recent) < self.MIN_DAYS:
            return []

        latest_data = recent[-1].get('data', {})
        results = []

        for symbol in latest_data:
            oi_series = []
            price_series = []

            for s in recent:
                d = s.get('data', {})
                if symbol in d:
                    oi_series.append(d[symbol].get('oi', 0))
                    price_series.append(d[symbol].get('price', 0))

            if len(oi_series) < self.MIN_DAYS:
                continue

            # 触发：至少2天 OI 环比为正 + 末日 > 首日（净增长）
            up_days = sum(1 for i in range(1, len(oi_series)) if oi_series[i] > oi_series[i - 1])
            if up_days < 2:
                continue
            first_oi, last_oi = oi_series[0], oi_series[-1]
            if first_oi <= 0 or last_oi <= first_oi:
                continue

            oi_change = (last_oi - first_oi) / first_oi * 100

            # 确认加分：价格同步上涨（OI+Price 同向）
            price_up = len(price_series) >= 2 and price_series[0] > 0 and price_series[-1] > price_series[0]

            # 减分/排除：价格距5日低点 > 15%（追高风险）
            chase_high = False
            if price_series:
                low = min(price_series)
                if low > 0:
                    chase_high = (price_series[-1] - low) / low * 100 > 15

            results.append({
                "symbol": symbol,
                "up_days": up_days,
                "total_days": len(recent),
                "oi_change": oi_change,
                "price_up": price_up,
                "chase_high": chase_high,
                "ls": latest_data[symbol].get('ls', 1.0),
                "cvd": latest_data[symbol].get('cvd', 0),
                "fr": latest_data[symbol].get('fr', 0),
            })

        results.sort(key=lambda x: x['oi_change'], reverse=True)
        return results

    def format_oi_sustained_growth_message(self, results: List[Dict]) -> str:
        if not results:
            return ""

        clean = [r for r in results if not r['chase_high']]
        chase = [r for r in results if r['chase_high']]

        parts = []
        if clean:
            parts.append(f"🔥 **【OI持续升温】**\n发现 {len(clean)} 个候选：\n")
            for r in clean:
                tag = "共振确认" if r['price_up'] else "价格未确认"
                parts.append(f"• `{r['symbol']}` 🔥持续升温+{tag}")
                parts.append(f"  OI连续上涨 {r['up_days']} 天 | OI {r['oi_change']:+.1f}%")
                parts.append(f"  费率:{r['fr']:.3f}%")
                parts.append("")

        if chase:
            parts.append(f"⚠️ **【追高风险】** 价格已从5日低点涨>15%：\n")
            for r in chase:
                parts.append(f"• `{r['symbol']}` OI上涨 {r['up_days']} 天 | OI {r['oi_change']:+.1f}%")
            parts.append("")

        return "\n".join(parts).rstrip()

    def oi_sustained_growth_scan(self, all_metrics: List[Dict]) -> str:
        """存储每日快照并检测 OI 持续升温，返回 Telegram 消息片段"""
        self.store_daily_snapshot(all_metrics)
        snapshots = self.get_history()
        results = self.detect_oi_sustained_growth(snapshots)
        return self.format_oi_sustained_growth_message(results)

# ==================== 主入口 ====================
def main():
    try:
        mode = sys.argv[1] if len(sys.argv) > 1 else 'report'
        config = Config()
        fb = FirebaseManager(config.firebase_creds_json)
        monitor = OIMonitor(config.bot_token, config.chat_id)

        if mode == 'warmup':
            # 升温模式：轻扫 >$5M 池子，存快照并检测 OI 持续升温
            all_metrics = monitor.scan_light(threshold=5_000_000)
            if not all_metrics:
                logger.error("轻量扫描失败，跳过本次升温检测")
                return
            warmup_msg = WarmupTracker(fb.db).oi_sustained_growth_scan(all_metrics)
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
