import os
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
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
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.proxies = []
        self.proxy_index = 0

    def get_public_proxies(self):
        """从公共源获取最新代理列表"""
        if self.proxies: return
        try:
            logger.info("正在获取公共代理列表...")
            # 使用 reliable 的 GitHub 代理列表源
            url = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                # 只取前50个，避免太久
                all_proxies = resp.text.splitlines()[:50]
                self.proxies = [{"http": f"http://{p}", "https": f"http://{p}"} for p in all_proxies]
                logger.info(f"成功获取 {len(self.proxies)} 个代理")
        except Exception as e:
            logger.error(f"获取代理失败: {e}")

    def request_with_retry(self, url):
        """带代理重试的请求封装"""
        # 1. 先尝试直连
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                # 检查是否是 API 错误响应 (dict 且包含 code/msg)
                if isinstance(data, dict) and ('code' in data or 'msg' in data):
                     # 如果是 IP 限制，抛出异常进入代理重试
                     if "restricted" in str(data.get('msg', '')):
                         raise ValueError("IP Restricted")
                return data
        except Exception as e:
            logger.warning(f"直连失败 ({e})，尝试使用代理...")

        # 2. 直连失败，准备代理
        self.get_public_proxies()
        
        # 3. 遍历代理尝试
        max_retries = 10  # 最多试10个代理
        for i in range(min(len(self.proxies), max_retries)):
            proxy = self.proxies[i]
            try:
                logger.info(f"正在尝试代理 [{i+1}/{max_retries}]...")
                resp = requests.get(url, proxies=proxy, timeout=5)
                if resp.status_code == 200:
                    data = resp.json()
                     # 再次检查内容有效性
                    if isinstance(data, dict) and 'code' in data:
                        continue # 这个代理也被墙了，换下一个
                    return data
            except:
                continue
        
        # 全都失败
        return None

    def get_real_oi_growth(self, symbol: str):
        try:
            # 获取当前OI
            oi_resp = self.request_with_retry(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}")
            if not oi_resp or 'openInterest' not in oi_resp:
                return 0, 0, 1.0
            oi_now = float(oi_resp['openInterest'])
            
            # 获取历史OI
            hist_url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={symbol}&period=5m&limit=7"
            hist_resp = self.request_with_retry(hist_url)
            
            if not hist_resp or not isinstance(hist_resp, list):
                return oi_now, 0, 1.0

            oi_30m_ago = float(hist_resp[0]['sumOpenInterest'])
            oi_growth = ((oi_now - oi_30m_ago) / oi_30m_ago) * 100 if oi_30m_ago > 0 else 0

            # LS Ratio
            ls_url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period=30m&limit=1"
            ls_resp = self.request_with_retry(ls_url)
            ls_ratio = float(ls_resp[0]['longShortRatio']) if ls_resp else 1.0

            return oi_now, oi_growth, ls_ratio
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return 0, 0, 1.0

    def get_cvd_30m_usdt(self, symbol: str):
        """计算过去30分钟的主动买卖净差值 (CVD)，以 USDT 计价"""
        try:
            # 获取过去30分钟的 5m K线 (limit=6)
            url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=5m&limit=6"
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

    def scan_and_collect(self) -> Dict:
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
                "timestamp": datetime.now().isoformat()
            }
        
        if not p_resp or not isinstance(p_resp, list):
             return {
                "message": f"⚠️ 扫描失败: 资金费率API连接错误",
                "coins": {},
                "timestamp": datetime.now().isoformat()
            }

        premiums = {p['symbol']: p for p in p_resp}

        # 筛选USDT活跃交易对
        active_tickers = sorted(
            [t for t in t_resp if t['symbol'].endswith("USDT")],
            key=lambda x: float(x['quoteVolume']),
            reverse=True
        )[:50]

        all_metrics = []
        structured_coins = {} # 用于存入数据库

        for t in active_tickers:
            s = t['symbol']
            oi_val, oi_chg, ls = self.get_real_oi_growth(s)
            cvd_usdt = self.get_cvd_30m_usdt(s)
            funding = float(premiums[s]['lastFundingRate']) * 100 if s in premiums else 0
            
            data_point = {
                "symbol": s,
                "price_chg": float(t['priceChangePercent']),
                "oi_chg": oi_chg,
                "ls": ls,
                "cvd_usdt": cvd_usdt,
                "funding": funding
            }
            all_metrics.append(data_point)

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
        msg = f"🛰️ **【{beijing_time.strftime('%H:%M')} 真实持仓扫描 (GHA版)】**\n\n"
        
        msg += "💎 **低位埋伏 (横盘+OI增+大户多+CVD净买入)**\n"
        if not accumulation: msg += "• 暂无匹配\n"
        for d in accumulation:
            cvd_str = format_usd(d['cvd_usdt'])
            msg += f"• `{d['symbol']}`: OI:+{d['oi_chg']:.1f}% | LS:{d['ls']:.2f} | CVD:{cvd_str}\n"
            structured_coins[d['symbol']] = {"ls_value": d['ls'], "section": "accumulation", "extra_info": ""}

        msg += "\n📈 **30min OI 爆增榜**\n"
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
            "timestamp": datetime.now().isoformat()
        }

    def send_telegram(self, text):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        requests.post(url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"})

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

# ==================== 主入口 ====================
def main():
    try:
        config = Config()
        fb = FirebaseManager(config.firebase_creds_json)
        monitor = OIMonitor(config.bot_token, config.chat_id)

        # 1. 扫描并发送 OI 报告
        scan_result = monitor.scan_and_collect()
        monitor.send_telegram(scan_result['message'])
        logger.info("OI 报告发送成功")

        # 2. 保存数据到 Firebase
        report_record = {
            "timestamp": scan_result['timestamp'],
            "coins": scan_result['coins']
        }
        cycle_len = fb.add_report_to_cycle(report_record)
        logger.info(f"数据已保存，当前周期进度: {cycle_len}/{config.report_cycle}")

        # 3. 检查是否需要分析
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
