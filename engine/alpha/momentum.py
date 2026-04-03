"""
动量轮动策略（Momentum Rotation）

策略逻辑：
- 跟踪 SPY（股票）和 TLT（债券）的近期动量
- 每月评估：哪个资产近 N 天涨幅更好，就持有哪个
- 经典的"股债动量切换"：追涨杀跌，趋势跟踪

参数：
- 动量窗口：20 个交易日（约 1 个月）
- 切换条件：当前持有资产动量 < 对手资产动量 - 缓冲区
- 缓冲区：1%（避免频繁切换）

风险：
- 震荡市频繁切换导致摩擦成本
- 趋势反转时反应滞后
- 通过 1% 缓冲区 + 每月最多切换 1 次来控制

触发条件：
- 策略状态为 ENABLED（手动开启）
- 每月第一个交易日评估并执行

模拟模式：
- Paper Trading，使用历史日回报模拟动量
"""

import logging
from datetime import datetime

from engine.alpha.base import AlphaStrategy
from db.database import Database

logger = logging.getLogger("xirang.alpha.momentum")


def estimate_momentum(price: float, base_price: float) -> float:
    """估算动量（简化：基于价格变动百分比）"""
    if base_price <= 0:
        return 0.0
    return (price - base_price) / base_price


class MomentumRotationStrategy(AlphaStrategy):
    """
    动量轮动策略。

    在 SPY 和 TLT 之间切换，追踪近期表现更好的资产。
    """

    STRATEGY_ID = "momentum_rotation"
    STRATEGY_NAME = "动量轮动 (Momentum Rotation)"
    DESCRIPTION = "SPY/TLT 动量切换，追涨趋势资产。每月评估，1% 缓冲区防频繁切换。"
    DEFAULT_ALLOCATION = 0.05  # 默认 5%

    # 动量参数
    MOMENTUM_WINDOW = 20       # 20 日动量窗口
    SWITCH_BUFFER = 0.01       # 1% 缓冲区
    # 模拟 TLT 价格与 SPY 的关系
    TLT_SPY_RATIO = 0.20       # 假设 TLT ≈ SPY * 0.20（用于模拟）

    def run(self, portfolio_id: str, current_date: str, spy_price: float, nav: float) -> dict:
        """
        每月评估动量并执行轮动。

        逻辑：
        1. 首次运行：默认持有 SPY（股票动量通常更强）
        2. 后续运行：每月初比较 SPY vs TLT 动量，必要时切换
        """
        if not self.is_enabled(portfolio_id):
            return {"action": "SKIP", "reason": "策略未启用"}

        strategy = self.db.get_strategy(self.STRATEGY_ID)
        allocation_pct = strategy.get("allocation_pct", self.DEFAULT_ALLOCATION)
        capital = nav * allocation_pct

        # 检查是否月初（日期 <= 5）
        dt = datetime.strptime(current_date, "%Y-%m-%d")
        if dt.day > 5:
            self._save_snapshot(current_date, capital, spy_price, strategy)
            return {"action": "HOLD", "reason": "非月初，等待下次评估"}

        # 检查本月是否已评估
        recent_txs = self.db.get_alpha_transactions(self.STRATEGY_ID, limit=1)
        if recent_txs and recent_txs[0]["date"][:7] == current_date[:7]:
            return {"action": "SKIP", "reason": "本月已完成评估"}

        # 获取历史交易判断当前持仓
        all_txs = self.db.get_alpha_transactions(self.STRATEGY_ID, limit=50)
        current_holding = self._get_current_holding(all_txs)
        last_entry_price = self._get_last_entry_price(all_txs)

        if current_holding is None:
            # 首次运行：默认买入 SPY
            return self._enter_position(portfolio_id, current_date, "SPY",
                                        spy_price, capital, "首次建仓，默认持有 SPY")

        # 模拟 TLT 价格（实盘需替换为真实数据）
        tlt_price = spy_price * self.TLT_SPY_RATIO

        # 估算动量
        # 简化：用当前价格 vs 上次入场价格的变动作为动量代理
        if current_holding == "SPY":
            current_momentum = estimate_momentum(spy_price, last_entry_price)
            alt_momentum = self._estimate_alt_momentum(current_date, tlt_price)
            alt_asset = "TLT"
        else:
            current_momentum = estimate_momentum(tlt_price, last_entry_price)
            alt_momentum = self._estimate_alt_momentum(current_date, spy_price)
            alt_asset = "SPY"

        # 判断是否需要切换
        if alt_momentum > current_momentum + self.SWITCH_BUFFER:
            # 切换：卖出当前，买入对手
            return self._switch_position(
                portfolio_id, current_date,
                from_asset=current_holding,
                to_asset=alt_asset,
                spy_price=spy_price,
                capital=capital,
                current_momentum=current_momentum,
                alt_momentum=alt_momentum,
            )

        # 不切换：保持持有
        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="HOLD",
            premium=0,
            pnl=0,
            underlying=current_holding,
            spy_price=spy_price,
            detail=f"保持 {current_holding}，动量 {current_momentum:+.2%} vs {alt_asset} {alt_momentum:+.2%}",
        )

        self._save_snapshot(current_date, capital, spy_price, strategy)
        return {
            "action": "HOLD",
            "holding": current_holding,
            "current_momentum": round(current_momentum, 4),
            "alt_momentum": round(alt_momentum, 4),
            "reason": f"{current_holding} 动量 {current_momentum:+.2%} 仍优于 {alt_asset} {alt_momentum:+.2%}",
        }

    def _enter_position(self, portfolio_id: str, current_date: str, asset: str,
                        spy_price: float, capital: float, reason: str) -> dict:
        """建仓：买入指定资产"""
        price = spy_price if asset == "SPY" else spy_price * self.TLT_SPY_RATIO
        shares = int(capital / price)

        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="MOM_BUY",
            premium=0,
            pnl=0,
            underlying=asset,
            spy_price=spy_price,
            contracts=shares,
            strike=price,  # 复用 strike 存入场价
            detail=f"{reason}，买入 {asset} {shares} 股 @ ${price:.2f}",
        )

        self.db.upsert_strategy(self.STRATEGY_ID, capital=capital)
        logger.info(f"动量建仓: {asset} {shares} 股 @ ${price:.2f}")

        return {
            "action": "MOM_BUY",
            "asset": asset,
            "shares": shares,
            "price": price,
            "capital": round(capital, 2),
        }

    def _switch_position(self, portfolio_id: str, current_date: str,
                         from_asset: str, to_asset: str, spy_price: float,
                         capital: float, current_momentum: float, alt_momentum: float) -> dict:
        """切换持仓：卖出旧资产，买入新资产"""
        from_price = spy_price if from_asset == "SPY" else spy_price * self.TLT_SPY_RATIO
        to_price = spy_price if to_asset == "SPY" else spy_price * self.TLT_SPY_RATIO

        # 计算卖出利润
        last_entry = self._get_last_entry_price(self.db.get_alpha_transactions(self.STRATEGY_ID, limit=50))
        sell_pnl = 0
        if last_entry > 0:
            sell_pnl = (from_price - last_entry) / last_entry * capital

        # 卖出
        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="MOM_SELL",
            premium=max(sell_pnl, 0),
            pnl=sell_pnl,
            underlying=from_asset,
            spy_price=spy_price,
            strike=from_price,
            detail=f"轮动: {from_asset}({current_momentum:+.2%}) → {to_asset}({alt_momentum:+.2%}), PnL ${sell_pnl:,.2f}",
        )

        # 买入
        shares = int(capital / to_price)
        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="MOM_BUY",
            premium=0,
            pnl=0,
            underlying=to_asset,
            spy_price=spy_price,
            contracts=shares,
            strike=to_price,
            detail=f"买入 {to_asset} {shares} 股 @ ${to_price:.2f}",
        )

        logger.info(f"动量轮动: {from_asset} → {to_asset}, PnL ${sell_pnl:,.2f}")

        strategy = self.db.get_strategy(self.STRATEGY_ID)
        self._save_snapshot(current_date, capital, spy_price, strategy)

        return {
            "action": "MOM_SWITCH",
            "from": from_asset,
            "to": to_asset,
            "sell_pnl": round(sell_pnl, 2),
            "new_shares": shares,
            "new_price": to_price,
            "current_momentum": round(current_momentum, 4),
            "alt_momentum": round(alt_momentum, 4),
        }

    def _get_current_holding(self, txs: list) -> str:
        """从交易记录推断当前持有资产"""
        for t in txs:
            if t["action"] in ("MOM_BUY", "HOLD"):
                return t.get("underlying")
        return None

    def _get_last_entry_price(self, txs: list) -> float:
        """获取最近一次买入价格"""
        for t in txs:
            if t["action"] == "MOM_BUY":
                return t.get("strike", 0) or t.get("spy_price", 0)
        return 0

    def _estimate_alt_momentum(self, current_date: str, alt_price: float) -> float:
        """
        估算替代资产的动量。

        简化模拟：用随机扰动 + 均值回归。
        实盘需替换为真实历史价格计算。
        """
        # 用日期的 hash 产生确定性的伪随机动量
        dt = datetime.strptime(current_date, "%Y-%m-%d")
        seed = dt.year * 10000 + dt.month * 100 + dt.day
        # 简单伪随机：模拟 -3% ~ +5% 的月度动量
        pseudo = ((seed * 7919) % 1000) / 1000.0  # 0~1
        return (pseudo - 0.4) * 0.08  # -3.2% ~ +4.8%

    def _save_snapshot(self, current_date: str, capital: float, spy_price: float, strategy: dict):
        """保存每日快照"""
        total_pnl = strategy.get("total_pnl", 0) if strategy else 0
        nav = capital + total_pnl
        initial = strategy.get("capital", capital) if strategy else capital
        initial = initial or capital
        cum_return = (nav / initial - 1) if initial > 0 else 0
        self.db.save_alpha_snapshot(
            strategy_id=self.STRATEGY_ID,
            date=current_date,
            capital=capital,
            nav=nav,
            cumulative_return=cum_return,
        )
