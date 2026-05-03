"""
网格交易策略（Grid Trading）

策略逻辑：
- 以 SPY 当前价格为中心，上下各设 N 条网格线
- 价格每跌过一条网格线就买入一份，每涨过一条网格线就卖出一份
- 在震荡市中反复收割小利润，适合低波动区间

参数：
- 网格数量：上下各 5 条（共 10 条）
- 网格间距：2%（每条网格线间隔 2%）
- 每格仓位：总资金 / 网格数量

风险：
- 单边下跌时会持续买入（越买越跌）
- 单边上涨时过早卖光筹码
- 通过止损线（-10%）控制最大亏损

触发条件：
- 策略状态为 ENABLED（手动开启）
- 每个交易日检查是否触发网格

模拟模式：
- Paper Trading，按当前 SPY 价格模拟成交
"""

import logging
from datetime import datetime

from engine.alpha.base import AlphaStrategy
from db.database import Database

logger = logging.getLogger("xirang.alpha.grid_trading")


class GridTradingStrategy(AlphaStrategy):
    """
    网格交易策略。

    在 SPY 价格区间内设置等距网格，低买高卖。
    """

    STRATEGY_ID = "grid_trading"
    STRATEGY_NAME = "网格交易 (Grid Trading)"
    DESCRIPTION = "SPY 区间震荡网格，低买高卖自动收割。网格间距 2%，上下各 5 格。"
    DEFAULT_ALLOCATION = 0.05  # 默认 5%

    # 网格参数
    GRID_COUNT = 5             # 上下各 5 条线
    GRID_SPACING = 0.02        # 每格 2%
    STOP_LOSS_PCT = -0.10      # 止损 -10%

    def _run(self, portfolio_id: str, current_date: str, spy_price: float) -> dict:
        """
        每个交易日检查网格触发。

        逻辑：
        1. 首次运行：以当前价为基准建立网格，买入 50% 底仓
        2. 后续运行：检查价格是否穿越网格线，执行买入或卖出
        3. 止损检查：亏损超过 10% 平仓
        """
        if not self.is_enabled(portfolio_id):
            return {"action": "SKIP", "reason": "策略未启用"}

        strategy, alpha_account, capital = self.get_allocated_capital(portfolio_id)
        alpha_balance = float(alpha_account.get("cash_balance", 0.0))
        if alpha_balance <= 0:
            return {"action": "SKIP", "reason": "Alpha 独立账本余额为 0"}

        # 获取最近交易记录，判断是否初始化过
        recent_txs = self.db.get_alpha_transactions(self.STRATEGY_ID, portfolio_id=portfolio_id, limit=50)
        init_txs = [t for t in recent_txs if t["action"] == "GRID_INIT"]

        if not init_txs:
            return self._initialize_grid(portfolio_id, current_date, spy_price, capital, alpha_balance)

        # 已初始化：检查网格触发
        return self._check_grid(portfolio_id, current_date, spy_price, capital, strategy, recent_txs)

    def _initialize_grid(self, portfolio_id: str, current_date: str,
                         spy_price: float, capital: float, alpha_balance: float) -> dict:
        """首次运行：建立网格并买入底仓"""
        # 基准价格
        base_price = spy_price

        # 计算每格资金
        per_grid = capital / (self.GRID_COUNT * 2)

        # 买入 50% 底仓
        initial_shares = int((capital * 0.5) / spy_price)
        initial_cost = initial_shares * spy_price

        # 生成网格线
        grid_lines = []
        for i in range(1, self.GRID_COUNT + 1):
            grid_lines.append(round(base_price * (1 + self.GRID_SPACING * i), 2))   # 上方
            grid_lines.append(round(base_price * (1 - self.GRID_SPACING * i), 2))   # 下方
        grid_lines.sort()

        grid_info = f"基准={base_price:.2f}, 网格={grid_lines[0]:.2f}~{grid_lines[-1]:.2f}, 底仓={initial_shares}股"

        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="GRID_INIT",
            premium=0,
            pnl=0,
            underlying="SPY",
            spy_price=spy_price,
            strike=base_price,  # 复用 strike 存基准价
            contracts=initial_shares,  # 复用 contracts 存持仓股数
            detail=grid_info,
        )

        self.db.upsert_strategy(self.STRATEGY_ID, portfolio_id=portfolio_id, capital=capital)

        logger.info(f"网格初始化: {grid_info}")
        return {
            "action": "GRID_INIT",
            "base_price": base_price,
            "initial_shares": initial_shares,
            "grid_count": self.GRID_COUNT * 2,
            "capital": round(capital, 2),
            "alpha_balance": round(alpha_balance, 2),
        }

    def _check_grid(self, portfolio_id: str, current_date: str, spy_price: float,
                    capital: float, strategy: dict, recent_txs: list) -> dict:
        """检查价格是否穿越网格线"""
        # 找到初始化记录获取基准价
        init_tx = next((t for t in recent_txs if t["action"] == "GRID_INIT"), None)
        if not init_tx:
            return {"action": "ERROR", "reason": "找不到初始化记录"}

        base_price = init_tx.get("strike", spy_price)

        # 计算当前持仓（从交易记录重建）
        shares = 0
        total_cost = 0.0
        for t in reversed(recent_txs):
            if t["action"] == "GRID_INIT":
                shares = t.get("contracts", 0)
                total_cost = shares * base_price
            elif t["action"] == "GRID_BUY":
                qty = t.get("contracts", 0)
                shares += qty
                total_cost += qty * t.get("spy_price", spy_price)
            elif t["action"] == "GRID_SELL":
                qty = t.get("contracts", 0)
                shares -= qty
                total_cost -= qty * t.get("spy_price", spy_price) if shares > 0 else 0

        # 止损检查
        current_value = shares * spy_price
        invested = strategy.get("capital", capital) or capital
        pnl_pct = (current_value - invested) / invested if invested > 0 else 0
        if pnl_pct < self.STOP_LOSS_PCT and shares > 0:
            # 触发止损：全部卖出
            pnl = current_value - total_cost
            self.db.save_alpha_transaction(
                strategy_id=self.STRATEGY_ID,
                portfolio_id=portfolio_id,
                date=current_date,
                action="GRID_STOP",
                premium=0,
                pnl=pnl,
                underlying="SPY",
                spy_price=spy_price,
                contracts=shares,
                detail=f"止损触发: PnL {pnl_pct:.1%}, 卖出 {shares} 股",
            )
            logger.warning(f"网格止损: PnL {pnl_pct:.1%}")
            self._save_snapshot(portfolio_id, current_date, capital, strategy)
            return {"action": "GRID_STOP", "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 4)}

        # 计算当前价格在哪一格
        price_move = (spy_price - base_price) / base_price
        current_grid = int(price_move / self.GRID_SPACING)

        # 检查上次交易的网格位置
        last_trade_grid = self._get_last_grid_level(recent_txs, base_price)

        # 比较：是否穿越了新的网格线
        if current_grid == last_trade_grid:
            self._save_snapshot(portfolio_id, current_date, capital, strategy)
            return {"action": "HOLD", "reason": f"价格在同一格内（格位 {current_grid}）"}

        per_grid = capital / (self.GRID_COUNT * 2)
        qty = max(1, int(per_grid / spy_price))

        if current_grid < last_trade_grid:
            # 价格下跌穿越网格线 → 买入
            cost = qty * spy_price
            self.db.save_alpha_transaction(
                strategy_id=self.STRATEGY_ID,
                portfolio_id=portfolio_id,
                date=current_date,
                action="GRID_BUY",
                premium=0,
                pnl=0,
                underlying="SPY",
                spy_price=spy_price,
                contracts=qty,
                detail=f"格位 {last_trade_grid}→{current_grid}, 买入 {qty} 股 @ ${spy_price:.2f}",
            )
            logger.info(f"网格买入: {qty} 股 @ ${spy_price:.2f} (格位 {current_grid})")
            self._save_snapshot(portfolio_id, current_date, capital, strategy)
            return {"action": "GRID_BUY", "quantity": qty, "price": spy_price, "grid_level": current_grid}

        else:
            # 价格上涨穿越网格线 → 卖出
            sell_qty = min(qty, shares)
            if sell_qty <= 0:
                self._save_snapshot(portfolio_id, current_date, capital, strategy)
                return {"action": "HOLD", "reason": "无持仓可卖"}

            # 估算卖出利润（每格 2% 的价差）
            grid_profit = sell_qty * spy_price * self.GRID_SPACING
            self.db.save_alpha_transaction(
                strategy_id=self.STRATEGY_ID,
                portfolio_id=portfolio_id,
                date=current_date,
                action="GRID_SELL",
                premium=grid_profit,  # 用 premium 记录网格利润
                pnl=grid_profit,
                underlying="SPY",
                spy_price=spy_price,
                contracts=sell_qty,
                detail=f"格位 {last_trade_grid}→{current_grid}, 卖出 {sell_qty} 股 @ ${spy_price:.2f}, 利润 ${grid_profit:.2f}",
            )
            logger.info(f"网格卖出: {sell_qty} 股 @ ${spy_price:.2f}, 利润 ${grid_profit:.2f}")
            self._save_snapshot(portfolio_id, current_date, capital, strategy)
            return {"action": "GRID_SELL", "quantity": sell_qty, "price": spy_price,
                    "profit": round(grid_profit, 2), "grid_level": current_grid}

    def _get_last_grid_level(self, recent_txs: list, base_price: float) -> int:
        """获取上次交易的网格位置"""
        for t in recent_txs:
            if t["action"] in ("GRID_BUY", "GRID_SELL", "GRID_INIT"):
                last_price = t.get("spy_price", base_price)
                return int((last_price - base_price) / base_price / self.GRID_SPACING)
        return 0

    def _save_snapshot(self, portfolio_id: str, current_date: str, capital: float, strategy: dict):
        """保存每日快照"""
        total_pnl = strategy.get("total_pnl", 0)
        nav = capital + total_pnl
        initial = strategy.get("capital", capital) or capital
        cum_return = (nav / initial - 1) if initial > 0 else 0
        self.db.save_alpha_snapshot(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            capital=capital,
            nav=nav,
            cumulative_return=cum_return,
        )
