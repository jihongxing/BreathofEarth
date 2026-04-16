"""
备兑期权策略（Covered Call）

策略逻辑：
- 持有 SPY（或等价 ETF），卖出虚值看涨期权
- 每月滚动一次：平掉旧期权，卖出新期权
- 收取权利金作为额外收益，目标年化 +3-5%

风险：
- 如果 SPY 大涨超过行权价，收益被封顶（Cap Risk）
 - 只动用 Alpha 独立账本内的分配资金，不影响 Core/Stability

触发条件：
- 策略状态为 ENABLED（手动开启）
- 每月第一个交易日执行 monthly_roll

模拟模式：
- 当前为 Paper Trading，不连接真实券商
- 使用 Black-Scholes 简化估算权利金
"""

import math
import logging
from datetime import datetime, timedelta

from engine.alpha.base import AlphaStrategy
from db.database import Database

logger = logging.getLogger("xirang.alpha.covered_call")


def estimate_call_premium(
    spy_price: float, strike: float, days_to_expiry: int,
    volatility: float = 0.18, risk_free_rate: float = 0.05,
) -> float:
    """
    Black-Scholes 简化估算看涨期权权利金。

    这是 Paper Trading 用的近似值，实盘需替换为真实报价。
    """
    T = days_to_expiry / 365.0
    if T <= 0:
        return 0.0

    d1 = (math.log(spy_price / strike) + (risk_free_rate + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
    d2 = d1 - volatility * math.sqrt(T)

    # 标准正态 CDF 近似
    def norm_cdf(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    call_price = spy_price * norm_cdf(d1) - strike * math.exp(-risk_free_rate * T) * norm_cdf(d2)
    return max(call_price, 0.01)  # 至少 0.01


def next_monthly_expiry(current_date: str) -> tuple[str, int]:
    """
    计算下一个月度期权到期日（第三个周五）。

    Returns:
        (expiry_date_str, days_to_expiry)
    """
    dt = datetime.strptime(current_date, "%Y-%m-%d")
    # 目标：下个月的第三个周五
    if dt.month == 12:
        target_year, target_month = dt.year + 1, 1
    else:
        target_year, target_month = dt.year, dt.month + 1

    # 找第三个周五
    first_day = datetime(target_year, target_month, 1)
    # 周五 = weekday 4
    first_friday = first_day + timedelta(days=(4 - first_day.weekday()) % 7)
    third_friday = first_friday + timedelta(weeks=2)

    expiry_str = third_friday.strftime("%Y-%m-%d")
    days_to_expiry = (third_friday - dt).days
    return expiry_str, days_to_expiry


class CoveredCallStrategy(AlphaStrategy):
    """
    备兑期权策略。

    手动启用后，每月第一个交易日执行：
    1. 检查上月期权是否到期（模拟结算）
    2. 卖出新的虚值看涨期权（Delta ~0.3，约 5-10% OTM）
    3. 记录权利金收入
    """

    STRATEGY_ID = "covered_call"
    STRATEGY_NAME = "备兑期权 (Covered Call)"
    DESCRIPTION = "持有 SPY，卖出虚值看涨期权收取权利金。每月滚动，目标年化 +3-5%。"
    DEFAULT_ALLOCATION = 0.10

    # 期权参数
    OTM_PCT = 0.05          # 虚值幅度（5% OTM）
    IMPLIED_VOL = 0.18       # 假设隐含波动率
    CONTRACTS_PER_100 = 100  # 每张合约对应 100 股

    def run(self, portfolio_id: str, current_date: str, spy_price: float) -> dict:
        """
        执行备兑期权策略。

        只在每月第一个交易日执行 monthly_roll。
        策略必须处于 ENABLED 状态。
        """
        if not self.is_enabled(portfolio_id):
            return {"action": "SKIP", "reason": "策略未启用"}

        strategy, alpha_account, capital = self.get_allocated_capital(portfolio_id)
        alpha_balance = float(alpha_account.get("cash_balance", 0.0))
        if alpha_balance <= 0:
            return {"action": "SKIP", "reason": "Alpha 独立账本余额为 0"}

        # 检查是否是月初（日期 <= 5）
        dt = datetime.strptime(current_date, "%Y-%m-%d")
        if dt.day > 5:
            # 非月初：只记录快照
            self._save_daily_snapshot(portfolio_id, current_date, capital, spy_price, strategy)
            return {"action": "HOLD", "reason": "非月初，等待下次滚动"}

        # 检查本月是否已滚动过
        recent_txs = self.db.get_alpha_transactions(self.STRATEGY_ID, portfolio_id=portfolio_id, limit=1)
        if recent_txs and recent_txs[0]["date"][:7] == current_date[:7]:
            return {"action": "SKIP", "reason": "本月已完成滚动"}

        # 执行 monthly roll
        return self._monthly_roll(portfolio_id, current_date, spy_price, capital, alpha_balance)

    def _monthly_roll(self, portfolio_id: str, current_date: str, spy_price: float, capital: float, alpha_balance: float) -> dict:
        """月度滚动：结算旧期权 + 卖出新期权"""

        # 1. 结算上月期权（如果有）
        settle_result = self._settle_expiring(portfolio_id, current_date, spy_price)

        # 2. 计算新期权参数
        strike = round(spy_price * (1 + self.OTM_PCT), 2)
        expiry, days_to_expiry = next_monthly_expiry(current_date)

        # 3. 估算权利金
        premium_per_share = estimate_call_premium(
            spy_price, strike, days_to_expiry, self.IMPLIED_VOL,
        )

        # 4. 计算合约数量（capital 能买多少 SPY → 对应多少张合约）
        spy_shares = int(capital / spy_price)
        contracts = spy_shares // self.CONTRACTS_PER_100
        if contracts < 1:
            return {"action": "SKIP", "reason": f"资金不足（{capital:.0f}），需至少 {100 * spy_price:.0f}"}

        total_premium = premium_per_share * contracts * self.CONTRACTS_PER_100

        # 5. 记录交易
        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action="SELL_CALL",
            premium=total_premium,
            pnl=0,
            underlying="SPY",
            strike=strike,
            expiry=expiry,
            contracts=contracts,
            spy_price=spy_price,
            detail=f"OTM {self.OTM_PCT:.0%}, IV {self.IMPLIED_VOL:.0%}, DTE {days_to_expiry}",
        )

        # 6. 更新策略资金
        self.db.upsert_strategy(self.STRATEGY_ID, portfolio_id=portfolio_id, capital=capital)

        logger.info(
            f"备兑期权: SELL {contracts}x SPY {strike} Call @ {expiry}, "
            f"权利金 ${total_premium:,.2f} (${premium_per_share:.2f}/share)"
        )

        result = {
            "action": "SELL_CALL",
            "strike": strike,
            "expiry": expiry,
            "contracts": contracts,
            "premium_per_share": round(premium_per_share, 4),
            "total_premium": round(total_premium, 2),
            "spy_price": spy_price,
            "capital": round(capital, 2),
            "alpha_balance": round(alpha_balance, 2),
        }

        if settle_result:
            result["settlement"] = settle_result

        return result

    def _settle_expiring(self, portfolio_id: str, current_date: str, spy_price: float) -> dict:
        """结算到期的期权"""
        recent_txs = self.db.get_alpha_transactions(self.STRATEGY_ID, portfolio_id=portfolio_id, limit=5)
        sell_calls = [t for t in recent_txs if t["action"] == "SELL_CALL" and t.get("expiry")]

        if not sell_calls:
            return None

        last_call = sell_calls[0]
        expiry = last_call.get("expiry", "")
        if not expiry or current_date < expiry:
            return None  # 还未到期

        strike = last_call.get("strike", 0)
        contracts = last_call.get("contracts", 0)
        original_premium = last_call.get("premium", 0)

        if spy_price <= strike:
            # 期权作废（OTM），权利金全部落袋
            pnl = original_premium
            action = "EXPIRE"
            detail = f"期权到期作废，权利金 ${original_premium:,.2f} 全部保留"
        else:
            # 被行权（ITM），计算损失
            intrinsic = (spy_price - strike) * contracts * self.CONTRACTS_PER_100
            pnl = original_premium - intrinsic
            action = "ASSIGN"
            detail = f"被行权，内在价值 ${intrinsic:,.2f}，净损益 ${pnl:,.2f}"

        self.db.save_alpha_transaction(
            strategy_id=self.STRATEGY_ID,
            portfolio_id=portfolio_id,
            date=current_date,
            action=action,
            premium=0,
            pnl=pnl,
            underlying="SPY",
            strike=strike,
            expiry=expiry,
            contracts=contracts,
            spy_price=spy_price,
            detail=detail,
        )

        logger.info(f"备兑期权结算: {action} | {detail}")
        return {"action": action, "pnl": round(pnl, 2), "detail": detail}

    def _save_daily_snapshot(self, portfolio_id: str, current_date: str, capital: float, spy_price: float, strategy: dict):
        """保存每日快照（用于绩效评估）"""
        # 简化：用 capital + total_pnl 作为 nav
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
