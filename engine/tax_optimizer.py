"""
息壤（Xi-Rang）税务优化引擎

Tax-Loss Harvesting（税损收割）：
在年末扫描所有持仓，找出亏损的资产，卖出后立即买入高度相关的替代品，
从而收割税损（用于抵扣当年的资本利得税），同时保持市场敞口不变。

核心规则：
1. 只在年末（12月）执行
2. 只收割亏损 >= 5% 的资产
3. 替代品必须与原资产相关性 > 0.95
4. 遵守 Wash Sale Rule：30 天后才能换回原资产
5. 仅适用于美股账户（中国市场无资本利得税）
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from db.database import Database
from engine.insurance import (
    InsuranceDecision,
    build_authority_decision,
    build_missing_authority_decision,
    coerce_insurance_state,
)

logger = logging.getLogger("xirang.tax_optimizer")


# 高相关性 ETF 替代品映射表
SUBSTITUTE_MAP = {
    "SPY": ["VOO", "IVV"],          # 标普500：Vanguard S&P 500, iShares Core S&P 500
    "TLT": ["VGLT", "SPTL"],        # 长期国债：Vanguard Long-Term Treasury, SPDR Long-Term Treasury
    "GLD": ["IAU", "GLDM"],         # 黄金：iShares Gold Trust, SPDR Gold MiniShares
    "SHV": ["BIL", "SGOV"],         # 短期国债：SPDR 1-3 Month T-Bill, iShares 0-3 Month Treasury
}


@dataclass
class HarvestablePosition:
    """可收割的亏损持仓"""
    asset: str                      # 资产代码
    quantity: float                 # 持仓数量
    cost_basis: float               # 成本基础（总成本）
    current_value: float            # 当前市值
    unrealized_loss: float          # 未实现损失
    loss_pct: float                 # 亏损百分比
    substitute: str                 # 推荐的替代品
    purchase_date: str              # 购买日期


@dataclass
class HarvestResult:
    """税损收割执行结果"""
    success: bool
    harvested_positions: list[HarvestablePosition]
    total_loss_harvested: float
    estimated_tax_saved: float      # 预估节省的税款（按 20% 资本利得税率）
    message: str


class TaxLossHarvester:
    """税损收割引擎"""

    def __init__(self, db: Database, min_loss_pct: float = 0.05):
        """
        Args:
            db: 数据库实例
            min_loss_pct: 最小亏损百分比（默认 5%）
        """
        self.db = db
        self.min_loss_pct = min_loss_pct

    def _latest_insurance_authority(self, portfolio_id: str) -> tuple[InsuranceDecision, str | None]:
        latest = self.db.get_latest_insurance_decision(portfolio_id)
        if latest:
            decision = build_authority_decision(
                coerce_insurance_state(latest.get("new_state")),
                reasons=latest.get("reasons", []),
            )
            return decision, latest.get("id")
        return build_missing_authority_decision("missing persisted InsuranceDecision"), None

    def _block_tax_harvest(self, portfolio_id: str, decision: InsuranceDecision, decision_id: str | None) -> str:
        insurance_ref = decision_id or "missing"
        message = (
            "Insurance Layer blocked tax-loss harvest "
            f"for {portfolio_id}: {decision.state.value}"
        )
        try:
            self.db.save_audit_log(
                "TAX_HARVEST_BLOCKED",
                "tax_optimizer",
                f"{message} | InsuranceDecision={insurance_ref}",
            )
        except Exception as exc:
            logger.warning(f"税损收割阻断审计写入失败: {exc}")
        logger.warning(f"{message} ({'; '.join(decision.reasons)})")
        return "Insurance Layer blocked tax-loss harvest"

    def enforce_tax_harvest_authority(self, portfolio_id: str) -> tuple[bool, InsuranceDecision, str | None, str]:
        decision, decision_id = self._latest_insurance_authority(portfolio_id)
        if not decision.allow_tax_harvest:
            reason = self._block_tax_harvest(portfolio_id, decision, decision_id)
            return False, decision, decision_id, reason
        return True, decision, decision_id, "Insurance Layer allowed tax-loss harvest"

    def scan_harvestable_losses(
        self,
        portfolio_id: str,
        current_prices: dict[str, float],
    ) -> list[HarvestablePosition]:
        """
        扫描可收割的税损。

        Args:
            portfolio_id: 组合 ID
            current_prices: 当前价格 {"SPY": 550.0, ...}

        Returns:
            可收割的持仓列表
        """
        harvestable = []

        for asset, current_price in current_prices.items():
            # 获取成本基础（FIFO：先进先出）
            cost_basis_record = self.db.get_cost_basis(asset, portfolio_id)

            if not cost_basis_record:
                logger.debug(f"资产 {asset} 无成本基础记录，跳过")
                continue

            quantity = cost_basis_record["quantity"]
            cost_per_share = cost_basis_record["cost_per_share"]
            total_cost = cost_basis_record["total_cost"]
            purchase_date = cost_basis_record["purchase_date"]

            # 计算当前市值和未实现损失
            current_value = quantity * current_price
            unrealized_loss = current_value - total_cost
            loss_pct = unrealized_loss / total_cost

            # 判断是否满足收割条件
            if loss_pct <= -self.min_loss_pct:
                # 找到替代品
                substitute = self._find_best_substitute(asset)

                if substitute:
                    harvestable.append(HarvestablePosition(
                        asset=asset,
                        quantity=quantity,
                        cost_basis=total_cost,
                        current_value=current_value,
                        unrealized_loss=unrealized_loss,
                        loss_pct=loss_pct,
                        substitute=substitute,
                        purchase_date=purchase_date,
                    ))
                    logger.info(
                        f"发现可收割税损: {asset} 亏损 {loss_pct:.2%} "
                        f"(${unrealized_loss:,.2f}), 替代品: {substitute}"
                    )

        return harvestable

    def _find_best_substitute(self, asset: str) -> Optional[str]:
        """
        找到最佳替代品。

        Args:
            asset: 原资产代码

        Returns:
            替代品代码，如果没有则返回 None
        """
        substitutes = SUBSTITUTE_MAP.get(asset, [])
        if not substitutes:
            logger.warning(f"资产 {asset} 没有配置替代品")
            return None

        # 简单策略：返回第一个替代品
        # 未来可以扩展：检查替代品的流动性、费率等
        return substitutes[0]

    def execute_harvest(
        self,
        position: HarvestablePosition,
        current_date: str,
        portfolio_id: str = "us",
    ) -> bool:
        """
        执行税损收割：卖出亏损资产，买入替代品。

        Args:
            position: 可收割的持仓
            current_date: 当前日期
            portfolio_id: 组合 ID

        Returns:
            是否成功
        """
        try:
            allowed, _, _, _ = self.enforce_tax_harvest_authority(portfolio_id)
            if not allowed:
                return False

            # 计算 Wash Sale 安全日期（30 天后）
            current_dt = datetime.strptime(current_date, "%Y-%m-%d")
            washsale_safe_date = (current_dt + timedelta(days=31)).strftime("%Y-%m-%d")

            # 记录税损收割事件
            self.db.save_harvest_event(
                date=current_date,
                sold_asset=position.asset,
                substitute_asset=position.substitute,
                quantity=position.quantity,
                cost_basis=position.cost_basis,
                sale_price=position.current_value / position.quantity,
                loss_harvested=abs(position.unrealized_loss),
                washsale_safe_date=washsale_safe_date,
                portfolio_id=portfolio_id,
            )

            logger.info(
                f"税损收割执行成功: 卖出 {position.asset} {position.quantity:.2f} 股, "
                f"买入 {position.substitute}, 收割税损 ${abs(position.unrealized_loss):,.2f}"
            )

            return True

        except Exception as e:
            logger.error(f"税损收割执行失败: {e}")
            return False

    def check_and_reverse_harvests(
        self,
        current_date: str,
        portfolio_id: str = "us",
    ) -> int:
        """
        检查并换回已过 Wash Sale 期的税损收割。

        Args:
            current_date: 当前日期
            portfolio_id: 组合 ID

        Returns:
            换回的数量
        """
        pending_reversals = self.db.get_pending_reversals(current_date, portfolio_id)

        if not pending_reversals:
            return 0

        reversed_count = 0
        for event in pending_reversals:
            try:
                # 标记为已换回
                self.db.mark_harvest_reversed(event["id"], current_date)

                logger.info(
                    f"税损收割换回: {event['substitute_asset']} → {event['sold_asset']} "
                    f"(收割日期: {event['date']})"
                )

                reversed_count += 1

            except Exception as e:
                logger.error(f"税损收割换回失败: {e}")

        return reversed_count

    def run_year_end_harvest(
        self,
        portfolio_id: str,
        current_prices: dict[str, float],
        current_date: str,
    ) -> HarvestResult:
        """
        执行年末税损收割（主入口）。

        Args:
            portfolio_id: 组合 ID
            current_prices: 当前价格
            current_date: 当前日期

        Returns:
            收割结果
        """
        allowed, decision, decision_id, reason = self.enforce_tax_harvest_authority(portfolio_id)
        if not allowed:
            return HarvestResult(
                success=False,
                harvested_positions=[],
                total_loss_harvested=0.0,
                estimated_tax_saved=0.0,
                message=f"{reason}: {decision.state.value} | InsuranceDecision={decision_id or 'missing'}",
            )

        # 1. 扫描可收割的税损
        harvestable = self.scan_harvestable_losses(portfolio_id, current_prices)

        if not harvestable:
            return HarvestResult(
                success=True,
                harvested_positions=[],
                total_loss_harvested=0.0,
                estimated_tax_saved=0.0,
                message="未发现可收割的税损",
            )

        # 2. 执行收割
        harvested = []
        for position in harvestable:
            if self.execute_harvest(position, current_date, portfolio_id):
                harvested.append(position)

        # 3. 计算总收割金额和预估节税
        total_loss = sum(abs(p.unrealized_loss) for p in harvested)
        estimated_tax_saved = total_loss * 0.20  # 假设 20% 资本利得税率

        # 4. 更新年度税务报告
        year = datetime.strptime(current_date, "%Y-%m-%d").year
        self.db.update_annual_tax_report(
            year=year,
            portfolio_id=portfolio_id,
            total_harvested_losses=total_loss,
            estimated_tax_saved=estimated_tax_saved,
            harvest_count=len(harvested),
        )

        return HarvestResult(
            success=True,
            harvested_positions=harvested,
            total_loss_harvested=total_loss,
            estimated_tax_saved=estimated_tax_saved,
            message=f"成功收割 {len(harvested)} 个税损，总计 ${total_loss:,.2f}，预估节税 ${estimated_tax_saved:,.2f}",
        )
