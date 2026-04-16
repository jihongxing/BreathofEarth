"""
息壤（Xi-Rang）资金流引擎 — 严格三层模型

三层结构：
    Core（抗通胀层）：永久组合 [SPY, TLT, GLD, SHV] 等权 25%，不可破坏
    Stability（流动性层）：独立资金池，接收入金缓冲、提供出金资金
    Alpha（增长层）：由 alpha_strategies 独立管理，资金流引擎不触碰

入金流程：
    用户充值 → 100% 进入 Stability
    → 检测三层比例是否偏离目标
    → Core 占比 < 目标 → 从 Stability 转资金到 Core（Core 内部等权分配）
    → Stability 保持 ≥ 最低安全线

出金流程：
    优先从 Stability 扣 → 不足再卖 Core 超配资产
    → Alpha 永远不动
    → 出金后 Stability < 安全线 → 风控警告
"""

import json
import logging
import uuid
from datetime import datetime

import numpy as np

from db.database import Database
from engine.config import (
    PORTFOLIOS, WEIGHTS_IDLE, FEE_RATE,
    LAYER_TARGET_CORE, LAYER_TARGET_STABILITY,
    LAYER_MIN_STABILITY, LAYER_MAX_STABILITY, LAYER_TARGET_ALPHA,
)

logger = logging.getLogger("xirang.cashflow")

MIN_DEPOSIT = 100.0
MAX_DEPOSIT_NAV_RATIO = 5.0
MAX_DEPOSIT_HARD_CAP = 5_000_000.0
MAX_WITHDRAWAL_NAV_RATIO = 0.9


class CashflowResult:
    def __init__(self, status: str, message: str = "", **extra):
        self.status = status
        self.message = message
        self.extra = extra

    def to_dict(self) -> dict:
        d = {"status": self.status, "message": self.message}
        d.update(self.extra)
        return d


def _parse_positions(raw) -> list[float]:
    if isinstance(raw, str):
        return [float(p) for p in json.loads(raw)]
    return [float(p) for p in raw]


class CashflowEngine:

    def __init__(self, db: Database):
        self.db = db

    def _get_total_nav(self, core_sum: float, stability: float) -> float:
        return core_sum + stability

    def deposit_preview(
        self, amount: float, portfolio_id: str = "us"
    ) -> CashflowResult:
        """模拟入金分配方案，不写入数据库，供用户确认"""
        if amount < MIN_DEPOSIT:
            return CashflowResult("ERROR", f"最小入金额 ${MIN_DEPOSIT:,.0f}")

        if portfolio_id not in PORTFOLIOS:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 不存在")

        try:
            portfolio = self.db.get_portfolio(portfolio_id)
        except ValueError:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 未初始化")

        positions = _parse_positions(portfolio["positions"])
        stability = float(portfolio.get("stability_balance", 0.0))
        core_sum = sum(positions)
        old_nav = self._get_total_nav(core_sum, stability)

        max_deposit = min(old_nav * MAX_DEPOSIT_NAV_RATIO, MAX_DEPOSIT_HARD_CAP)
        if old_nav > 0 and amount > max_deposit:
            return CashflowResult("ERROR", f"单次入金上限 ${max_deposit:,.0f}")

        state = portfolio["state"]
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]
        asset_names = pf_config["asset_names"]

        # 模拟分配
        new_stability = stability + amount
        new_nav = self._get_total_nav(core_sum, new_stability)
        transfer_to_core = 0.0
        core_allocation = {}

        if state != "PROTECTION":
            core_ratio = core_sum / new_nav if new_nav > 0 else 0
            stability_ratio = new_stability / new_nav if new_nav > 0 else 0

            if core_ratio < LAYER_TARGET_CORE and stability_ratio > LAYER_TARGET_STABILITY:
                target_core = LAYER_TARGET_CORE * new_nav
                transfer_to_core = target_core - core_sum
                min_stability_keep = LAYER_TARGET_STABILITY * new_nav
                max_transferable = new_stability - min_stability_keep
                transfer_to_core = max(0, min(transfer_to_core, max_transferable))

                if transfer_to_core > 0:
                    sim_stability = new_stability - transfer_to_core
                    target_weights = np.array(WEIGHTS_IDLE, dtype=float)
                    current_positions = np.array(positions, dtype=float)
                    target_core_total = core_sum + transfer_to_core
                    target_positions = target_weights * target_core_total
                    deltas = target_positions - current_positions
                    positive_deltas = np.maximum(deltas, 0)
                    delta_sum = float(np.sum(positive_deltas))

                    if delta_sum > 1e-6:
                        core_add = positive_deltas / delta_sum * transfer_to_core
                        new_positions = (current_positions + core_add).tolist()
                    else:
                        equal_add = transfer_to_core / len(positions)
                        new_positions = [p + equal_add for p in positions]

                    core_allocation = {
                        assets[i]: {
                            "name": asset_names[assets[i]],
                            "add": round(new_positions[i] - positions[i], 2),
                            "before": round(positions[i], 2),
                            "after": round(new_positions[i], 2),
                        }
                        for i in range(len(assets))
                    }
                    new_stability = sim_stability

        stay_in_stability = amount - transfer_to_core
        final_nav = self._get_total_nav(
            core_sum + transfer_to_core, new_stability
        )

        # 分配前后对比
        before_core_ratio = core_sum / old_nav if old_nav > 0 else 0
        before_stability_ratio = stability / old_nav if old_nav > 0 else 0
        after_core_ratio = (core_sum + transfer_to_core) / final_nav if final_nav > 0 else 0
        after_stability_ratio = new_stability / final_nav if final_nav > 0 else 0

        return CashflowResult(
            "SUCCESS", "入金分配预览",
            amount=amount,
            portfolio_id=portfolio_id,
            state=state,
            step1_to_stability=round(amount, 2),
            step2_to_core=round(transfer_to_core, 2),
            stay_in_stability=round(stay_in_stability, 2),
            core_allocation=core_allocation,
            before=dict(
                nav=round(old_nav, 2),
                core=round(core_sum, 2),
                core_ratio=round(before_core_ratio, 4),
                stability=round(stability, 2),
                stability_ratio=round(before_stability_ratio, 4),
            ),
            after=dict(
                nav=round(final_nav, 2),
                core=round(core_sum + transfer_to_core, 2),
                core_ratio=round(after_core_ratio, 4),
                stability=round(new_stability, 2),
                stability_ratio=round(after_stability_ratio, 4),
            ),
            protection_hold=state == "PROTECTION",
            note="PROTECTION状态下全额留Stability，下一周期系统自动分配" if state == "PROTECTION" else "确认后资金立即入账，系统按方案分配",
        )

    def deposit(
        self, amount: float, depositor: str, portfolio_id: str = "us"
    ) -> CashflowResult:
        if amount < MIN_DEPOSIT:
            return CashflowResult("ERROR", f"最小入金额 ${MIN_DEPOSIT:,.0f}")

        if portfolio_id not in PORTFOLIOS:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 不存在")

        try:
            portfolio = self.db.get_portfolio(portfolio_id)
        except ValueError:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 未初始化")

        positions = _parse_positions(portfolio["positions"])
        stability = float(portfolio.get("stability_balance", 0.0))
        core_sum = sum(positions)
        old_nav = self._get_total_nav(core_sum, stability)

        max_deposit = min(old_nav * MAX_DEPOSIT_NAV_RATIO, MAX_DEPOSIT_HARD_CAP)
        if old_nav > 0 and amount > max_deposit:
            return CashflowResult("ERROR", f"单次入金上限 ${max_deposit:,.0f}")

        deposit_id = str(uuid.uuid4())[:8]
        state = portfolio["state"]

        # ── 第一步：100% 进入 Stability ──
        new_stability = stability + amount
        new_nav = self._get_total_nav(core_sum, new_stability)

        allocation_detail = {"Stability": round(amount, 2)}
        transfer_to_core = 0.0
        core_allocation = {}

        # ── 第二步：检测是否需要从 Stability 转资金到 Core ──
        # PROTECTION 状态下不转入 Core，全额留在 Stability 缓冲
        if state != "PROTECTION":
            core_ratio = core_sum / new_nav if new_nav > 0 else 0
            stability_ratio = new_stability / new_nav if new_nav > 0 else 0

            if core_ratio < LAYER_TARGET_CORE and stability_ratio > LAYER_TARGET_STABILITY:
                # 需要转入 Core 的金额 = 使 Core 达到目标占比所需
                target_core = LAYER_TARGET_CORE * new_nav
                transfer_to_core = target_core - core_sum

                # 但不能让 Stability 低于目标占比
                min_stability_keep = LAYER_TARGET_STABILITY * new_nav
                max_transferable = new_stability - min_stability_keep

                transfer_to_core = max(0, min(transfer_to_core, max_transferable))

                if transfer_to_core > 0:
                    new_stability -= transfer_to_core

                    # Core 内部按等权分配转入资金
                    target_weights = np.array(WEIGHTS_IDLE, dtype=float)
                    current_positions = np.array(positions, dtype=float)
                    target_core_total = core_sum + transfer_to_core
                    target_positions = target_weights * target_core_total

                    # 用增量补缺口（只加不卖）
                    deltas = target_positions - current_positions
                    positive_deltas = np.maximum(deltas, 0)
                    delta_sum = float(np.sum(positive_deltas))

                    if delta_sum > 1e-6:
                        core_add = positive_deltas / delta_sum * transfer_to_core
                        positions = (current_positions + core_add).tolist()
                    else:
                        # 所有资产都超配，等权分配
                        equal_add = transfer_to_core / len(positions)
                        positions = [p + equal_add for p in positions]

                    pf_config = PORTFOLIOS[portfolio_id]
                    assets = pf_config["assets"]
                    core_allocation = {
                        assets[i]: round(positions[i] - current_positions[i], 2)
                        for i in range(len(assets))
                    }

        # 重新计算 NAV
        core_sum_new = sum(positions)
        new_nav = self._get_total_nav(core_sum_new, new_stability)

        allocation_detail = {
            "Stability": round(amount - transfer_to_core, 2),
        }
        if core_allocation:
            allocation_detail["Core转入"] = round(transfer_to_core, 2)
            allocation_detail["Core明细"] = core_allocation

        desc_parts = [f"${amount:,.2f} 进入Stability"]
        if transfer_to_core > 0:
            desc_parts.append(f"${transfer_to_core:,.2f} 分配到Core层")
        if state == "PROTECTION":
            desc_parts.append("(PROTECTION状态，全额留Stability)")
        allocation_desc = "，".join(desc_parts)

        with self.db.transaction() as conn:
            self.db.update_portfolio(
                portfolio_id, conn=conn,
                nav=round(new_nav, 2),
                positions=json.dumps([round(p, 2) for p in positions]),
                stability_balance=round(new_stability, 2),
                high_water_mark=max(portfolio["high_water_mark"], new_nav),
            )
            self.db.save_transaction(
                date=datetime.now().strftime("%Y-%m-%d"),
                tx_type="DEPOSIT",
                reason=f"入金 {allocation_desc}",
                portfolio_id=portfolio_id,
                conn=conn,
            )
            self.db.save_deposit_record(
                deposit_id=deposit_id,
                amount=amount,
                depositor=depositor,
                portfolio_id=portfolio_id,
                allocation=json.dumps(allocation_detail),
                conn=conn,
            )
            self.db.save_audit_log(
                "DEPOSIT", depositor,
                f"入金 {allocation_desc} → 组合 {portfolio_id}",
                conn=conn,
            )

        logger.info(f"入金完成: {allocation_desc} → {portfolio_id} (#{deposit_id})")

        return CashflowResult(
            "SUCCESS",
            f"入金 ${amount:,.2f} 成功",
            deposit_id=deposit_id,
            new_nav=round(new_nav, 2),
            stability_balance=round(new_stability, 2),
            core_balance=round(core_sum_new, 2),
            allocation=allocation_detail,
            allocation_desc=allocation_desc,
        )

    def withdrawal_preview(
        self, amount: float, portfolio_id: str = "us"
    ) -> CashflowResult:
        """模拟出金扣减方案，不写入数据库"""
        if amount <= 0:
            return CashflowResult("ERROR", "出金金额必须大于 0")

        try:
            portfolio = self.db.get_portfolio(portfolio_id)
        except ValueError:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 不存在")

        positions = _parse_positions(portfolio["positions"])
        stability = float(portfolio.get("stability_balance", 0.0))
        core_sum = sum(positions)
        nav = self._get_total_nav(core_sum, stability)

        if amount > nav * MAX_WITHDRAWAL_NAV_RATIO:
            limit = nav * MAX_WITHDRAWAL_NAV_RATIO
            return CashflowResult(
                "ERROR",
                f"出金 ${amount:,.2f} 超过 NAV 的 {MAX_WITHDRAWAL_NAV_RATIO:.0%}（${limit:,.2f}）",
            )

        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]
        asset_names = pf_config["asset_names"]

        remaining = amount
        deductions = []
        est_friction = 0.0

        # 第一步：Stability
        stability_deduct = min(remaining, stability)
        if stability_deduct > 0:
            remaining -= stability_deduct
            deductions.append(dict(
                layer="Stability", name="流动性池",
                amount=round(stability_deduct, 2),
                friction=0,
            ))

        # 第二步：Core 超配
        if remaining > 0.01:
            sim_positions = list(positions)
            target_weights = np.array(WEIGHTS_IDLE, dtype=float)
            current_positions = np.array(sim_positions, dtype=float)
            pos_sum = float(np.sum(current_positions))
            if pos_sum > 0:
                overweight = current_positions / pos_sum - target_weights
            else:
                overweight = np.zeros(len(sim_positions))

            for idx in np.argsort(-overweight):
                idx = int(idx)
                if remaining < 0.01:
                    break
                sellable = min(remaining, sim_positions[idx])
                if sellable > 0:
                    friction = sellable * FEE_RATE
                    sim_positions[idx] -= sellable
                    remaining -= sellable
                    est_friction += friction
                    deductions.append(dict(
                        layer="Core",
                        name=asset_names.get(assets[idx], assets[idx]),
                        asset=assets[idx],
                        amount=round(sellable, 2),
                        friction=round(friction, 2),
                    ))

        if remaining > 0.01:
            return CashflowResult("ERROR", f"资金不足，还差 ${remaining:,.2f}")

        new_stability = stability - stability_deduct
        new_core = core_sum - (amount - stability_deduct)
        new_nav = self._get_total_nav(new_core, new_stability) - est_friction

        return CashflowResult(
            "SUCCESS", "出金扣减预览",
            amount=amount,
            deductions=deductions,
            estimated_friction=round(est_friction, 2),
            before=dict(
                nav=round(nav, 2),
                core=round(core_sum, 2),
                stability=round(stability, 2),
            ),
            after=dict(
                nav=round(new_nav, 2),
                core=round(new_core, 2),
                stability=round(new_stability, 2),
            ),
            stability_warning=new_stability / new_nav < LAYER_MIN_STABILITY if new_nav > 0 else False,
            state=portfolio["state"],
        )

    def execute_withdrawal(
        self, withdrawal_id: str, executor: str
    ) -> CashflowResult:
        withdrawal = self.db.get_withdrawal_request(withdrawal_id)
        if not withdrawal:
            return CashflowResult("ERROR", "出金请求不存在")

        if withdrawal["status"] != "APPROVED":
            return CashflowResult("ERROR", f"出金请求状态为 {withdrawal['status']}，需要 APPROVED")

        now_str = datetime.now().strftime("%Y-%m-%d")
        if now_str > withdrawal["expires_at"]:
            self.db.update_withdrawal_status(withdrawal_id, "EXPIRED")
            return CashflowResult("ERROR", "出金请求已过期")

        portfolio_id = withdrawal["portfolio_id"]
        amount = withdrawal["amount"]

        try:
            portfolio = self.db.get_portfolio(portfolio_id)
        except ValueError:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 不存在")

        positions = _parse_positions(portfolio["positions"])
        stability = float(portfolio.get("stability_balance", 0.0))
        core_sum = sum(positions)
        nav = self._get_total_nav(core_sum, stability)

        if amount > nav * MAX_WITHDRAWAL_NAV_RATIO:
            limit = nav * MAX_WITHDRAWAL_NAV_RATIO
            return CashflowResult(
                "ERROR",
                f"出金 ${amount:,.2f} 超过 NAV 的 {MAX_WITHDRAWAL_NAV_RATIO:.0%}（${limit:,.2f}），不允许",
            )

        state = portfolio["state"]
        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]

        remaining = amount
        deductions = {}
        friction_cost = 0.0

        # ── 第一步：从 Stability 层扣 ──
        stability_deduct = min(remaining, stability)
        if stability_deduct > 0:
            stability -= stability_deduct
            remaining -= stability_deduct
            deductions["Stability"] = round(stability_deduct, 2)

        # ── 第二步：Stability 不足，从 Core 层超配资产卖 ──
        if remaining > 0.01:
            target_weights = np.array(WEIGHTS_IDLE, dtype=float)
            current_positions = np.array(positions, dtype=float)
            pos_sum = float(np.sum(current_positions))

            if pos_sum > 0:
                current_weights = current_positions / pos_sum
                overweight = current_weights - target_weights
            else:
                overweight = np.zeros(len(positions))

            # 从超配最多的资产开始卖
            for idx in np.argsort(-overweight):
                idx = int(idx)
                if remaining < 0.01:
                    break
                sellable = min(remaining, positions[idx])
                if sellable > 0:
                    sell_friction = sellable * FEE_RATE
                    positions[idx] -= sellable
                    remaining -= sellable
                    friction_cost += sell_friction
                    asset_name = assets[idx]
                    deductions[asset_name] = round(
                        deductions.get(asset_name, 0) + sellable, 2
                    )

        if remaining > 0.01:
            return CashflowResult("ERROR", f"资金不足，还差 ${remaining:,.2f}")

        core_sum_new = sum(positions)
        new_nav = self._get_total_nav(core_sum_new, stability) - friction_cost

        # 风控检查：Stability 占比
        stability_ratio = stability / new_nav if new_nav > 0 else 0
        risk_warning = None
        if stability_ratio < LAYER_MIN_STABILITY:
            risk_warning = (
                f"⚠️ 出金后 Stability 层 ${stability:,.2f} "
                f"占比 {stability_ratio:.1%}，低于 {LAYER_MIN_STABILITY:.0%} 安全线"
            )

        protection_warning = None
        if state == "PROTECTION":
            protection_warning = "当前处于风控保护状态，出金可能锁定亏损"

        with self.db.transaction() as conn:
            self.db.update_portfolio(
                portfolio_id, conn=conn,
                nav=round(new_nav, 2),
                positions=json.dumps([round(p, 2) for p in positions]),
                stability_balance=round(stability, 2),
            )
            self.db.update_withdrawal_status(withdrawal_id, "EXECUTED", conn=conn)
            conn.execute(
                "UPDATE withdrawal_requests SET executed_at = ? WHERE id = ?",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), withdrawal_id),
            )
            self.db.save_transaction(
                date=now_str,
                tx_type="WITHDRAWAL",
                turnover=amount,
                friction_cost=round(friction_cost, 2),
                reason=f"出金执行 #{withdrawal_id}，${amount:,.2f}",
                portfolio_id=portfolio_id,
                conn=conn,
            )
            if risk_warning:
                self.db.save_risk_event(
                    date=now_str,
                    event_type="LOW_STABILITY",
                    severity="WARNING",
                    action_taken=risk_warning,
                    portfolio_id=portfolio_id,
                    conn=conn,
                )
            self.db.save_audit_log(
                "WITHDRAWAL_EXECUTED", executor,
                f"出金执行 #{withdrawal_id}，${amount:,.2f}，摩擦 ${friction_cost:.2f}",
                conn=conn,
            )

        logger.info(f"出金执行: #{withdrawal_id} ${amount:,.2f} ← {portfolio_id}")

        result_extra = dict(
            withdrawal_id=withdrawal_id,
            amount=amount,
            new_nav=round(new_nav, 2),
            stability_balance=round(stability, 2),
            core_balance=round(core_sum_new, 2),
            deductions=deductions,
            friction_cost=round(friction_cost, 2),
        )
        if risk_warning:
            result_extra["risk_warning"] = risk_warning
        if protection_warning:
            result_extra["protection_warning"] = protection_warning

        return CashflowResult("SUCCESS", f"出金 ${amount:,.2f} 执行完成", **result_extra)

    def get_layer_status(self, portfolio_id: str = "us") -> CashflowResult:
        """查询三层资金分布状态"""
        try:
            portfolio = self.db.get_portfolio(portfolio_id)
        except ValueError:
            return CashflowResult("ERROR", f"组合 {portfolio_id} 不存在")

        alpha_account = self.db.get_alpha_account(portfolio_id)
        positions = _parse_positions(portfolio["positions"])
        stability = float(portfolio.get("stability_balance", 0.0))
        alpha_balance = float(alpha_account.get("cash_balance", 0.0))
        core_sum = sum(positions)
        nav = self._get_total_nav(core_sum, stability)
        family_nav = nav + alpha_balance

        pf_config = PORTFOLIOS[portfolio_id]
        assets = pf_config["assets"]
        asset_names = pf_config["asset_names"]

        core_detail = {
            assets[i]: {
                "balance": round(positions[i], 2),
                "name": asset_names[assets[i]],
                "weight_in_core": round(positions[i] / core_sum, 4) if core_sum > 0 else 0,
            }
            for i in range(len(assets))
        }

        return CashflowResult(
            "SUCCESS", "三层状态",
            nav=round(nav, 2),
            family_nav=round(family_nav, 2),
            core=dict(
                balance=round(core_sum, 2),
                ratio=round(core_sum / family_nav, 4) if family_nav > 0 else 0,
                target=LAYER_TARGET_CORE,
                assets=core_detail,
            ),
            stability=dict(
                balance=round(stability, 2),
                ratio=round(stability / family_nav, 4) if family_nav > 0 else 0,
                target=LAYER_TARGET_STABILITY,
                min_safe=LAYER_MIN_STABILITY,
            ),
            alpha=dict(
                balance=round(alpha_balance, 2),
                ratio=round(alpha_balance / family_nav, 4) if family_nav > 0 else 0,
                target=LAYER_TARGET_ALPHA,
                max_target=LAYER_TARGET_ALPHA,
                total_inflows=round(float(alpha_account.get("total_inflows", 0.0)), 2),
                total_outflows=round(float(alpha_account.get("total_outflows", 0.0)), 2),
                last_manual_adjustment=alpha_account.get("last_manual_adjustment"),
            ),
            state=portfolio["state"],
        )
