"""
息壤（Xi-Rang）出金治理引擎

职责：
- 判断出金是否需要多签
- 冷却期管理（大额出金 7 天等待）
- 过期请求自动清理
- 出金通知推送给家族成员
- 审计日志记录

与 API 路由层解耦：路由层负责 HTTP，治理引擎负责业务逻辑。
"""

import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional

from db.database import Database
from engine.insurance import build_authority_decision, build_missing_authority_decision, coerce_insurance_state
from engine.notifier import notify_withdrawal, notify_approval

logger = logging.getLogger("xirang.governance")

# ── 治理参数 ──────────────────────────────────────────

MULTISIG_THRESHOLD = 500_000.0   # 50 万以上需多签
REQUIRED_APPROVALS = 2           # 需要 2 人批准
SMALL_WITHDRAWAL_APPROVALS = 1   # 小额出金也必须有人审批
COOLING_DAYS = 7                 # 冷却期天数
REQUEST_EXPIRY_DAYS = 14         # 请求过期天数


class GovernanceResult:
    """治理操作结果"""
    def __init__(self, status: str, withdrawal_id: str = "", message: str = "", **extra):
        self.status = status
        self.withdrawal_id = withdrawal_id
        self.message = message
        self.extra = extra

    def to_dict(self) -> dict:
        d = {"status": self.status, "id": self.withdrawal_id, "message": self.message}
        d.update(self.extra)
        return d


class WithdrawalGovernance:
    """出金治理引擎"""

    def __init__(self, db: Database):
        self.db = db

    def _latest_insurance_authority(self, portfolio_id: str):
        latest = self.db.get_latest_insurance_decision(portfolio_id)
        if latest:
            decision = build_authority_decision(
                coerce_insurance_state(latest.get("new_state")),
                reasons=latest.get("reasons", []),
            )
            return decision, latest.get("id")
        return build_missing_authority_decision("missing persisted InsuranceDecision"), None

    def _latest_insurance_decision(self, portfolio_id: str):
        return self._latest_insurance_authority(portfolio_id)[0]

    def request_withdrawal(
        self,
        amount: float,
        reason: str,
        requester: str,
        portfolio_id: str = "us",
        account_id: str | None = None,
        member_id: str | None = None,
        requested_by_user_id: int | None = None,
        source_pool_id: str | None = None,
        currency: str = "USD",
    ) -> GovernanceResult:
        """
        发起出金请求。

        所有出金都只创建请求，不自动批准。
        < 50万：单人审批，无冷却期
        >= 50万：多人审批 + 7天冷却期
        """
        authority, insurance_decision_id = self._latest_insurance_authority(portfolio_id)
        if not authority.allow_withdrawal_request:
            return GovernanceResult(
                "ERROR",
                message="Insurance Layer blocked withdrawal request",
                insurance_state=authority.state.value,
                reasons=authority.reasons,
            )

        source_pool_id = source_pool_id or portfolio_id
        shares_requested = 0.0
        share_price = None
        if account_id:
            account = self.db.get_capital_account(account_id)
            if account is None:
                return GovernanceResult("ERROR", message="资产账户不存在")
            member_id = member_id or account.get("member_id")
            pool = self.db.ensure_investment_pool(source_pool_id, portfolio_id=portfolio_id)
            share_price = float(pool.get("share_price") or 0.0)
            if share_price <= 0:
                return GovernanceResult("ERROR", message="投资池份额净值无效，不能发起出金")
            position = self.db.get_account_pool_position(account_id, source_pool_id)
            reserved = self.db.get_reserved_withdrawal_shares(account_id, source_pool_id)
            available_shares = round(float(position.get("shares") or 0.0) - reserved, 8)
            shares_requested = round(float(amount) / share_price, 8)
            if shares_requested > available_shares + 1e-8:
                return GovernanceResult(
                    "ERROR",
                    message="账户可赎回份额不足",
                    account_id=account_id,
                    source_pool_id=source_pool_id,
                    requested_shares=shares_requested,
                    available_shares=max(0.0, available_shares),
                    share_price=share_price,
                )

        withdrawal_id = str(uuid.uuid4())[:8]
        needs_multisig = amount >= MULTISIG_THRESHOLD
        cooling = COOLING_DAYS if needs_multisig else 0
        required_approvals = REQUIRED_APPROVALS if needs_multisig else SMALL_WITHDRAWAL_APPROVALS
        expires_at = (datetime.now() + timedelta(days=cooling + REQUEST_EXPIRY_DAYS)).strftime("%Y-%m-%d")

        with self.db.transaction() as conn:
            self.db.create_withdrawal_request(
                withdrawal_id=withdrawal_id,
                amount=amount,
                reason=reason,
                requester=requester,
                expires_at=expires_at,
                portfolio_id=portfolio_id,
                required_approvals=required_approvals,
                cooling_days=cooling,
                account_id=account_id,
                member_id=member_id,
                requested_by_user_id=requested_by_user_id,
                source_pool_id=source_pool_id,
                currency=currency,
                shares_requested=shares_requested,
                share_price=share_price,
                conn=conn,
            )
            request_entry = None
            if account_id:
                request_entry = self.db.record_ledger_entry(
                    account_id=account_id,
                    portfolio_id=portfolio_id,
                    pool_id=source_pool_id,
                    entry_type="WITHDRAWAL_REQUESTED",
                    amount=amount,
                    currency=currency,
                    shares_delta=-shares_requested,
                    share_price=share_price,
                    actor=requester,
                    source_ref_type="withdrawal_request",
                    source_ref_id=withdrawal_id,
                    memo="出金申请已提交，份额仅作审批占用，不改变账户持仓",
                    conn=conn,
                )
            self.db.save_audit_log(
                "WITHDRAW_REQUEST", requester,
                (
                    f"发起出金 ${amount:,.2f}，账户: {account_id or '-'}，组合: {portfolio_id}，"
                    f"份额: {shares_requested:,.8f}，原因: {reason} "
                    f"| InsuranceDecision={insurance_decision_id}"
                ),
                conn=conn,
            )

        if needs_multisig:
            logger.info(f"大额出金 ${amount:,.2f} 需要多签 (#{withdrawal_id})")
        else:
            logger.info(f"小额出金 ${amount:,.2f} 已创建待审批请求 (#{withdrawal_id})")

        notify_withdrawal(
            withdrawal_id=withdrawal_id,
            amount=amount,
            reason=reason,
            requester=requester,
            portfolio_id=portfolio_id,
            cooling_days=cooling,
            expires_at=expires_at,
        )

        return GovernanceResult(
            status="PENDING",
            withdrawal_id=withdrawal_id,
            message=f"出金请求已创建，需要 {required_approvals} 位家族成员批准" + (f"，冷却期 {cooling} 天" if cooling else ""),
            required_approvals=required_approvals,
            cooling_days=cooling,
            expires_at=expires_at,
            account_id=account_id,
            source_pool_id=source_pool_id,
            shares_requested=shares_requested,
            share_price=share_price,
            ledger_entry_id=request_entry["id"] if account_id and request_entry else None,
        )

    def approve_withdrawal(
        self,
        withdrawal_id: str,
        approver: str,
        decision: str = "APPROVED",
        comment: str = "",
        approver_user_id: int | None = None,
        approver_role: str | None = None,
    ) -> GovernanceResult:
        """
        审批出金请求。

        校验：请求存在、状态为PENDING、非本人、未过期、冷却期已过。
        """
        withdrawal = self.db.get_withdrawal_request(withdrawal_id)
        if not withdrawal:
            return GovernanceResult("ERROR", withdrawal_id, "出金请求不存在")

        authority, insurance_decision_id = self._latest_insurance_authority(withdrawal["portfolio_id"])
        if not authority.allow_withdrawal_approval:
            return GovernanceResult(
                "ERROR",
                withdrawal_id,
                "Insurance Layer blocked withdrawal approval",
                insurance_state=authority.state.value,
                reasons=authority.reasons,
            )

        if withdrawal["status"] != "PENDING":
            return GovernanceResult("ERROR", withdrawal_id, f"请求已处于 {withdrawal['status']} 状态")

        if withdrawal["requester"] == approver:
            return GovernanceResult("ERROR", withdrawal_id, "不能审批自己发起的请求")

        # 过期检查
        now_str = datetime.now().strftime("%Y-%m-%d")
        if now_str > withdrawal["expires_at"]:
            self.db.update_withdrawal_status(withdrawal_id, "EXPIRED")
            return GovernanceResult("EXPIRED", withdrawal_id, "出金请求已过期")

        # 冷却期检查
        created = withdrawal["created_at"]
        if isinstance(created, str) and withdrawal["cooling_days"] > 0:
            try:
                created_dt = datetime.strptime(created[:10], "%Y-%m-%d")
                cooldown_end = created_dt + timedelta(days=withdrawal["cooling_days"])
                if datetime.now() < cooldown_end:
                    remaining = (cooldown_end - datetime.now()).days + 1
                    return GovernanceResult(
                        "COOLING", withdrawal_id,
                        f"冷却期未结束，还需等待 {remaining} 天",
                        cooling_remaining=remaining,
                    )
            except (ValueError, TypeError):
                pass  # 时间解析失败则跳过冷却期检查

        # 记录审批
        self.db.add_withdrawal_approval(
            withdrawal_id,
            approver,
            decision,
            comment,
            approver_user_id=approver_user_id,
            approver_role=approver_role,
        )
        self.db.save_audit_log(
            f"WITHDRAW_{decision}", approver,
            (
                f"审批出金 #{withdrawal_id}: {decision}，备注: {comment} "
                f"| InsuranceDecision={insurance_decision_id}"
            ),
        )

        if decision == "REJECTED":
            self.db.update_withdrawal_status(withdrawal_id, "REJECTED")
            if withdrawal.get("account_id"):
                self.db.record_ledger_entry(
                    account_id=withdrawal["account_id"],
                    portfolio_id=withdrawal["portfolio_id"],
                    pool_id=withdrawal.get("source_pool_id") or withdrawal["portfolio_id"],
                    entry_type="WITHDRAWAL_REJECTED",
                    amount=float(withdrawal["amount"]),
                    currency=withdrawal.get("currency") or "USD",
                    shares_delta=0,
                    share_price=withdrawal.get("share_price"),
                    actor=approver,
                    source_ref_type="withdrawal_request",
                    source_ref_id=withdrawal_id,
                    memo=comment or "出金申请被拒绝",
                )
            notify_approval(withdrawal_id, withdrawal["amount"], approver, "REJECTED")
            return GovernanceResult("REJECTED", withdrawal_id, "出金请求已被拒绝")

        # 检查多签条件
        approvals = self.db.get_withdrawal_approvals(withdrawal_id)
        approved_count = sum(1 for a in approvals if a["decision"] == "APPROVED")

        if approved_count >= withdrawal["required_approvals"]:
            self.db.update_withdrawal_status(withdrawal_id, "APPROVED")
            if approver_user_id is not None:
                self.db.update_withdrawal_request(withdrawal_id, approved_by_user_id=approver_user_id)
            if withdrawal.get("account_id"):
                self.db.record_ledger_entry(
                    account_id=withdrawal["account_id"],
                    portfolio_id=withdrawal["portfolio_id"],
                    pool_id=withdrawal.get("source_pool_id") or withdrawal["portfolio_id"],
                    entry_type="WITHDRAWAL_APPROVED",
                    amount=float(withdrawal["amount"]),
                    currency=withdrawal.get("currency") or "USD",
                    shares_delta=-float(withdrawal.get("shares_requested") or 0.0),
                    share_price=withdrawal.get("share_price"),
                    actor=approver,
                    source_ref_type="withdrawal_request",
                    source_ref_id=withdrawal_id,
                    memo=f"出金审批通过 ({approved_count}/{withdrawal['required_approvals']})",
                )
            notify_approval(withdrawal_id, withdrawal["amount"], approver, "APPROVED")
            logger.info(f"出金 #{withdrawal_id} 多签通过 ({approved_count}/{withdrawal['required_approvals']})")
            return GovernanceResult(
                "APPROVED", withdrawal_id,
                "多签条件满足，出金已批准",
                approvals=approved_count,
            )

        return GovernanceResult(
            "PENDING", withdrawal_id,
            f"已批准 {approved_count}/{withdrawal['required_approvals']}，等待更多审批",
            approvals=approved_count,
            required=withdrawal["required_approvals"],
        )

    def cleanup_expired(self) -> int:
        """清理过期的出金请求，返回清理数量"""
        now_str = datetime.now().strftime("%Y-%m-%d")
        pending = self.db.list_withdrawal_requests(status="PENDING", limit=100)
        expired_count = 0
        for req in pending:
            if now_str > req["expires_at"]:
                self.db.update_withdrawal_status(req["id"], "EXPIRED")
                self.db.save_audit_log("WITHDRAW_EXPIRED", "system", f"出金 #{req['id']} 已过期自动关闭")
                expired_count += 1
        if expired_count:
            logger.info(f"清理 {expired_count} 个过期出金请求")
        return expired_count
