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
from engine.notifier import notify_withdrawal, notify_approval

logger = logging.getLogger("xirang.governance")

# ── 治理参数 ──────────────────────────────────────────

MULTISIG_THRESHOLD = 500_000.0   # 50 万以上需多签
REQUIRED_APPROVALS = 2           # 需要 2 人批准
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

    def request_withdrawal(
        self, amount: float, reason: str, requester: str, portfolio_id: str = "us"
    ) -> GovernanceResult:
        """
        发起出金请求。

        < 50万：自动批准
        >= 50万：进入多签流程 + 7天冷却期 + 通知家族成员
        """
        withdrawal_id = str(uuid.uuid4())[:8]
        needs_multisig = amount >= MULTISIG_THRESHOLD
        cooling = COOLING_DAYS if needs_multisig else 0
        expires_at = (datetime.now() + timedelta(days=cooling + REQUEST_EXPIRY_DAYS)).strftime("%Y-%m-%d")

        self.db.create_withdrawal_request(
            withdrawal_id=withdrawal_id,
            amount=amount,
            reason=reason,
            requester=requester,
            expires_at=expires_at,
            portfolio_id=portfolio_id,
            required_approvals=REQUIRED_APPROVALS if needs_multisig else 0,
            cooling_days=cooling,
        )

        self.db.save_audit_log(
            "WITHDRAW_REQUEST", requester,
            f"发起出金 ${amount:,.2f}，组合: {portfolio_id}，原因: {reason}",
        )

        if not needs_multisig:
            self.db.update_withdrawal_status(withdrawal_id, "APPROVED")
            logger.info(f"小额出金 ${amount:,.2f} 自动批准 (#{withdrawal_id})")
            return GovernanceResult(
                status="APPROVED",
                withdrawal_id=withdrawal_id,
                message=f"小额出金（< ${MULTISIG_THRESHOLD:,.0f}），已自动批准",
            )

        # 大额：通知家族成员
        logger.info(f"大额出金 ${amount:,.2f} 需要多签 (#{withdrawal_id})")
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
            message=f"大额出金需要 {REQUIRED_APPROVALS} 位家族成员批准，冷却期 {cooling} 天",
            required_approvals=REQUIRED_APPROVALS,
            cooling_days=cooling,
            expires_at=expires_at,
        )

    def approve_withdrawal(
        self, withdrawal_id: str, approver: str, decision: str = "APPROVED", comment: str = ""
    ) -> GovernanceResult:
        """
        审批出金请求。

        校验：请求存在、状态为PENDING、非本人、未过期、冷却期已过。
        """
        withdrawal = self.db.get_withdrawal_request(withdrawal_id)
        if not withdrawal:
            return GovernanceResult("ERROR", withdrawal_id, "出金请求不存在")

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
        self.db.add_withdrawal_approval(withdrawal_id, approver, decision, comment)
        self.db.save_audit_log(
            f"WITHDRAW_{decision}", approver,
            f"审批出金 #{withdrawal_id}: {decision}，备注: {comment}",
        )

        if decision == "REJECTED":
            self.db.update_withdrawal_status(withdrawal_id, "REJECTED")
            notify_approval(withdrawal_id, withdrawal["amount"], approver, "REJECTED")
            return GovernanceResult("REJECTED", withdrawal_id, "出金请求已被拒绝")

        # 检查多签条件
        approvals = self.db.get_withdrawal_approvals(withdrawal_id)
        approved_count = sum(1 for a in approvals if a["decision"] == "APPROVED")

        if approved_count >= withdrawal["required_approvals"]:
            self.db.update_withdrawal_status(withdrawal_id, "APPROVED")
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
