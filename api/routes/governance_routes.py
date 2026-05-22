"""
治理层路由 — 入金、出金审批、出金执行、审计日志

业务逻辑委托给 engine/governance.py 和 engine/cashflow.py，此处只做 HTTP 层。
"""

from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime

from db.database import Database
from engine.governance import WithdrawalGovernance
from engine.cashflow import CashflowEngine
from api.deps import (
    ensure_account_permission,
    ensure_portfolio_access,
    get_db,
    get_current_user,
    is_admin_user,
    require_role,
)
from api.models import WithdrawalRequest, ApprovalRequest, DepositRequest, DepositConfirmRequest

router = APIRouter(prefix="/api/governance", tags=["治理"])


def _request_fields(req) -> set[str]:
    fields = getattr(req, "model_fields_set", None)
    if fields is None:
        fields = getattr(req, "__fields_set__", set())
    return set(fields)


def _resolve_deposit_scope(
    req: DepositRequest,
    db: Database,
    user: dict,
    permission: str = "deposit_request",
) -> tuple[dict | None, str]:
    """
    Resolve the capital account boundary for a deposit.

    New platformized deposits should carry account_id. For member convenience,
    a single accessible account is inferred. Admin legacy calls may omit
    account_id and remain portfolio-scoped.
    """
    if req.account_id:
        account = ensure_account_permission(db, user, req.account_id, permission=permission)
        portfolio_id = account.get("default_portfolio_id") or req.portfolio_id
        if "portfolio_id" in _request_fields(req) and req.portfolio_id != portfolio_id:
            raise HTTPException(status_code=400, detail="入金组合必须与资产账户默认组合一致")
        return account, portfolio_id

    if is_admin_user(user):
        return None, req.portfolio_id

    accounts = db.list_user_accounts(
        user["id"],
        member_id=user.get("member_id"),
        permission=permission,
    )
    if not accounts:
        raise HTTPException(status_code=403, detail="没有可用于入金的资产账户")
    if len(accounts) > 1:
        raise HTTPException(status_code=400, detail="请指定 account_id")
    return accounts[0], accounts[0].get("default_portfolio_id") or req.portfolio_id


def _resolve_withdrawal_scope(
    req: WithdrawalRequest,
    db: Database,
    user: dict,
    permission: str = "withdraw_request",
) -> tuple[dict | None, str, str]:
    """Resolve account and source pool for platformized withdrawals."""
    if req.account_id:
        account = ensure_account_permission(db, user, req.account_id, permission=permission)
        portfolio_id = account.get("default_portfolio_id") or req.portfolio_id
        if "portfolio_id" in _request_fields(req) and req.portfolio_id != portfolio_id:
            raise HTTPException(status_code=400, detail="出金组合必须与资产账户默认组合一致")
        return account, portfolio_id, req.source_pool_id or portfolio_id

    if is_admin_user(user):
        return None, req.portfolio_id, req.source_pool_id or req.portfolio_id

    accounts = db.list_user_accounts(
        user["id"],
        member_id=user.get("member_id"),
        permission=permission,
    )
    if not accounts:
        raise HTTPException(status_code=403, detail="没有可用于出金的资产账户")
    if len(accounts) > 1:
        raise HTTPException(status_code=400, detail="请指定 account_id")
    account = accounts[0]
    portfolio_id = account.get("default_portfolio_id") or req.portfolio_id
    return account, portfolio_id, req.source_pool_id or portfolio_id


def _account_withdrawal_preview(db: Database, account_id: str, pool_id: str, amount: float) -> dict:
    pool = db.ensure_investment_pool(pool_id, portfolio_id=pool_id)
    share_price = float(pool.get("share_price") or 0.0)
    if share_price <= 0:
        raise HTTPException(status_code=400, detail="投资池份额净值无效")
    position = db.get_account_pool_position(account_id, pool_id)
    reserved_shares = db.get_reserved_withdrawal_shares(account_id, pool_id)
    available_shares = round(float(position.get("shares") or 0.0) - reserved_shares, 8)
    requested_shares = round(float(amount) / share_price, 8)
    if requested_shares > available_shares + 1e-8:
        raise HTTPException(status_code=400, detail="账户可赎回份额不足")
    return {
        "account_id": account_id,
        "source_pool_id": pool_id,
        "share_price": share_price,
        "requested_shares": requested_shares,
        "held_shares": round(float(position.get("shares") or 0.0), 8),
        "reserved_shares": reserved_shares,
        "available_shares": max(0.0, available_shares),
    }


@router.post("/deposit/preview")
async def deposit_preview(
    req: DepositRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """入金预览：展示分配方案，供用户确认后再执行"""
    _, portfolio_id = _resolve_deposit_scope(req, db, user, permission="deposit_request")
    ensure_portfolio_access(db, user, portfolio_id, permission="deposit_request")
    engine = CashflowEngine(db)
    result = engine.deposit_preview(
        amount=req.amount,
        portfolio_id=portfolio_id,
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.post("/deposit/requests")
async def create_deposit_request(
    req: DepositRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """提交入金申请，不直接改组合资金。"""
    account, portfolio_id = _resolve_deposit_scope(req, db, user, permission="deposit_request")
    if account is None:
        raise HTTPException(status_code=400, detail="创建入金申请必须指定资产账户")
    result = CashflowEngine(db).request_deposit(
        amount=req.amount,
        requester=user["username"],
        account_id=account["id"],
        portfolio_id=portfolio_id,
        currency=account.get("base_currency") or "USD",
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.get("/deposit/requests")
async def list_deposit_requests(
    portfolio_id: str = None,
    status: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看入金申请。管理员看全局，成员只看自己可见账户。"""
    if is_admin_user(user):
        return db.list_deposit_requests(portfolio_id=portfolio_id, status=status)

    accounts = db.list_user_accounts(user["id"], member_id=user.get("member_id"), permission="view")
    account_ids = [a["id"] for a in accounts]
    return db.list_deposit_requests(
        portfolio_id=portfolio_id,
        status=status,
        account_ids=account_ids,
    )


@router.post("/deposit/requests/{deposit_request_id}/confirm")
async def confirm_deposit_request(
    deposit_request_id: str,
    req: DepositConfirmRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "investment_manager")),
):
    """管理员确认真实到账，写入组合、旧记录和总账。"""
    result = CashflowEngine(db).confirm_deposit_request(
        deposit_request_id=deposit_request_id,
        confirmer=user["username"],
        note=req.note,
        external_reference=req.external_reference,
    )
    if result.status == "ERROR":
        code = 404 if "不存在" in result.message else 400
        raise HTTPException(status_code=code, detail=result.message)
    return result.to_dict()


@router.post("/deposit")
async def deposit(
    req: DepositRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """兼容入口：创建并立即确认入金，内部仍写入申请和总账。"""
    account, portfolio_id = _resolve_deposit_scope(req, db, user, permission="deposit_request")
    ensure_portfolio_access(db, user, portfolio_id, permission="deposit_request")
    engine = CashflowEngine(db)
    result = engine.deposit(
        amount=req.amount,
        depositor=user["username"],
        portfolio_id=portfolio_id,
        account_id=account["id"] if account else None,
        confirm_actor=user["username"],
        currency=(account or {}).get("base_currency") or "USD",
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.get("/deposits")
async def list_deposits(
    portfolio_id: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看入金记录"""
    if is_admin_user(user):
        return db.list_deposit_records(portfolio_id=portfolio_id)

    if portfolio_id:
        ensure_portfolio_access(db, user, portfolio_id, permission="view")
    accounts = db.list_user_accounts(user["id"], member_id=user.get("member_id"), permission="view")
    account_ids = [a["id"] for a in accounts]
    return db.list_deposit_records(portfolio_id=portfolio_id, account_ids=account_ids)


@router.get("/ledger")
async def list_ledger_entries(
    account_id: str = None,
    portfolio_id: str = None,
    entry_type: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看平台总账流水。"""
    if account_id:
        ensure_account_permission(db, user, account_id, permission="view")
        return db.list_ledger_entries(
            account_id=account_id,
            portfolio_id=portfolio_id,
            entry_type=entry_type,
        )

    if is_admin_user(user):
        return db.list_ledger_entries(portfolio_id=portfolio_id, entry_type=entry_type)

    accounts = db.list_user_accounts(user["id"], member_id=user.get("member_id"), permission="view")
    entries = []
    for account in accounts:
        entries.extend(
            db.list_ledger_entries(
                account_id=account["id"],
                portfolio_id=portfolio_id,
                entry_type=entry_type,
            )
        )
    return sorted(entries, key=lambda item: (item.get("created_at") or "", item.get("id") or 0), reverse=True)[:50]


@router.post("/withdraw/preview")
async def withdrawal_preview(
    req: WithdrawalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """出金预览：展示扣减方案"""
    account, portfolio_id, source_pool_id = _resolve_withdrawal_scope(req, db, user, permission="withdraw_request")
    ensure_portfolio_access(db, user, portfolio_id, permission="withdraw_request")
    engine = CashflowEngine(db)
    result = engine.withdrawal_preview(
        amount=req.amount,
        portfolio_id=portfolio_id,
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    body = result.to_dict()
    if account:
        body["redemption"] = _account_withdrawal_preview(db, account["id"], source_pool_id, req.amount)
    return body


@router.post("/withdraw")
async def request_withdrawal(
    req: WithdrawalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """发起出金请求"""
    account, portfolio_id, source_pool_id = _resolve_withdrawal_scope(req, db, user, permission="withdraw_request")
    ensure_portfolio_access(db, user, portfolio_id, permission="withdraw_request")
    if account:
        _account_withdrawal_preview(db, account["id"], source_pool_id, req.amount)
    gov = WithdrawalGovernance(db)
    result = gov.request_withdrawal(
        amount=req.amount,
        reason=req.reason,
        requester=user["username"],
        portfolio_id=portfolio_id,
        account_id=account["id"] if account else None,
        member_id=(account or {}).get("member_id") or user.get("member_id"),
        requested_by_user_id=user.get("id"),
        source_pool_id=source_pool_id,
        currency=(account or {}).get("base_currency") or "USD",
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.post("/withdraw/{withdrawal_id}/approve")
async def approve_withdrawal(
    withdrawal_id: str,
    req: ApprovalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "family_principal", "investment_manager", "operations", "member")),
):
    """审批出金请求"""
    withdrawal = db.get_withdrawal_request(withdrawal_id)
    if not withdrawal:
        raise HTTPException(status_code=404, detail="出金请求不存在")
    if withdrawal.get("account_id") and not is_admin_user(user):
        ensure_account_permission(db, user, withdrawal["account_id"], permission="approve_withdrawal")
    elif not withdrawal.get("account_id"):
        ensure_portfolio_access(db, user, withdrawal["portfolio_id"], permission="approve_withdrawal")
    gov = WithdrawalGovernance(db)
    result = gov.approve_withdrawal(
        withdrawal_id=withdrawal_id,
        approver=user["username"],
        decision=req.decision,
        comment=req.comment,
        approver_user_id=user.get("id"),
        approver_role=user.get("role"),
    )
    if result.status == "ERROR":
        code = 404 if "不存在" in result.message else 400
        raise HTTPException(status_code=code, detail=result.message)
    if result.status == "EXPIRED":
        raise HTTPException(status_code=400, detail=result.message)
    if result.status == "COOLING":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.get("/withdrawals")
async def list_withdrawals(
    status: str = None,
    account_id: str = None,
    portfolio_id: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看出金请求列表"""
    if account_id:
        ensure_account_permission(db, user, account_id, permission="view")
        requests = db.list_withdrawal_requests(status=status, account_id=account_id, portfolio_id=portfolio_id)
    elif is_admin_user(user):
        requests = db.list_withdrawal_requests(status=status, portfolio_id=portfolio_id)
    else:
        accounts = db.list_user_accounts(user["id"], member_id=user.get("member_id"), permission="view")
        account_ids = [account["id"] for account in accounts]
        requests = db.list_withdrawal_requests(status=status, portfolio_id=portfolio_id, account_ids=account_ids)
    for r in requests:
        r["approvals"] = db.get_withdrawal_approvals(r["id"])
    return requests


@router.get("/withdrawals/{withdrawal_id}")
async def get_withdrawal(
    withdrawal_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看单个出金请求详情"""
    withdrawal = db.get_withdrawal_request(withdrawal_id)
    if not withdrawal:
        raise HTTPException(status_code=404, detail="出金请求不存在")
    if withdrawal.get("account_id"):
        ensure_account_permission(db, user, withdrawal["account_id"], permission="view")
    else:
        ensure_portfolio_access(db, user, withdrawal["portfolio_id"], permission="withdraw_request")
    withdrawal["approvals"] = db.get_withdrawal_approvals(withdrawal_id)
    return withdrawal


@router.get("/audit-log")
async def get_audit_log(
    action: str = None,
    actor: str = None,
    since: str = None,
    until: str = None,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """查看审计日志（仅管理员）"""
    return db.get_audit_log(limit=50, action=action, actor=actor, since=since, until=until)


@router.get("/audit-log/export")
async def export_audit_log(
    action: str = None,
    actor: str = None,
    since: str = None,
    until: str = None,
    limit: int = 500,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "family_principal", "operations", "auditor")),
):
    """审计导出：返回筛选后的结构化审计事件。"""
    limit = max(1, min(limit, 2000))
    rows = db.get_audit_log(limit=limit, action=action, actor=actor, since=since, until=until)
    return {
        "exported_by": user["username"],
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filters": {"action": action, "actor": actor, "since": since, "until": until},
        "count": len(rows),
        "rows": rows,
    }


@router.get("/reports/accounts/{account_id}/monthly")
async def get_account_monthly_report(
    account_id: str,
    year: int = None,
    month: int = None,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """成员月度报表：资产、份额、流水和出金状态。"""
    ensure_account_permission(db, user, account_id, permission="view")
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    try:
        return db.get_account_monthly_report(account_id, year, month)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/reports/family/monthly")
async def get_family_monthly_report(
    year: int = None,
    month: int = None,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "family_principal", "investment_manager", "operations", "auditor")),
):
    """家族全局月报：AUM、账户汇总、流水和出金审批状态。"""
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    return db.get_family_global_report(year, month)


@router.post("/withdraw/{withdrawal_id}/execute")
async def execute_withdrawal(
    withdrawal_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "platform_admin", "investment_manager", "operations")),
):
    """执行已批准的出金请求：从组合扣减资金"""
    engine = CashflowEngine(db)
    result = engine.execute_withdrawal(
        withdrawal_id=withdrawal_id,
        executor=user["username"],
    )
    if result.status == "ERROR":
        code = 404 if "不存在" in result.message else 400
        raise HTTPException(status_code=code, detail=result.message)
    return result.to_dict()


@router.get("/layers/{portfolio_id}")
async def get_layer_status(
    portfolio_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查询三层资金分布：Core / Stability / Alpha"""
    ensure_portfolio_access(db, user, portfolio_id, permission="view")
    engine = CashflowEngine(db)
    result = engine.get_layer_status(portfolio_id=portfolio_id)
    if result.status == "ERROR":
        raise HTTPException(status_code=404, detail=result.message)
    return result.to_dict()
