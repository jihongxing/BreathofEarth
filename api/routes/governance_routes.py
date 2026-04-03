"""
治理层路由 — 入金、出金审批、出金执行、审计日志

业务逻辑委托给 engine/governance.py 和 engine/cashflow.py，此处只做 HTTP 层。
"""

from fastapi import APIRouter, Depends, HTTPException

from db.database import Database
from engine.governance import WithdrawalGovernance
from engine.cashflow import CashflowEngine
from api.deps import get_db, get_current_user, require_role
from api.models import WithdrawalRequest, ApprovalRequest, DepositRequest

router = APIRouter(prefix="/api/governance", tags=["治理"])


@router.post("/deposit/preview")
async def deposit_preview(
    req: DepositRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """入金预览：展示分配方案，供用户确认后再执行"""
    engine = CashflowEngine(db)
    result = engine.deposit_preview(
        amount=req.amount,
        portfolio_id=req.portfolio_id,
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.post("/deposit")
async def deposit(
    req: DepositRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """入金：资金按三层结构分配进入组合"""
    engine = CashflowEngine(db)
    result = engine.deposit(
        amount=req.amount,
        depositor=user["username"],
        portfolio_id=req.portfolio_id,
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
    return db.list_deposit_records(portfolio_id=portfolio_id)


@router.post("/withdraw/preview")
async def withdrawal_preview(
    req: WithdrawalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """出金预览：展示扣减方案"""
    engine = CashflowEngine(db)
    result = engine.withdrawal_preview(
        amount=req.amount,
        portfolio_id=req.portfolio_id,
    )
    if result.status == "ERROR":
        raise HTTPException(status_code=400, detail=result.message)
    return result.to_dict()


@router.post("/withdraw")
async def request_withdrawal(
    req: WithdrawalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """发起出金请求"""
    gov = WithdrawalGovernance(db)
    result = gov.request_withdrawal(
        amount=req.amount,
        reason=req.reason,
        requester=user["username"],
        portfolio_id=req.portfolio_id,
    )
    return result.to_dict()


@router.post("/withdraw/{withdrawal_id}/approve")
async def approve_withdrawal(
    withdrawal_id: str,
    req: ApprovalRequest,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin", "member")),
):
    """审批出金请求"""
    gov = WithdrawalGovernance(db)
    result = gov.approve_withdrawal(
        withdrawal_id=withdrawal_id,
        approver=user["username"],
        decision=req.decision,
        comment=req.comment,
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
    db: Database = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """查看出金请求列表"""
    requests = db.list_withdrawal_requests(status=status)
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
    withdrawal["approvals"] = db.get_withdrawal_approvals(withdrawal_id)
    return withdrawal


@router.get("/audit-log")
async def get_audit_log(
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
):
    """查看审计日志（仅管理员）"""
    return db.get_audit_log(limit=50)


@router.post("/withdraw/{withdrawal_id}/execute")
async def execute_withdrawal(
    withdrawal_id: str,
    db: Database = Depends(get_db),
    user: dict = Depends(require_role("admin")),
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
    engine = CashflowEngine(db)
    result = engine.get_layer_status(portfolio_id=portfolio_id)
    if result.status == "ERROR":
        raise HTTPException(status_code=404, detail=result.message)
    return result.to_dict()
