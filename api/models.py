"""
API 请求/响应模型（Pydantic）
"""

from typing import Optional
from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    display_name: str
    member_id: Optional[str] = None


class WithdrawalRequest(BaseModel):
    amount: float = Field(gt=0, description="出金金额（USD）")
    reason: str = Field(min_length=1, max_length=500)
    portfolio_id: str = "us"
    account_id: Optional[str] = None
    source_pool_id: Optional[str] = None


class ApprovalRequest(BaseModel):
    decision: str = Field(pattern="^(APPROVED|REJECTED)$")
    comment: Optional[str] = ""


class DepositRequest(BaseModel):
    amount: float = Field(gt=0, description="入金金额（USD）")
    portfolio_id: str = "us"
    account_id: Optional[str] = None


class DepositConfirmRequest(BaseModel):
    external_reference: str = Field(default="", max_length=120)
    note: str = Field(default="", max_length=300)


class PoolRevalueRequest(BaseModel):
    nav: Optional[float] = Field(default=None, gt=0)
    snapshot_date: Optional[str] = None
    source: str = Field(default="NAV_REVALUED", max_length=80)


class InitUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    display_name: str = ""
    email: str = ""
    member_id: Optional[str] = None


class BindMemberRequest(BaseModel):
    username: str = Field(min_length=1)
    member_id: Optional[str] = None


class FamilyMemberCreateRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=100)
    member_type: str = Field(default="individual", max_length=50)
    risk_profile: str = Field(default="balanced", max_length=50)


class CapitalAccountCreateRequest(BaseModel):
    member_id: str = Field(min_length=1)
    account_name: str = Field(min_length=1, max_length=100)
    base_currency: str = Field(default="USD", min_length=3, max_length=8)
    default_portfolio_id: str = Field(default="us", min_length=1, max_length=50)


class AccountPermissionRequest(BaseModel):
    username: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    permission: str = Field(default="view", pattern="^(view|deposit_request|withdraw_request|approve_withdrawal|manage_account|admin)$")
