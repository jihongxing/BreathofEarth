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


class WithdrawalRequest(BaseModel):
    amount: float = Field(gt=0, description="出金金额（USD）")
    reason: str = Field(min_length=1, max_length=500)
    portfolio_id: str = "us"


class ApprovalRequest(BaseModel):
    decision: str = Field(pattern="^(APPROVED|REJECTED)$")
    comment: Optional[str] = ""


class DepositRequest(BaseModel):
    amount: float = Field(gt=0, description="入金金额（USD）")
    portfolio_id: str = "us"


class InitUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"
    display_name: str = ""
    email: str = ""
