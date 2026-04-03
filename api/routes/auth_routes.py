"""
认证相关路由
"""

from fastapi import APIRouter, Depends, HTTPException, status

from db.database import Database
from api.auth import hash_password, verify_password, create_access_token
from api.deps import get_db, get_current_user, require_role
from api.models import LoginRequest, LoginResponse, InitUserRequest

router = APIRouter(prefix="/api/auth", tags=["认证"])


@router.post("/login", response_model=LoginResponse)
async def login(req: LoginRequest, db: Database = Depends(get_db)):
    """登录获取 JWT Token"""
    user = db.get_user_by_username(req.username)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="用户名或密码错误")

    token = create_access_token({"sub": user["username"], "role": user["role"]})
    db.save_audit_log("LOGIN", user["username"], "登录成功")
    return LoginResponse(
        access_token=token,
        role=user["role"],
        display_name=user.get("display_name") or user["username"],
    )


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """获取当前用户信息"""
    return {
        "username": user["username"],
        "role": user["role"],
        "display_name": user.get("display_name") or user["username"],
        "email": user.get("email", ""),
    }
