"""
Bearer Token 认证
所有 API 端点均需要在请求头携带：
    Authorization: Bearer <API_TOKEN>
API_TOKEN 通过环境变量设置。
"""
import os
from fastapi import Security, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

_security = HTTPBearer()


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(_security),
) -> str:
    expected = os.environ.get("API_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_TOKEN 环境变量未配置",
        )
    if credentials.credentials != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token 无效",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials
