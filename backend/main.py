"""
Fullway 邮件便捷处理工具 - FastAPI 后端
部署到 Railway，前端 PWA 部署在 GitHub Pages
"""
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware

from core.config import get_allowed_origin
from core.auth import verify_token
from routers import emails, database, config_api

app = FastAPI(
    title="Fullway Email API",
    version="1.0.0",
    docs_url="/docs",       # 可通过 /docs 访问 Swagger UI（调试用）
    redoc_url=None,
)

# CORS：仅允许配置的前端域名
allowed_origin = get_allowed_origin()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[allowed_origin] if allowed_origin != "*" else ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

# 所有业务路由均需要 Bearer Token
_auth = [Depends(verify_token)]

app.include_router(emails.router,     prefix="/api/emails",  dependencies=_auth)
app.include_router(database.router,   prefix="/api/db",      dependencies=_auth)
app.include_router(config_api.router, prefix="/api/config",  dependencies=_auth)


@app.get("/health", tags=["system"])
def health_check():
    return {"status": "ok"}
