"""清关公司 & 草稿模板管理路由（CRUD）"""
from fastapi import APIRouter, HTTPException

from core.config import get_default_cc, get_forward_body
from models.schemas import BrokerCreate, TemplateCreate, OpResult
from services.parser_service import (
    get_brokers, upsert_broker, delete_broker,
    get_templates, upsert_template, delete_template,
    _ensure_config_tables,
)

router = APIRouter(tags=["config"])


def _init():
    try:
        _ensure_config_tables()
    except Exception:
        pass


# ── 清关公司 ──────────────────────────────────────────────────

@router.get("/brokers", response_model=dict)
def list_brokers():
    _init()
    return get_brokers()


@router.post("/brokers", response_model=OpResult)
def create_broker(req: BrokerCreate):
    _init()
    ok = upsert_broker(req.name, req.emails)
    return OpResult(success=ok, message="已保存" if ok else "保存失败")


@router.put("/brokers/{name}", response_model=OpResult)
def update_broker(name: str, req: BrokerCreate):
    _init()
    ok = upsert_broker(name, req.emails)
    return OpResult(success=ok, message="已更新" if ok else "更新失败")


@router.delete("/brokers/{name}", response_model=OpResult)
def remove_broker(name: str):
    _init()
    ok = delete_broker(name)
    return OpResult(success=ok, message="已删除" if ok else "删除失败")


# ── 草稿模板 ──────────────────────────────────────────────────

@router.get("/templates", response_model=list)
def list_templates():
    _init()
    return get_templates()


@router.post("/templates", response_model=OpResult)
def create_template(req: TemplateCreate):
    _init()
    ok = upsert_template(req.name, req.body)
    return OpResult(success=ok, message="已保存" if ok else "保存失败")


@router.put("/templates/{name}", response_model=OpResult)
def update_template(name: str, req: TemplateCreate):
    _init()
    ok = upsert_template(name, req.body)
    return OpResult(success=ok, message="已更新" if ok else "更新失败")


@router.delete("/templates/{name}", response_model=OpResult)
def remove_template(name: str):
    _init()
    ok = delete_template(name)
    return OpResult(success=ok, message="已删除" if ok else "删除失败")


# ── 全局配置 ──────────────────────────────────────────────────

@router.get("/defaults", response_model=dict)
def get_defaults():
    return {
        "cc": get_default_cc(),
        "forward_body": get_forward_body(),
    }
