"""数据库操作路由：解析邮件、写入/更新 MySQL"""
from fastapi import APIRouter

from models.schemas import InsertDbRequest, UpdateProgressRequest, UpdateMingxiRequest, OpResult
from services.parser_service import (
    parse_full_email, parse_email_text,
    insert_to_database, update_progress_by_bl,
    update_mingxi_progress, query_broker_by_bl,
    extract_attachment_text,
)

router = APIRouter(tags=["database"])


@router.post("/parse", response_model=dict)
def parse_email(req: InsertDbRequest):
    """综合解析（标题+正文+附件），返回结构化字段"""
    parsed = req.parsed
    subject = parsed.get("subject", "")
    from_addr = parsed.get("from_addr", "")
    body_text = parsed.get("body_text", "")
    attachment_texts = parsed.get("attachment_texts", [])
    return parse_full_email(subject, from_addr, body_text, attachment_texts)


@router.post("/parse/text", response_model=dict)
def parse_text(body: dict):
    """解析单段文字"""
    text = body.get("text", "")
    return parse_email_text(text)


@router.post("/insert", response_model=dict)
def insert_db(req: InsertDbRequest):
    return insert_to_database(req.parsed)


@router.put("/progress", response_model=OpResult)
def update_progress(req: UpdateProgressRequest):
    result = update_progress_by_bl(req.bl_no, req.progress_value)
    return OpResult(success=result["success"], message=result["message"])


@router.put("/mingxi", response_model=OpResult)
def update_mingxi(req: UpdateMingxiRequest):
    result = update_mingxi_progress(req.bl_nos)
    return OpResult(success=result["success"], message=result["message"])


@router.get("/broker/{bl_no}", response_model=dict)
def get_broker_by_bl(bl_no: str):
    broker = query_broker_by_bl(bl_no)
    return {"bl_no": bl_no, "broker": broker}
