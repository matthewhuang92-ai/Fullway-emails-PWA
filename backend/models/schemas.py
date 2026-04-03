"""Pydantic 数据模型"""
from __future__ import annotations
from typing import Optional
from pydantic import BaseModel


# ── 附件元信息（不含文件内容） ───────────────────────────────

class AttachmentMeta(BaseModel):
    index: int
    filename: str
    content_type: str
    size: int  # bytes


# ── 附件内容（base64） ────────────────────────────────────────

class AttachmentContent(BaseModel):
    index: int
    filename: str
    content_type: str
    data_base64: str
    size: int


# ── 邮件摘要（列表用） ────────────────────────────────────────

class EmailSummary(BaseModel):
    id: str
    folder: str
    subject: str
    from_addr: str
    to_addr: str
    date: str
    attachments: list[AttachmentMeta]


# ── 邮件详情（含正文） ────────────────────────────────────────

class EmailDetail(EmailSummary):
    body_text: str
    body_html: Optional[str] = None


# ── 转发正本请求 ──────────────────────────────────────────────

class ForwardRequest(BaseModel):
    email_id: str
    folder: str
    broker_name: str
    to_addrs: list[str]
    cc_addrs: list[str]
    forward_body: str
    selected_attachment_indices: list[int]  # 空列表 = 全部


# ── 转发草稿请求 ──────────────────────────────────────────────

class DraftRequest(BaseModel):
    email_id: str
    folder: str
    broker_name: str
    to_addrs: list[str]
    body_text: str


# ── 标记已读请求 ──────────────────────────────────────────────

class MarkReadRequest(BaseModel):
    email_id: str
    folder: str = "INBOX"


# ── 搜索请求 ─────────────────────────────────────────────────

class SearchRequest(BaseModel):
    keyword: str
    search_in: str = "all"   # subject / from / body / all
    folders: list[str] = ["INBOX"]
    date_from: Optional[str] = None   # YYYY-MM-DD
    date_to: Optional[str] = None
    max_results: int = 100


# ── 数据库操作 ────────────────────────────────────────────────

class InsertDbRequest(BaseModel):
    parsed: dict


class UpdateProgressRequest(BaseModel):
    bl_no: str
    progress_value: str


class UpdateMingxiRequest(BaseModel):
    bl_nos: list[str]


# ── 清关公司 ──────────────────────────────────────────────────

class BrokerCreate(BaseModel):
    name: str
    emails: list[str]


# ── 草稿模板 ──────────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str
    body: str


# ── 通用响应 ──────────────────────────────────────────────────

class OpResult(BaseModel):
    success: bool
    message: str = ""
