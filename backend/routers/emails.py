"""邮件相关路由：读取、搜索、发送、转发"""
from __future__ import annotations

from datetime import date
from fastapi import APIRouter, HTTPException

from core.config import get_email_config, get_default_cc, get_forward_body
from models.schemas import (
    EmailSummary, EmailDetail, AttachmentContent,
    ForwardRequest, DraftRequest, SearchRequest, ReplyAllRequest, OpResult,
)
from services.email_service import (
    EmailService, attachment_to_meta, attachment_to_content,
    get_email_body_text, get_email_body_html,
)

router = APIRouter(tags=["emails"])


def _make_service() -> EmailService:
    cfg = {"email": get_email_config()}
    return EmailService(cfg)


# ── 获取未读邮件 ──────────────────────────────────────────────

@router.get("/unread", response_model=list[EmailSummary])
def get_unread_emails():
    svc = _make_service()
    try:
        svc.connect_imap()
        emails = svc.fetch_unread_emails()
        return [
            EmailSummary(
                id=e["id"],
                folder="INBOX",
                subject=e["subject"],
                from_addr=e["from_addr"],
                to_addr=e["to_addr"],
                date=e["date"],
                attachments=[attachment_to_meta(a, i) for i, a in enumerate(e["attachments"])],
            )
            for e in emails
        ]
    finally:
        svc.disconnect()


# ── 获取完整邮件详情 ──────────────────────────────────────────

@router.get("/{folder}/{email_id}", response_model=EmailDetail)
def get_email_detail(folder: str, email_id: str):
    svc = _make_service()
    try:
        svc.connect_imap()
        e = svc.fetch_full_email(email_id, folder)
        if not e:
            raise HTTPException(status_code=404, detail="邮件不存在")
        return EmailDetail(
            id=e["id"],
            folder=folder,
            subject=e["subject"],
            from_addr=e["from_addr"],
            to_addr=e["to_addr"],
            date=e["date"],
            body_text=get_email_body_text(e["msg"]),
            body_html=get_email_body_html(e["msg"]),
            attachments=[attachment_to_meta(a, i) for i, a in enumerate(e["attachments"])],
        )
    finally:
        svc.disconnect()


# ── 获取单个附件内容（base64） ────────────────────────────────

@router.get("/{folder}/{email_id}/attachments/{index}", response_model=AttachmentContent)
def get_attachment(folder: str, email_id: str, index: int):
    svc = _make_service()
    try:
        svc.connect_imap()
        e = svc.fetch_full_email(email_id, folder)
        if not e:
            raise HTTPException(status_code=404, detail="邮件不存在")
        if index < 0 or index >= len(e["attachments"]):
            raise HTTPException(status_code=404, detail="附件不存在")
        return attachment_to_content(e["attachments"][index], index)
    finally:
        svc.disconnect()


# ── 邮件夹列表 ────────────────────────────────────────────────

@router.get("/folders", response_model=list[str])
def get_folders():
    svc = _make_service()
    try:
        svc.connect_imap()
        return svc.get_folders()
    finally:
        svc.disconnect()


# ── 搜索邮件 ──────────────────────────────────────────────────

@router.post("/search", response_model=list[EmailSummary])
def search_emails(req: SearchRequest):
    svc = _make_service()
    try:
        svc.connect_imap()
        date_from = date.fromisoformat(req.date_from) if req.date_from else None
        date_to = date.fromisoformat(req.date_to) if req.date_to else None
        results = svc.search_emails(
            keyword=req.keyword,
            search_in=req.search_in,
            folders=req.folders,
            date_from=date_from,
            date_to=date_to,
            max_results=req.max_results,
        )
        return [
            EmailSummary(
                id=e["id"],
                folder=e["folder"],
                subject=e["subject"],
                from_addr=e["from_addr"],
                to_addr=e["to_addr"],
                date=e["date"],
                attachments=[],
            )
            for e in results
        ]
    finally:
        svc.disconnect()


# ── 标记已读 ──────────────────────────────────────────────────

@router.post("/{folder}/{email_id}/mark-read", response_model=OpResult)
def mark_read(folder: str, email_id: str):
    svc = _make_service()
    try:
        svc.connect_imap()
        svc.mark_as_read(email_id, folder)
        return OpResult(success=True, message="已标记为已读")
    except Exception as e:
        return OpResult(success=False, message=str(e))
    finally:
        svc.disconnect()


# ── 转发正本 ──────────────────────────────────────────────────

@router.post("/forward", response_model=OpResult)
def forward_original(req: ForwardRequest):
    svc = _make_service()
    try:
        svc.connect_imap()
        svc.connect_smtp()
        e = svc.fetch_full_email(req.email_id, req.folder)
        if not e:
            raise HTTPException(status_code=404, detail="邮件不存在")

        cc = req.cc_addrs if req.cc_addrs else get_default_cc()
        body = req.forward_body if req.forward_body else get_forward_body()

        saved = svc.forward_email(
            original_email=e,
            to_addrs=req.to_addrs,
            cc_addrs=cc,
            forward_body=body,
            selected_attachment_indices=req.selected_attachment_indices,
        )
        msg = "转发成功" + ("，已保存到已发送" if saved else "（保存已发送失败，但邮件已发出）")
        return OpResult(success=True, message=msg)
    except HTTPException:
        raise
    except Exception as e:
        return OpResult(success=False, message=f"转发失败：{e}")
    finally:
        svc.disconnect()


# ── 转发草稿 ──────────────────────────────────────────────────

@router.post("/forward-draft", response_model=OpResult)
def forward_draft(req: DraftRequest):
    svc = _make_service()
    try:
        svc.connect_imap()
        svc.connect_smtp()
        e = svc.fetch_full_email(req.email_id, req.folder)
        if not e:
            raise HTTPException(status_code=404, detail="邮件不存在")

        saved, marked = svc.send_draft_email(
            original_email=e,
            to_addrs=req.to_addrs,
            body_text=req.body_text,
        )
        msg = "草稿发送成功"
        if not saved:
            msg += "（保存已发送失败）"
        if not marked:
            msg += "（标记已读失败）"
        return OpResult(success=True, message=msg)
    except HTTPException:
        raise
    except Exception as e:
        return OpResult(success=False, message=f"发送失败：{e}")
    finally:
        svc.disconnect()


# ── 回复全部 ──────────────────────────────────────────────────

@router.post("/reply-all", response_model=OpResult)
def reply_all_email(req: ReplyAllRequest):
    svc = _make_service()
    try:
        svc.connect_imap()
        svc.connect_smtp()
        e = svc.fetch_full_email(req.email_id, req.folder)
        if not e:
            raise HTTPException(status_code=404, detail="邮件不存在")
        saved = svc.send_reply_all(original_email=e, reply_body=req.reply_body)
        msg = "回复成功" + ("，已保存到已发送" if saved else "（保存已发送失败，但邮件已发出）")
        return OpResult(success=True, message=msg)
    except HTTPException:
        raise
    except Exception as e:
        return OpResult(success=False, message=f"回复失败：{e}")
    finally:
        svc.disconnect()
