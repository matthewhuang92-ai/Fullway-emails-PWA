"""
邮件服务层 — 从原 邮件便捷处理工具_gui.py 的 EmailService 类迁移
去除所有 tkinter 依赖，改用 logging 代替 print/GUI回调
"""
from __future__ import annotations

import base64
import imaplib
import io
import logging
import smtplib
import ssl
import time as _time
from email import encoders
from email.header import decode_header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
import email as email_lib
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


# ── SSL ──────────────────────────────────────────────────────

def _create_ssl_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_default_certs()
    return ctx


# ── 邮件头解码 ────────────────────────────────────────────────

def decode_mime_header(header_value: str | None) -> str:
    if header_value is None:
        return ""
    parts = decode_header(header_value)
    decoded = []
    for content, charset in parts:
        if isinstance(content, bytes):
            charset = charset or "utf-8"
            try:
                decoded.append(content.decode(charset, errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded.append(content.decode("utf-8", errors="replace"))
        else:
            decoded.append(content)
    return "".join(decoded)


# ── 日期格式化 ────────────────────────────────────────────────

def format_date(date_str: str | None) -> str:
    if not date_str:
        return "未知日期"
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        dt_local = dt.astimezone(timezone(timedelta(hours=8)))
        return dt_local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return date_str


# ── 正文提取 ──────────────────────────────────────────────────

def get_email_body_text(msg) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                try:
                    body = part.get_payload(decode=True).decode(charset, errors="replace")
                except Exception:
                    body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                break
    else:
        charset = msg.get_content_charset() or "utf-8"
        try:
            body = msg.get_payload(decode=True).decode(charset, errors="replace")
        except Exception:
            body = msg.get_payload(decode=True).decode("utf-8", errors="replace")
    return body.strip()


def get_email_body_html(msg) -> str | None:
    """提取 HTML 正文（用于前端渲染，优先级低于纯文本）"""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/html" and "attachment" not in cd:
                charset = part.get_content_charset() or "utf-8"
                try:
                    return part.get_payload(decode=True).decode(charset, errors="replace")
                except Exception:
                    return part.get_payload(decode=True).decode("utf-8", errors="replace")
    return None


# ── 附件提取 ──────────────────────────────────────────────────

def get_attachments(msg) -> list[dict]:
    """提取所有附件，返回 [{filename, data(bytes), content_type}, ...]"""
    attachments = []
    for part in msg.walk():
        cd = str(part.get("Content-Disposition", ""))
        if "attachment" in cd or "inline" in cd:
            filename = part.get_filename()
            if filename:
                filename = decode_mime_header(filename)
                data = part.get_payload(decode=True)
                if data:
                    attachments.append({
                        "filename": filename,
                        "data": data,
                        "content_type": part.get_content_type(),
                    })
    return attachments


def attachment_to_meta(att: dict, index: int) -> dict:
    """将附件 dict 转为前端友好的元信息（不含内容）"""
    return {
        "index": index,
        "filename": att["filename"],
        "content_type": att["content_type"],
        "size": len(att["data"]),
    }


def attachment_to_content(att: dict, index: int) -> dict:
    """将附件 dict 转为含 base64 内容的结构"""
    return {
        "index": index,
        "filename": att["filename"],
        "content_type": att["content_type"],
        "data_base64": base64.b64encode(att["data"]).decode("ascii"),
        "size": len(att["data"]),
    }


# ── 主服务类 ──────────────────────────────────────────────────

class EmailService:
    """
    封装 IMAP / SMTP 操作。
    每个请求创建新实例（无状态 API），使用完毕后调用 disconnect()。
    """

    def __init__(self, config: dict):
        self.config = config
        self.imap: imaplib.IMAP4_SSL | None = None
        self.smtp: smtplib.SMTP_SSL | None = None

    # ── 连接管理 ──────────────────────────────────────────────

    def connect_imap(self):
        cfg = self.config["email"]
        ssl_ctx = _create_ssl_context()
        self.imap = imaplib.IMAP4_SSL(cfg["imap_server"], cfg["imap_port"], ssl_context=ssl_ctx)
        self.imap.login(cfg["address"], cfg["password"])
        self.imap.select("INBOX")
        log.info("IMAP 连接成功")

    def connect_smtp(self):
        cfg = self.config["email"]
        ssl_ctx = _create_ssl_context()
        self.smtp = smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"], timeout=30, context=ssl_ctx)
        self.smtp.login(cfg["address"], cfg["password"])
        log.info("SMTP 连接成功")

    def ensure_imap(self):
        try:
            if self.imap:
                status, _ = self.imap.noop()
                if status == "OK":
                    return
        except Exception:
            pass
        try:
            if self.imap:
                self.imap.logout()
        except Exception:
            pass
        self.connect_imap()

    def ensure_smtp(self):
        try:
            if self.smtp and self.smtp.noop()[0] == 250:
                return
        except Exception:
            pass
        try:
            if self.smtp:
                self.smtp.quit()
        except Exception:
            pass
        self.connect_smtp()

    def disconnect(self):
        for conn, method in [(self.smtp, "quit"), (self.imap, "logout")]:
            if conn:
                try:
                    getattr(conn, method)()
                except Exception:
                    pass

    # ── 读取邮件 ──────────────────────────────────────────────

    def fetch_unread_emails(self) -> list[dict]:
        """获取所有未读邮件（含附件内容，用于转发）"""
        try:
            if self.imap:
                self.imap.logout()
        except Exception:
            pass
        self.imap = None
        self.connect_imap()

        try:
            self.imap.examine("INBOX")
        except Exception:
            pass
        self.imap.select("INBOX", readonly=False)

        status, messages = self.imap.search(None, "UNSEEN")
        if status != "OK":
            return []

        email_ids = messages[0].split()
        if not email_ids:
            return []

        emails = []
        for eid in email_ids:
            status, msg_data = self.imap.fetch(eid, "(BODY.PEEK[])")
            if status != "OK":
                continue
            raw_email = msg_data[0][1]
            msg = email_lib.message_from_bytes(raw_email)
            atts = get_attachments(msg)
            emails.append({
                "id": eid.decode() if isinstance(eid, bytes) else str(eid),
                "folder": "INBOX",
                "subject": decode_mime_header(msg.get("Subject")),
                "from_addr": decode_mime_header(msg.get("From")),
                "to_addr": decode_mime_header(msg.get("To")),
                "date": format_date(msg.get("Date")),
                "msg": msg,
                "attachments": atts,
            })
        return emails

    def get_folders(self) -> list[str]:
        self.ensure_imap()
        status, folder_list = self.imap.list()
        if status != "OK":
            return ["INBOX"]
        folders = []
        for item in folder_list:
            if isinstance(item, bytes):
                item = item.decode("utf-8", errors="replace")
            if '"' in item:
                parts = item.rsplit('"', 2)
                name = parts[-2] if len(parts) >= 2 else item.split()[-1]
            else:
                name = item.split()[-1]
            name = name.strip().strip('"')
            if name and name not in folders:
                folders.append(name)
        return folders or ["INBOX"]

    def search_emails(self, keyword: str, search_in: str = "all",
                      folders: list[str] | None = None,
                      date_from=None, date_to=None,
                      max_results: int = 100) -> list[dict]:
        if folders is None:
            folders = ["INBOX"]
        all_results = []

        for folder in folders:
            if len(all_results) >= max_results:
                break
            try:
                self.ensure_imap()
                try:
                    status, _ = self.imap.select(f'"{folder}"', readonly=True)
                except Exception:
                    status, _ = self.imap.select(folder, readonly=True)
                if status != "OK":
                    continue

                criteria_parts = []
                if keyword:
                    search_map = {
                        "subject": f'SUBJECT "{keyword}"',
                        "from":    f'FROM "{keyword}"',
                        "body":    f'BODY "{keyword}"',
                        "all":     f'TEXT "{keyword}"',
                    }
                    criteria_parts.append(search_map.get(search_in, f'TEXT "{keyword}"'))
                else:
                    criteria_parts.append("ALL")

                if date_from:
                    criteria_parts.append(f'SINCE "{date_from.strftime("%d-%b-%Y")}"')
                if date_to:
                    criteria_parts.append(f'BEFORE "{date_to.strftime("%d-%b-%Y")}"')

                criteria = " ".join(criteria_parts)
                try:
                    if keyword:
                        status, messages = self.imap.search("UTF-8", criteria)
                    else:
                        status, messages = self.imap.search(None, criteria)
                except Exception:
                    status, messages = self.imap.search(None, criteria)

                if status != "OK":
                    continue

                email_ids = list(reversed(messages[0].split()))
                remaining = max_results - len(all_results)
                email_ids = email_ids[:remaining]

                for eid in email_ids:
                    try:
                        status, msg_data = self.imap.fetch(
                            eid,
                            "(BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)])"
                        )
                        if status != "OK":
                            continue
                        msg = email_lib.message_from_bytes(msg_data[0][1])
                        all_results.append({
                            "id": eid.decode() if isinstance(eid, bytes) else str(eid),
                            "folder": folder,
                            "subject": decode_mime_header(msg.get("Subject")),
                            "from_addr": decode_mime_header(msg.get("From")),
                            "to_addr": decode_mime_header(msg.get("To")),
                            "date": format_date(msg.get("Date")),
                            "attachments": [],
                        })
                    except Exception:
                        continue
            except Exception:
                continue

        try:
            self.imap.select("INBOX")
        except Exception:
            pass

        if keyword:
            kw_lower = keyword.lower()
            def _match(em):
                if search_in in ("subject", "all") and kw_lower in em["subject"].lower():
                    return True
                if search_in in ("from", "all") and kw_lower in em["from_addr"].lower():
                    return True
                if search_in == "body":
                    return True
                return False
            all_results = [e for e in all_results if _match(e)]

        return all_results

    def fetch_full_email(self, email_id: str, folder: str) -> dict | None:
        """拉取完整邮件（含附件），不标记已读"""
        self.ensure_imap()
        eid = email_id.encode() if isinstance(email_id, str) else email_id
        try:
            self.imap.select(f'"{folder}"', readonly=True)
        except Exception:
            self.imap.select(folder, readonly=True)

        status, msg_data = self.imap.fetch(eid, "(BODY.PEEK[])")
        try:
            self.imap.select("INBOX")
        except Exception:
            pass

        if status != "OK":
            return None

        raw_email = msg_data[0][1]
        msg = email_lib.message_from_bytes(raw_email)
        atts = get_attachments(msg)
        return {
            "id": email_id,
            "folder": folder,
            "subject": decode_mime_header(msg.get("Subject")),
            "from_addr": decode_mime_header(msg.get("From")),
            "to_addr": decode_mime_header(msg.get("To")),
            "date": format_date(msg.get("Date")),
            "msg": msg,
            "attachments": atts,
        }

    def mark_as_read(self, email_id: str, folder: str = "INBOX"):
        self.ensure_imap()
        try:
            self.imap.select(folder)
        except Exception:
            pass
        eid = email_id.encode() if isinstance(email_id, str) else email_id
        self.imap.store(eid, "+FLAGS", "\\Seen")

    # ── 发送邮件 ──────────────────────────────────────────────

    def forward_email(self, original_email: dict, to_addrs: list[str],
                      cc_addrs: list[str], forward_body: str,
                      selected_attachment_indices: list[int]) -> bool:
        self.ensure_smtp()
        self.ensure_imap()

        cfg = self.config["email"]
        atts = original_email["attachments"]
        if selected_attachment_indices:
            atts = [a for i, a in enumerate(atts) if i in selected_attachment_indices]

        msg = MIMEMultipart()
        msg["From"] = formataddr(("", cfg["address"]))
        msg["To"] = ", ".join(to_addrs)
        msg["Cc"] = ", ".join(cc_addrs)
        subj = original_email["subject"]
        msg["Subject"] = subj if subj.upper().startswith("FW:") else f"Fw: {subj}"

        orig_section = (
            "\n\n\n"
            "------------------ Original ------------------\n"
            f"From: {original_email['from_addr']}\n"
            f"Date: {original_email['date']}\n"
            f"To: {original_email.get('to_addr', '')}\n"
            f"Subject: {original_email['subject']}\n"
            "\n"
            f"{get_email_body_text(original_email['msg'])}"
        )
        msg.attach(MIMEText(forward_body + orig_section, "plain", "utf-8"))

        for att in atts:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(att["data"])
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment",
                            filename=("utf-8", "", att["filename"]))
            msg.attach(part)

        self.smtp.sendmail(cfg["address"], to_addrs + cc_addrs, msg.as_string())

        saved = False
        try:
            status, _ = self.imap.append(
                '"Sent Messages"', "\\Seen",
                imaplib.Time2Internaldate(_time.time()),
                msg.as_bytes()
            )
            self.imap.select("INBOX")
            saved = (status == "OK")
        except Exception:
            try:
                self.imap.select("INBOX")
            except Exception:
                pass

        try:
            self.mark_as_read(original_email["id"], original_email.get("folder", "INBOX"))
        except Exception:
            pass

        return saved

    def send_draft_email(self, original_email: dict, to_addrs: list[str],
                         body_text: str) -> tuple[bool, bool]:
        self.ensure_smtp()
        self.ensure_imap()

        cfg = self.config["email"]
        msg = MIMEMultipart()
        msg["From"] = formataddr(("", cfg["address"]))
        msg["To"] = ", ".join(to_addrs)
        subj = original_email["subject"]
        msg["Subject"] = subj if subj.upper().startswith("FW:") else f"Fw: {subj}"

        orig_section = (
            "\n\n\n"
            "------------------ Original ------------------\n"
            f"From: {original_email['from_addr']}\n"
            f"Date: {original_email['date']}\n"
            f"To: {original_email.get('to_addr', '')}\n"
            f"Subject: {original_email['subject']}\n"
            "\n"
            f"{get_email_body_text(original_email['msg'])}"
        )
        msg.attach(MIMEText(body_text + orig_section, "plain", "utf-8"))

        for att in original_email["attachments"]:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(att["data"])
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment",
                            filename=("utf-8", "", att["filename"]))
            msg.attach(part)

        self.smtp.sendmail(cfg["address"], to_addrs, msg.as_string())

        saved = False
        try:
            status, _ = self.imap.append(
                '"Sent Messages"', "\\Seen",
                imaplib.Time2Internaldate(_time.time()),
                msg.as_bytes()
            )
            saved = (status == "OK")
        except Exception:
            pass

        marked = False
        try:
            self.ensure_imap()
            self.mark_as_read(original_email["id"], original_email.get("folder", "INBOX"))
            marked = True
        except Exception:
            pass

        return saved, marked

    def send_reply_all(self, original_email: dict, reply_body: str) -> bool:
        """回复全部：回复给原发件人，抄送所有原收件人（不含自己），并保存到已发送"""
        self.ensure_smtp()
        self.ensure_imap()

        import email.utils as _eu

        cfg = self.config["email"]
        my_addr = cfg["address"].lower()

        orig_from_addr = _eu.parseaddr(original_email.get("from_addr", ""))[1]
        orig_to_raw    = original_email.get("to_addr", "")
        orig_to_addrs  = [addr for name, addr in _eu.getaddresses([orig_to_raw])
                          if addr and addr.lower() != my_addr]

        to_addrs = [orig_from_addr] if orig_from_addr else []
        cc_addrs = [a for a in orig_to_addrs if a.lower() != orig_from_addr.lower()]

        msg = MIMEMultipart()
        msg["From"]    = formataddr(("", cfg["address"]))
        msg["To"]      = orig_from_addr
        if cc_addrs:
            msg["Cc"] = ", ".join(cc_addrs)

        subj = original_email.get("subject", "")
        msg["Subject"] = f"Re: {subj}" if not subj.lower().startswith("re:") else subj

        orig_section = (
            "\n\n\n"
            "------------------ Original ------------------\n"
            f"From: {original_email['from_addr']}\n"
            f"Date: {original_email['date']}\n"
            f"To: {original_email.get('to_addr', '')}\n"
            f"Subject: {original_email['subject']}\n"
            "\n"
            f"{get_email_body_text(original_email['msg'])}"
        )
        msg.attach(MIMEText(reply_body + orig_section, "plain", "utf-8"))

        all_recipients = to_addrs + cc_addrs
        self.smtp.sendmail(cfg["address"], all_recipients, msg.as_string())

        saved = False
        try:
            status, _ = self.imap.append(
                '"Sent Messages"', "\\Seen",
                imaplib.Time2Internaldate(_time.time()),
                msg.as_bytes()
            )
            saved = (status == "OK")
        except Exception:
            pass

        return saved
