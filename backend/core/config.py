"""
从环境变量读取所有配置（替代原 config.json）
敏感信息（邮箱密码、数据库密码）均不入代码库
"""
import os
from functools import lru_cache


@lru_cache(maxsize=1)
def get_email_config() -> dict:
    return {
        "address":     os.environ["EMAIL_ADDRESS"],
        "password":    os.environ["EMAIL_PASSWORD"],
        "imap_server": os.environ.get("IMAP_SERVER", "imap.exmail.qq.com"),
        "imap_port":   int(os.environ.get("IMAP_PORT", "993")),
        "smtp_server": os.environ.get("SMTP_SERVER", "smtp.exmail.qq.com"),
        "smtp_port":   int(os.environ.get("SMTP_PORT", "465")),
    }


def get_default_cc() -> list[str]:
    raw = os.environ.get("DEFAULT_CC", "")
    return [a.strip() for a in raw.split(",") if a.strip()]


def get_forward_body() -> str:
    return os.environ.get(
        "FORWARD_BODY",
        "Hi,\n\nPlease find attached the final B/L and Form E for your reference."
        "\n\nThanks and regards\nMatthew H.\nFullway"
    )


def get_mysql_config() -> dict:
    return {
        "host":     os.environ["MYSQL_HOST"],
        "port":     int(os.environ["MYSQL_PORT"]),
        "user":     os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
        "database": os.environ["MYSQL_DB"],
    }


def get_allowed_origin() -> str:
    return os.environ.get("ALLOWED_ORIGIN", "*")
