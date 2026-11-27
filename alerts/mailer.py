"""Pluggable mail backends. 'file' is the default and always works; 'smtp' needs every env var set.

Credentials come only from the environment and are never stored, logged or written to the ledger.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
SMTP_VARS = ("ALERT_SMTP_HOST", "ALERT_SMTP_PORT", "ALERT_SMTP_USER", "ALERT_SMTP_PASSWORD", "ALERT_TO")


class FileMailer:
    name = "file"

    def __init__(self, outbox: Path | None = None, recipient: str = ""):
        self.outbox = Path(outbox or PACKAGE_DIR / "outbox")
        self.recipient = recipient or "(outbox file)"

    def send(self, msg: EmailMessage) -> str:
        self.outbox.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", msg["Subject"] or "message").strip("-")[:70]
        path = self.outbox / f"{dt.datetime.now():%Y%m%d-%H%M%S-%f}-{slug}.eml"
        path.write_bytes(bytes(msg))
        return str(path)

    def describe(self) -> str:
        return f"file -> {self.outbox}"


class SmtpMailer:
    name = "smtp"

    def __init__(self, host: str, port: int, user: str, password: str, to: str, sender: str | None = None):
        self.host, self.port, self.user, self._password = host, port, user, password
        self.recipient, self.sender = to, sender or user

    def send(self, msg: EmailMessage) -> str:
        if self.port == 465:
            with smtplib.SMTP_SSL(self.host, self.port, context=ssl.create_default_context(), timeout=20) as s:
                s.login(self.user, self._password)
                s.send_message(msg)
        else:
            with smtplib.SMTP(self.host, self.port, timeout=20) as s:
                s.starttls(context=ssl.create_default_context())
                s.login(self.user, self._password)
                s.send_message(msg)
        return f"{self.host}:{self.port}"

    def describe(self) -> str:
        return f"smtp {self.host}:{self.port} as {self.user}"

    def __repr__(self) -> str:                # never expose the password
        return f"SmtpMailer({self.host}:{self.port}, user={self.user}, to={self.recipient})"


def select_backend(env=None, outbox: Path | None = None):
    """SmtpMailer only if ALL of ALERT_SMTP_{HOST,PORT,USER,PASSWORD} and ALERT_TO are set (and the
    port is a number); otherwise the file backend."""
    env = os.environ if env is None else env
    if all(env.get(k) for k in SMTP_VARS):
        try:
            port = int(env["ALERT_SMTP_PORT"])
        except ValueError:
            return FileMailer(outbox)
        return SmtpMailer(env["ALERT_SMTP_HOST"], port, env["ALERT_SMTP_USER"], env["ALERT_SMTP_PASSWORD"],
                          env["ALERT_TO"], env.get("ALERT_FROM") or None)
    return FileMailer(outbox)


def build_message(subject: str, body: str, to: str, sender: str) -> EmailMessage:
    m = EmailMessage()
    m["Subject"], m["To"], m["From"] = subject, to or "pipeline-agent@localhost", sender
    m["Date"] = dt.datetime.now().astimezone().strftime("%a, %d %b %Y %H:%M:%S %z")
    m.set_content(body)
    return m
