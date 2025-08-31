import os
import smtplib
from email.mime.text import MIMEText

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
NOTIFY_TO = os.getenv("NOTIFY_TO")


def notify(text: str, level: str = "info"):
    """
    Send a plain text email notification.
    Requires SMTP_* and NOTIFY_TO env vars.
    """
    if not (SMTP_USER and SMTP_PASS and NOTIFY_TO):
        return

    subject = f"[ETL {level.upper()}] WooCommerce Pipeline"
    msg = MIMEText(text)
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = NOTIFY_TO

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [NOTIFY_TO], msg.as_string())
    except Exception as e:
        print(f"Email notify failed: {e}")
