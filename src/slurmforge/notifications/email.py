from __future__ import annotations

from email.message import EmailMessage
import subprocess


def build_email_message(
    *,
    sender: str,
    recipients: tuple[str, ...],
    subject: str,
    body: str,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = sender
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.set_content(body)
    return message


def send_email_summary(
    *,
    sender: str,
    recipients: tuple[str, ...],
    subject: str,
    body: str,
    sendmail: str,
) -> None:
    message = build_email_message(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=body,
    )
    subprocess.run([sendmail, "-t"], input=message.as_string(), text=True, check=True)
