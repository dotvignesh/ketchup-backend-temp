"""Send invite emails via SMTP."""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import get_settings

logger = logging.getLogger(__name__)


def send_invite_email(
    to_email: str,
    group_name: str,
    inviter_name: str,
    group_id: str,
) -> bool:
    """Send an invitation email. Returns True on success, False otherwise."""
    settings = get_settings()

    if not settings.smtp_host or not settings.smtp_user:
        logger.warning("SMTP not configured; skipping invite email to %s", to_email)
        return False

    frontend_url = settings.frontend_url.rstrip("/")
    accept_url = f"{frontend_url}/invites/{group_id}?action=accept"
    decline_url = f"{frontend_url}/invites/{group_id}?action=decline"

    subject = f'🍅 {inviter_name} invited you to join "{group_name}" on Ketchup'

    text_body = (
        f"Hey!\n\n"
        f'{inviter_name} invited you to join the group "{group_name}" on Ketchup.\n\n'
        f"Accept the invite: {accept_url}\n"
        f"Decline the invite: {decline_url}\n\n"
        f"This invite expires in 24 hours. If you don't respond, "
        f"it will be automatically declined.\n\n"
        f"- The Ketchup Team"
    )

    html_body = f"""\
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                max-width: 560px; margin: 0 auto; padding: 32px 24px; color: #1a1a1a;">
        <div style="text-align: center; margin-bottom: 32px;">
            <span style="font-size: 32px;">🍅</span>
            <h1 style="font-size: 22px; color: #c92a2a; margin: 8px 0 0 0;">Ketchup</h1>
        </div>

        <p style="font-size: 16px; line-height: 1.6;">
            <strong>{inviter_name}</strong> invited you to join
            <strong>"{group_name}"</strong> on Ketchup.
        </p>
        <p style="font-size: 14px; color: #555; line-height: 1.6;">
            Ketchup helps groups coordinate plans and decisions.
        </p>

        <div style="text-align: center; margin: 32px 0;">
            <a href="{accept_url}"
               style="display: inline-block; background: #c92a2a; color: #fff;
                      padding: 12px 32px; border-radius: 6px; text-decoration: none;
                      font-weight: 600; font-size: 15px; margin-right: 12px;">
                Accept invite
            </a>
            <a href="{decline_url}"
               style="display: inline-block; background: #f1f3f5; color: #495057;
                      padding: 12px 32px; border-radius: 6px; text-decoration: none;
                      font-weight: 600; font-size: 15px;">
                Decline
            </a>
        </div>

        <p style="font-size: 13px; color: #868e96; text-align: center; margin-top: 32px;">
            This invite expires in <strong>24 hours</strong>. If you don't respond,
            it will be automatically declined.
        </p>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.smtp_from_email or settings.smtp_user
    msg["To"] = to_email
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.ehlo()
            if settings.smtp_port != 25:
                server.starttls()
                server.ehlo()
            server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(msg["From"], [to_email], msg.as_string())
        logger.info("Invite email sent to %s for group %s", to_email, group_name)
        return True
    except Exception:
        logger.exception("Failed to send invite email to %s", to_email)
        return False
