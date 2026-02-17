"""
Telegram bot module for sending notifications.

Sends formatted deployment alerts to a Telegram channel.
"""

from .client import (
    TelegramClient,
    send_notification,
    send_deployment_alert,
)

__all__ = [
    "TelegramClient",
    "send_notification",
    "send_deployment_alert",
]
