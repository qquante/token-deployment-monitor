"""
Telegram bot client for sending notifications.

Sends deployment alerts to a Telegram channel with clickable hyperlinks.

Usage:
    client = TelegramClient()
    client.send_message("Hello *bold* [link](https://example.com)")
    
    # Or use module-level function
    send_notification("New deployment!")
"""

import os
import logging
from typing import Optional, Dict, Any

import requests

logger = logging.getLogger(__name__)


class TelegramClient:
    """
    Telegram bot client for sending messages.
    
    Uses Telegram Bot API to send formatted messages to a channel.
    Supports Markdown formatting for clickable links.
    
    Attributes:
        bot_token: Telegram bot token from @BotFather
        channel_id: Target channel ID (negative for private channels)
    """
    
    BASE_URL = "https://api.telegram.org/bot"
    
    def __init__(
        self,
        bot_token: Optional[str] = None,
        channel_id: Optional[str] = None
    ):
        """
        Initialize Telegram client.
        
        Args:
            bot_token: Bot token (or set TELEGRAM_BOT_TOKEN env var)
            channel_id: Channel ID (or set TELEGRAM_CHANNEL_ID env var)
        """
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.channel_id = channel_id or os.getenv("TELEGRAM_CHANNEL_ID")
        
        if not self.bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN not set - messages will not be sent")
        if not self.channel_id:
            logger.warning("TELEGRAM_CHANNEL_ID not set - messages will not be sent")
        
        self._session = requests.Session()
    
    @property
    def api_url(self) -> str:
        """Get the API base URL with token."""
        return f"{self.BASE_URL}{self.bot_token}"
    
    def send_message(
        self,
        text: str,
        parse_mode: str = "Markdown",
        disable_web_page_preview: bool = True,
        disable_notification: bool = False
    ) -> bool:
        """
        Send a message to the configured channel.
        
        Args:
            text: Message text (supports Markdown/HTML)
            parse_mode: "Markdown" or "HTML"
            disable_web_page_preview: Don't show link previews
            disable_notification: Send silently
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.bot_token or not self.channel_id:
            logger.warning("Telegram not configured - skipping message")
            return False
        
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": self.channel_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
            "disable_notification": disable_notification,
        }
        
        try:
            response = self._session.post(url, json=payload, timeout=10)
            result = response.json()
            
            if result.get("ok"):
                logger.info(f"Telegram message sent to {self.channel_id}")
                return True
            else:
                logger.error(f"Telegram API error: {result.get('description', 'Unknown')}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
    
    def send_photo(
        self,
        photo_url: str,
        caption: str = "",
        parse_mode: str = "Markdown"
    ) -> bool:
        """
        Send a photo with caption.
        
        Args:
            photo_url: URL of the photo
            caption: Caption text (supports Markdown)
            parse_mode: "Markdown" or "HTML"
            
        Returns:
            True if sent successfully
        """
        if not self.bot_token or not self.channel_id:
            return False
        
        url = f"{self.api_url}/sendPhoto"
        payload = {
            "chat_id": self.channel_id,
            "photo": photo_url,
            "caption": caption,
            "parse_mode": parse_mode,
        }
        
        try:
            response = self._session.post(url, json=payload, timeout=10)
            return response.json().get("ok", False)
        except Exception as e:
            logger.error(f"Failed to send photo: {e}")
            return False


# =============================================================================
# Module-level convenience functions
# =============================================================================

_client: Optional[TelegramClient] = None


def _get_client() -> TelegramClient:
    """Get or create singleton client."""
    global _client
    if _client is None:
        _client = TelegramClient()
    return _client


def send_notification(text: str, parse_mode: str = "Markdown") -> bool:
    """
    Send a notification to Telegram.
    
    Args:
        text: Message text with Markdown formatting
        parse_mode: "Markdown" or "HTML"
        
    Returns:
        True if sent successfully
    """
    return _get_client().send_message(text, parse_mode)


def send_deployment_alert(
    token_name: str,
    token_symbol: str,
    contract: str,
    from_user: Dict[str, Any],
    to_users: list,
    tweet_url: str,
    tweet_text: str,
    fee_recipient: Optional[str] = None,
    fee_wallet: Optional[str] = None
) -> bool:
    """
    Send a formatted deployment alert.
    
    Args:
        token_name: Token name
        token_symbol: Token symbol
        contract: Contract address
        from_user: Enriched FROM user dict
        to_users: List of enriched TO user dicts
        tweet_url: URL to the tweet
        tweet_text: Original tweet text
        fee_recipient: Fee recipient username (if any)
        fee_wallet: Fee recipient wallet (if any)
        
    Returns:
        True if sent successfully
    """
    message = format_telegram_deployment(
        token_name=token_name,
        token_symbol=token_symbol,
        contract=contract,
        from_user=from_user,
        to_users=to_users,
        tweet_url=tweet_url,
        tweet_text=tweet_text,
        fee_recipient=fee_recipient,
        fee_wallet=fee_wallet
    )
    
    return send_notification(message)


def format_telegram_deployment(
    token_name: str,
    token_symbol: str,
    contract: str,
    from_user: Dict[str, Any],
    to_users: list,
    tweet_url: str,
    tweet_text: str,
    fee_recipient: Optional[str] = None,
    fee_wallet: Optional[str] = None
) -> str:
    """
    Format a deployment alert for Telegram with clickable links.
    
    Uses Markdown format for hyperlinks: [text](url)
    """
    lines = []
    
    # Header
    lines.append(f"🟢 *NEW BANKR DEPLOY* \\[{_escape_md(token_symbol)}\\]")
    lines.append("")
    lines.append(f"🪙 *{_escape_md(token_name)}* ({_escape_md(token_symbol)})")
    lines.append(f"📄 Contract: `{contract}`")
    
    # FROM Section
    lines.append("")
    lines.append("━━━━━━━💼 *FROM* ━━━━━━━")
    lines.append("")
    lines.append(_format_user_telegram(from_user))
    
    # Tweet text
    lines.append("")
    tweet_preview = tweet_text[:200] + "..." if len(tweet_text) > 200 else tweet_text
    lines.append(f"📖 Tweet: {_escape_md(tweet_preview)}")
    
    # TO Section
    if to_users:
        lines.append("")
        lines.append("━━━━━━━📬 *TO* ━━━━━━━━")
        for to_user in to_users:
            lines.append("")
            role = to_user.get("role", "mentioned")
            role_emoji = "💰" if role == "fee_recipient" else "📝"
            lines.append(f"{role_emoji} *{role.upper()}*")
            lines.append(_format_user_telegram(to_user))
    
    # Fee routing info
    if fee_recipient or fee_wallet:
        lines.append("")
        lines.append(f"💸 *REWARDS:* [@{fee_recipient}](https://x.com/{fee_recipient})")
        if fee_wallet:
            lines.append(f"   Wallet: `{fee_wallet}`")
    
    # Footer with links
    lines.append("")
    lines.append(f"*{_escape_md(token_name)}* ({_escape_md(token_symbol)}) - BANKR")
    lines.append("")
    lines.append(f"🔗 [View Tweet]({tweet_url})")
    
    return "\n".join(lines)


def _format_user_telegram(user: Dict[str, Any]) -> str:
    """Format a user section for Telegram with clickable links."""
    lines = []
    
    username = user.get("username", "?")
    user_id = user.get("user_id", "")
    followers = user.get("followers", 0)
    is_verified = user.get("is_blue_verified", False)
    created_at = user.get("created_at", "")
    bio = user.get("bio", "")
    ens = user.get("ens", "")
    wallet = user.get("wallet", "")
    smart_followers_count = user.get("smart_followers", 0)
    smart_followers_list = user.get("smart_followers_list", [])
    
    # Badge
    badge = " ☑️" if is_verified else ""
    
    # Format followers
    if followers >= 1_000_000:
        followers_str = f"{followers/1_000_000:.2f}M"
    elif followers >= 1_000:
        followers_str = f"{followers/1_000:.2f}K"
    else:
        followers_str = str(followers)
    
    # Format date
    date_str = ""
    if created_at:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00").replace(".000000", ""))
            date_str = dt.strftime("%d %b %Y")
        except:
            date_str = created_at[:10] if len(created_at) >= 10 else created_at
    
    # Twitter link (clickable)
    twitter_url = f"https://x.com/{username}"
    user_url = f"https://x.com/i/user/{user_id}" if user_id else ""
    
    lines.append(f"👑 Twitter: [@{username}]({twitter_url}){badge}")
    if user_id:
        lines.append(f"   [View Profile by ID]({user_url})")
    
    lines.append(f"📗 ENS: {_escape_md(ens) if ens else '(none)'}")
    lines.append(f"📁 Wallet: `{wallet}`" if wallet else "📁 Wallet: (none)")
    lines.append(f"👤 Followers: {followers_str} 💎")
    
    # Smart followers with clickable links
    lines.append(f"⭐️ Smart Followers: {smart_followers_count} 💎")
    if smart_followers_list:
        for i, sf in enumerate(smart_followers_list, 1):
            sf_handle = sf.get('username', '?')
            sf_count = sf.get('followers_count', 0)
            
            # Format count
            if sf_count >= 1_000_000:
                sf_count_str = f"{sf_count/1_000_000:.1f}M"
            elif sf_count >= 1_000:
                sf_count_str = f"{sf_count/1_000:.1f}K"
            else:
                sf_count_str = str(sf_count)
            
            # Clickable link
            lines.append(f"   {i}. [@{sf_handle}](https://x.com/{sf_handle}) ({sf_count_str})")
    
    if date_str:
        lines.append(f"📅 Criado em: {date_str}")
    
    if bio:
        bio_preview = bio[:100] + "..." if len(bio) > 100 else bio
        lines.append(f"📕 Bio: {_escape_md(bio_preview)}")
    
    return "\n".join(lines)


def _escape_md(text: str) -> str:
    """Escape Markdown special characters."""
    if not text:
        return ""
    # Characters that need escaping in Markdown
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text
