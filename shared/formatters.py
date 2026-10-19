"""
Message formatters for Telegram notifications.

Based on the existing src/async_clients/telegram.py message classes
but adapted for the microservices architecture.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime


def format_number(n: int) -> str:
    """Format number with K/M suffix."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def get_account_age(created_at: str) -> str:
    """Get account age from creation date."""
    if not created_at:
        return "?"
    
    try:
        # Twitter format: "Mon Jan 01 00:00:00 +0000 2020"
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
        age = datetime.now(dt.tzinfo) - dt
        
        years = age.days // 365
        months = (age.days % 365) // 30
        
        if years > 0:
            return f"{years}y"
        elif months > 0:
            return f"{months}mo"
        else:
            return f"{age.days}d"
    except:
        return "?"


def build_user_line(
    username: str,
    name: str = "",
    followers: int = 0,
    smart_followers: int = 0,
    prev_usernames: int = 0,
    contracts: int = 0,
    is_verified: bool = False,
    affiliate_label: str = "",
    role: str = "FROM",  # FROM, TO, FEE
) -> str:
    """
    Build a single user info line for Telegram.
    
    Format: [ROLE] @username (Name) | 10K | SF:5 | R:0 | C:0
    """
    parts = []
    
    # Role prefix
    if role == "FROM":
        parts.append("FROM")
    elif role == "TO":
        parts.append("TO")
    elif role == "FEE":
        parts.append("FEE")
    
    # Username with verification
    user_part = f"@{username}"
    if is_verified:
        user_part += " ✓"
    
    if name and name.lower() != username.lower():
        user_part += f" ({name})"
    
    parts.append(user_part)
    
    # Stats
    stats = []
    if followers:
        stats.append(format_number(followers))
    if smart_followers:
        stats.append(f"SF:{smart_followers}")
    if prev_usernames:
        stats.append(f"R:{prev_usernames}")
    if contracts:
        stats.append(f"C:{contracts}")
    
    if stats:
        parts.append(" | ".join(stats))
    
    # Affiliate label
    if affiliate_label:
        parts.append(f"[{affiliate_label}]")
    
    return " | ".join(parts)


def build_deployed_message(
    token_name: str,
    token_symbol: str,
    contract_address: str,
    tweet_url: str = "",
    from_user: Dict[str, Any] = None,
    to_users: List[Dict[str, Any]] = None,
    fee_percentage: str = "",
    fee_recipient: str = "",
    platform: str = "X",
) -> tuple:
    """
    Build DEPLOYED notification message.
    
    Returns (text, keyboard).
    """
    lines = []
    
    # Header
    if platform.lower() == "farcaster":
        lines.append(f"<b>[FARCASTER]</b> {token_name}")
    else:
        lines.append(f"<b>[BANKR]</b> {token_name}")
    
    lines.append(f"<code>${token_symbol}</code>")
    lines.append("")
    
    # Contract
    lines.append(f"<code>{contract_address}</code>")
    lines.append("")
    
    # FROM user
    if from_user:
        from_line = build_user_line(
            username=from_user.get("username", ""),
            name=from_user.get("name", ""),
            followers=from_user.get("followers", 0),
            smart_followers=from_user.get("smart_followers", 0),
            prev_usernames=from_user.get("prev_usernames", 0),
            contracts=from_user.get("contracts", 0),
            is_verified=from_user.get("is_verified", False),
            affiliate_label=from_user.get("affiliate_label", ""),
            role="FROM",
        )
        lines.append(from_line)
    
    # TO users
    if to_users:
        for to_user in to_users:
            to_line = build_user_line(
                username=to_user.get("username", ""),
                name=to_user.get("name", ""),
                followers=to_user.get("followers", 0),
                smart_followers=to_user.get("smart_followers", 0),
                prev_usernames=to_user.get("prev_usernames", 0),
                contracts=to_user.get("contracts", 0),
                is_verified=to_user.get("is_verified", False),
                affiliate_label=to_user.get("affiliate_label", ""),
                role="TO",
            )
            lines.append(to_line)
    
    # Fee info
    if fee_percentage or fee_recipient:
        lines.append("")
        if fee_percentage:
            lines.append(f"Fee: {fee_percentage}")
        if fee_recipient:
            lines.append(f"Fee to: @{fee_recipient}")
    
    text = "\n".join(lines)
    
    # Keyboard
    buttons = []
    
    # Row 1: Links
    row1 = []
    if tweet_url:
        row1.append({"text": "🔗 Tweet", "url": tweet_url})
    row1.append({"text": "📊 Basescan", "url": f"https://basescan.org/token/{contract_address}"})
    row1.append({"text": "📈 Dexscreener", "url": f"https://dexscreener.com/base/{contract_address}"})
    buttons.append(row1)
    
    keyboard = {"inline_keyboard": buttons}
    
    return text, keyboard


def build_prebankr_message(
    token_name: str = None,
    token_symbol: str = None,
    tweet_id: str = "",
    tweet_url: str = "",
    from_user: Dict[str, Any] = None,
    to_users: List[Dict[str, Any]] = None,
    confidence: int = 0,
    like_count: int = 0,
) -> tuple:
    """
    Build PRE-BANKR notification message.
    
    Returns (text, keyboard).
    
    Keyboard layout:
    ┌─────────┬─────────┐
    │  Tweet  │  From   │
    ├─────────┼─────────┤
    │  Like   │  BUY    │
    └─────────┴─────────┘
    """
    lines = []
    
    # Header
    lines.append("<b>[PRE-BANKR]</b>")
    
    # Token info if available
    if token_name or token_symbol:
        token_str = ""
        if token_name:
            token_str = token_name
        if token_symbol:
            token_str += f" (${token_symbol})" if token_str else f"${token_symbol}"
        lines.append(f"<b>{token_str}</b>")
    
    lines.append("")
    
    # FROM user
    from_username = ""
    if from_user:
        from_username = from_user.get("username", "")
        from_line = build_user_line(
            username=from_username,
            name=from_user.get("name", ""),
            followers=from_user.get("followers", 0),
            smart_followers=from_user.get("smart_followers", 0),
            prev_usernames=from_user.get("prev_usernames", 0),
            contracts=from_user.get("contracts", 0),
            is_verified=from_user.get("is_verified", False),
            affiliate_label=from_user.get("affiliate_label", ""),
            role="FROM",
        )
        lines.append(from_line)
    
    # TO users
    if to_users:
        for to_user in to_users:
            to_line = build_user_line(
                username=to_user.get("username", ""),
                name=to_user.get("name", ""),
                followers=to_user.get("followers", 0),
                smart_followers=to_user.get("smart_followers", 0),
                prev_usernames=to_user.get("prev_usernames", 0),
                contracts=to_user.get("contracts", 0),
                is_verified=to_user.get("is_verified", False),
                affiliate_label=to_user.get("affiliate_label", ""),
                role="TO",
            )
            lines.append(to_line)
    
    # Confidence
    if confidence > 0:
        lines.append("")
        lines.append(f"Confidence: {confidence}%")
    
    text = "\n".join(lines)
    
    # Keyboard: Tweet | From | Like | BUY (1 row x 4 buttons)
    tweet_link = tweet_url if tweet_url else f"https://twitter.com/i/status/{tweet_id}"
    like_text = f"❤️ {like_count}" if like_count > 0 else "❤️ Like"
    
    row = [
        {"text": "Tweet", "url": tweet_link},
    ]
    
    if from_username:
        row.append({"text": "From", "url": f"https://twitter.com/{from_username}"})
    
    row.append({"text": like_text, "callback_data": f"like:{tweet_id}"})
    row.append({"text": "💰 BUY", "callback_data": f"buy:{tweet_id}"})
    
    keyboard = {"inline_keyboard": [row]}
    
    return text, keyboard


def build_trade_notification(
    status: str,  # APPROVED, WAITING, EXECUTING, DONE, FAILED
    token_symbol: str,
    token_address: str = "",
    eth_amount: float = 0,
    block_offset: int = 0,
    fee_expected: float = 0,
    target_block: int = 0,
    current_block: int = 0,
    tx_hash: str = "",
    error: str = "",
) -> str:
    """Build trade notification message."""
    lines = []
    
    if status == "APPROVED":
        lines.append("<b>BUY APPROVED</b>")
        lines.append("")
        lines.append(f"Token: <b>{token_symbol}</b>")
        lines.append(f"ETH: <b>{eth_amount}</b>")
        lines.append(f"Bloco: <b>+{block_offset}</b> ({fee_expected:.1f}% fee)")
        lines.append("")
        lines.append("Aguardando token ir on-chain...")
    
    elif status == "WAITING":
        lines.append("<b>TOKEN DETECTADO!</b>")
        lines.append("")
        lines.append(f"Token: <b>{token_symbol}</b>")
        if token_address:
            lines.append(f"Contract: <code>{token_address}</code>")
        lines.append("")
        lines.append("<b>Compra agendada:</b>")
        lines.append(f"Bloco: <b>{target_block}</b> (+{block_offset})")
        lines.append(f"Fee: <b>{fee_expected:.1f}%</b>")
        lines.append(f"ETH: <b>{eth_amount}</b>")
        lines.append("")
        lines.append("Aguardando bloco...")
    
    elif status == "EXECUTING":
        lines.append("<b>EXECUTANDO COMPRA...</b>")
        lines.append("")
        lines.append(f"Token: <b>{token_symbol}</b>")
        lines.append(f"Bloco: <b>{current_block}</b>")
        lines.append(f"ETH: <b>{eth_amount}</b>")
        lines.append(f"Fee: <b>{fee_expected:.1f}%</b>")
    
    elif status == "DONE":
        is_simulated = tx_hash and "simulated" in tx_hash.lower()
        lines.append(f"<b>COMPRA {'SIMULADA' if is_simulated else 'EXECUTADA'}!</b>")
        lines.append("")
        lines.append(f"Token: <b>{token_symbol}</b>")
        lines.append(f"ETH: <b>{eth_amount}</b>")
        lines.append(f"Fee: <b>{fee_expected:.1f}%</b>")
        lines.append("")
        if tx_hash and not is_simulated:
            lines.append(f"TX: <code>{tx_hash}</code>")
        elif is_simulated:
            lines.append("<i>Swap real requer implementacao web3.</i>")
        if token_address:
            lines.append("")
            lines.append(f"Contract: <code>{token_address}</code>")
    
    elif status == "FAILED":
        lines.append("<b>COMPRA FALHOU</b>")
        lines.append("")
        lines.append(f"Token: <b>{token_symbol}</b>")
        if error:
            lines.append(f"Erro: {error[:100]}")
    
    return "\n".join(lines)
