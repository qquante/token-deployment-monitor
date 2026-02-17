"""
Notification formatter for Bankr deployments.

Formats deployment alerts with FROM/TO relationships.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime


def format_deployment_notification(
    deployment: Dict[str, Any],
    from_user: Dict[str, Any],
    to_users: List[Dict[str, Any]],
    token_info: Dict[str, Any],
    original_tweet: Optional[Dict[str, Any]] = None
) -> str:
    """
    Format a deployment notification message.
    
    Args:
        deployment: Deployment info (token name, contract, etc)
        from_user: The requester/deployer info
        to_users: List of mentioned users (fee recipients, references)
        token_info: Token name, symbol
        original_tweet: The original request tweet (if available)
    
    Returns:
        Formatted notification string
    """
    lines = []
    
    # Header
    token_name = token_info.get("name", "Unknown")
    token_symbol = token_info.get("symbol", "???")
    
    lines.append(f"🟢 NEW PRE BANKR  [{token_symbol}]")
    lines.append("")
    lines.append(f" {token_name} ({token_symbol}) ")
    lines.append("")
    
    # FROM Section
    lines.append("━━━━━━━💼 FROM━━━━━━━")
    lines.append("")
    lines.append(format_user_section(from_user))
    
    # Original tweet text if available
    if original_tweet:
        tweet_text = original_tweet.get("text", "")
        if tweet_text:
            lines.append("")
            lines.append(f"📖 Tweet: {tweet_text[:200]}{'...' if len(tweet_text) > 200 else ''}")
    
    lines.append("")
    
    # TO Section (if there are recipients)
    if to_users:
        lines.append("━━━━━━━📬 TO━━━━━━━━")
        lines.append("")
        for user in to_users:
            lines.append(format_user_section(user))
            lines.append("")
    
    # Footer with links
    contract = deployment.get("contract_address", "")
    tweet_url = deployment.get("tweet_url", "")
    from_url = from_user.get("url", "")
    
    lines.append(f"{token_name} ({token_symbol}) - BANKR")
    lines.append("")
    
    # Links
    link_parts = []
    if tweet_url:
        link_parts.append(f"[Tweet]({tweet_url})")
    if from_url:
        link_parts.append(f"[From]({from_url})")
    if to_users and to_users[0].get("url"):
        link_parts.append(f"[To]({to_users[0].get('url')})")
    
    if link_parts:
        lines.append(" ".join(link_parts))
    
    return "\n".join(lines)


def format_user_section(user: Dict[str, Any]) -> str:
    """Format a user info section."""
    lines = []
    
    username = user.get("username", user.get("userName", "Unknown"))
    name = user.get("name", username)
    user_id = user.get("id", user.get("user_id", ""))
    followers = user.get("followers", 0)
    is_verified = user.get("isBlueVerified", user.get("is_blue_verified", False))
    verified_type = user.get("verifiedType", user.get("verified_type", ""))
    created_at = user.get("createdAt", user.get("created_at", ""))
    bio = user.get("description", user.get("bio", ""))
    ens = user.get("ens", "")
    wallet = user.get("wallet", "")
    smart_followers = user.get("smart_followers", 0)
    
    # Verification badge: ✅ for blue verified, ☑️ for business/government
    badge = ""
    if is_verified:
        if verified_type == "Business":
            badge = " ☑️"  # Business/Government (blue/gray)
        else:
            badge = " ✅"  # Regular verified (green)
    
    # Twitter link
    twitter_url = f"https://x.com/{username}"
    user_url = f"https://x.com/i/user/{user_id}" if user_id else ""
    
    lines.append(f"👑 Twitter: @{username} ({twitter_url})   ({user_url}){badge}")
    
    if ens:
        lines.append(f"📗ENS: {ens}")
    else:
        lines.append("📗ENS:")
    
    if wallet:
        lines.append(f"📁Wallet: {wallet}")
    else:
        lines.append("📁Wallet:")
    
    lines.append(f"👤 Followers: {format_number(followers)} 💎")
    lines.append(f"⭐️Smart Followers: {smart_followers} 💎")
    
    if created_at:
        # Parse and format date
        try:
            if isinstance(created_at, str):
                # Try parsing Twitter date format
                dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y")
                formatted_date = dt.strftime("%d %b %Y")
            else:
                formatted_date = str(created_at)
        except:
            formatted_date = created_at
        lines.append(f"📅 Criado em: {formatted_date}")
    
    if bio:
        lines.append(f"📕 Bio: {bio[:150]}{'...' if len(bio) > 150 else ''}")
    
    return "\n".join(lines)


def format_number(n: int) -> str:
    """Format number with thousands separator."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def format_simple_alert(
    token_name: str,
    token_symbol: str,
    contract: str,
    requester: str,
    recipients: List[str],
    tweet_url: str,
    tweet_text: str,
    fee_recipient: Optional[str] = None,
    fee_wallet: Optional[str] = None,
    fee_status: Optional[str] = None,
    fee_percentage: Optional[int] = None
) -> str:
    """
    Format a simple deployment alert (without full user details).
    """
    lines = []
    
    lines.append(f"🟢 NEW BANKR DEPLOY [{token_symbol}]")
    lines.append("")
    lines.append(f"🪙 Token: {token_name} ({token_symbol})")
    lines.append(f"📄 Contract: {contract}")
    lines.append("")
    lines.append(f"💼 FROM: @{requester}")
    
    if recipients:
        lines.append(f"📬 TO: {', '.join(['@' + r for r in recipients])}")
    
    # Fee/Rewards routing (with wallet and percentage if available)
    if fee_recipient or fee_wallet:
        status_emoji = {"success": "✅", "failed": "❌", "requires_access": "🔒", "attempted": "⏳"}.get(fee_status, "❓")
        lines.append("")
        if fee_recipient:
            lines.append(f"💸 REWARDS: {status_emoji} @{fee_recipient}")
        else:
            lines.append(f"💸 REWARDS: {status_emoji}")
        if fee_wallet:
            lines.append(f"   Wallet: {fee_wallet}")
        if fee_percentage:
            lines.append(f"   Fee Share: {fee_percentage}%")
    
    lines.append("")
    lines.append(f"📖 Tweet: {tweet_text[:150]}...")
    lines.append("")
    lines.append(f"🔗 {tweet_url}")
    
    return "\n".join(lines)
