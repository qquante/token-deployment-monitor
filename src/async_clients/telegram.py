"""
Async Telegram client with Progressive Message support.

Features:
- Async message sending/editing via aiohttp
- ProgressiveMessage class for coordinated updates
- Lock-based concurrency control for safe edits
"""

import os
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


class AsyncTelegramClient:
    """
    Async Telegram bot client.
    
    Supports:
    - Sending messages
    - Editing messages (for progressive updates)
    - Markdown formatting
    
    Example:
        async with AsyncTelegramClient() as client:
            # Send and get message_id
            msg_id = await client.send_message("Initial message")
            
            # Edit the message
            await client.edit_message(msg_id, "Updated message")
    """
    
    BASE_URL = "https://api.telegram.org/bot"
    
    def __init__(
        self,
        bot_token: Optional[str] = None,
        channel_id: Optional[str] = None
    ):
        """
        Initialize async Telegram client.
        
        Args:
            bot_token: Bot token from @BotFather
            channel_id: Target channel ID
        """
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.channel_id = channel_id or os.getenv("TELEGRAM_CHANNEL_ID", "")
        self._session: Optional[aiohttp.ClientSession] = None
        
        if not self.bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN not set")
        if not self.channel_id:
            logger.warning("TELEGRAM_CHANNEL_ID not set")
    
    async def __aenter__(self):
        """Create session on context enter."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close session on context exit."""
        await self.close()
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            self._session = aiohttp.ClientSession(timeout=timeout)
    
    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    @property
    def api_url(self) -> str:
        """Get the API base URL with token."""
        return f"{self.BASE_URL}{self.bot_token}"
    
    async def send_message(
        self,
        text: str,
        parse_mode: str = None,
        disable_web_page_preview: bool = True,
        disable_notification: bool = False,
        reply_markup: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Send a message and return message_id for later editing.
        
        Args:
            text: Message text (plain text, Telegram auto-links URLs)
            parse_mode: Optional "Markdown" or "HTML" (None = plain text with auto-links)
            disable_web_page_preview: Don't show link previews
            disable_notification: Send silently
            reply_markup: Optional inline keyboard markup
            
        Returns:
            message_id if successful, None otherwise
        """
        if not self.bot_token or not self.channel_id:
            logger.warning("Telegram not configured - skipping message")
            return None
        
        await self._ensure_session()
        
        url = f"{self.api_url}/sendMessage"
        payload = {
            "chat_id": self.channel_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
            "disable_notification": disable_notification,
        }
        
        # Only add parse_mode if specified
        if parse_mode:
            payload["parse_mode"] = parse_mode
        
        # Add inline keyboard if provided
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            # Verbose: show what we're sending
            text_preview = text[:80].replace('\n', ' ') + "..." if len(text) > 80 else text.replace('\n', ' ')
            logger.info(f"[TG SEND] channel={self.channel_id} text={text_preview}")
            
            async with self._session.post(url, json=payload) as response:
                result = await response.json()
                
                if result.get("ok"):
                    message_id = result["result"]["message_id"]
                    logger.info(f"[TG OK] message_id={message_id} sent to {self.channel_id}")
                    return message_id
                else:
                    error_desc = result.get('description', 'Unknown')
                    logger.error(f"[TG ERROR] {error_desc} (channel={self.channel_id})")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error("[TG TIMEOUT] send_message timeout")
            return None
        except Exception as e:
            logger.error(f"[TG EXCEPTION] send_message: {e}")
            return None
    
    async def edit_message(
        self,
        message_id: int,
        text: str,
        parse_mode: str = None,
        disable_web_page_preview: bool = True,
        reply_markup: Optional[Dict] = None,
        max_retries: int = 3,
    ) -> bool:
        """
        Edit an existing message with rate limit retry.
        
        Args:
            message_id: ID of message to edit
            text: New message text
            parse_mode: Optional "Markdown" or "HTML" (None = plain text)
            disable_web_page_preview: Don't show link previews
            reply_markup: Optional inline keyboard markup
            max_retries: Max retries on rate limit (default 3)
            
        Returns:
            True if successful
        """
        if not self.bot_token or not self.channel_id:
            return False
        
        await self._ensure_session()
        
        url = f"{self.api_url}/editMessageText"
        payload = {
            "chat_id": self.channel_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
        }
        
        # Only add parse_mode if specified
        if parse_mode:
            payload["parse_mode"] = parse_mode
        
        # Add inline keyboard if provided
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        # Retry loop for rate limiting
        for attempt in range(max_retries + 1):
            try:
                async with self._session.post(url, json=payload) as response:
                    result = await response.json()
                    
                    if result.get("ok"):
                        logger.info(f"[TG EDIT OK] message_id={message_id} edited")
                        return True
                    else:
                        desc = result.get("description", "")
                        
                        # Handle rate limiting with retry
                        if "Too Many Requests" in desc:
                            # Extract retry_after from error
                            params = result.get("parameters", {})
                            retry_after = params.get("retry_after", 5)
                            
                            if attempt < max_retries:
                                # Wait less than retry_after to be faster, but still back off
                                wait_time = min(retry_after, 2 + attempt * 2)
                                logger.debug(f"[TG RATE LIMITED] msg={message_id}, retry in {wait_time}s ({attempt+1}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue
                            else:
                                logger.warning(f"[TG RATE LIMITED] msg={message_id}, max retries exceeded")
                                return False
                        
                        # Don't log "message not modified" as error
                        if "not modified" not in desc.lower():
                            logger.warning(f"[TG EDIT WARN] {desc} (msg={message_id})")
                        return False
                        
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    logger.debug(f"[TG EDIT TIMEOUT] msg={message_id}, retry {attempt+1}/{max_retries}")
                    await asyncio.sleep(1)
                    continue
                logger.error(f"[TG EDIT TIMEOUT] message_id={message_id}")
                return False
            except Exception as e:
                logger.error(f"[TG EDIT ERROR] message_id={message_id}: {e}")
                return False
        
        return False
    
    async def answer_callback_query(
        self,
        callback_query_id: str,
        text: Optional[str] = None,
        show_alert: bool = False
    ) -> bool:
        """
        Answer a callback query (when user clicks inline button).
        
        Args:
            callback_query_id: ID of the callback query
            text: Optional text to show as notification
            show_alert: Show as alert popup instead of toast
            
        Returns:
            True if successful
        """
        if not self.bot_token:
            return False
        
        await self._ensure_session()
        
        url = f"{self.api_url}/answerCallbackQuery"
        payload = {
            "callback_query_id": callback_query_id,
            "show_alert": show_alert,
        }
        
        if text:
            payload["text"] = text
        
        try:
            async with self._session.post(url, json=payload) as response:
                result = await response.json()
                return result.get("ok", False)
        except Exception as e:
            logger.error(f"Failed to answer callback query: {e}")
            return False
    
    async def get_updates(
        self,
        offset: Optional[int] = None,
        timeout: int = 30,
        allowed_updates: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get updates from Telegram (for polling mode).
        
        Args:
            offset: Identifier of first update to be returned
            timeout: Long polling timeout in seconds
            allowed_updates: List of update types to receive
            
        Returns:
            List of updates
        """
        if not self.bot_token:
            return []
        
        await self._ensure_session()
        
        url = f"{self.api_url}/getUpdates"
        payload = {"timeout": timeout}
        
        if offset:
            payload["offset"] = offset
        if allowed_updates:
            payload["allowed_updates"] = allowed_updates
        
        try:
            async with self._session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout + 10)) as response:
                result = await response.json()
                if result.get("ok"):
                    return result.get("result", [])
                return []
        except Exception as e:
            logger.error(f"Failed to get updates: {e}")
            return []


class ProgressiveMessage:
    """
    Manages progressive message updates with concurrency control.
    
    Sends initial message immediately, then updates as data arrives.
    Uses asyncio.Lock to prevent race conditions during edits.
    
    Example:
        pm = ProgressiveMessage(telegram_client, initial_data)
        await pm.send_initial()
        
        # As enrichment completes, update sections
        await pm.update_user("from", enriched_from_data)
        await pm.update_user("to_1", enriched_to_data)
        await pm.update_smart_followers("from", smart_followers)
    """
    
    def __init__(
        self,
        client: AsyncTelegramClient,
        token_name: str,
        token_symbol: str,
        contract: str,
        from_username: str,
        to_usernames: List[str],
        tweet_url: str,
        tweet_text: str,
        fee_recipient: Optional[str] = None,
        fee_wallet: Optional[str] = None,
        liked_by: Optional[List[str]] = None,
        original_tweet_url: Optional[str] = None,
        preloaded_from: Optional[Dict[str, Any]] = None,
        preloaded_to: Optional[List[Dict[str, Any]]] = None,
    ):
        """
        Initialize progressive message.
        
        Args:
            client: AsyncTelegramClient instance
            token_name: Token name
            token_symbol: Token ticker symbol
            contract: Contract address
            from_username: FROM user's username
            to_usernames: List of TO usernames
            tweet_url: URL to the @bankrbot tweet (deploy confirmation)
            tweet_text: Original tweet text
            fee_recipient: Fee recipient username (optional)
            fee_wallet: Fee recipient wallet (optional)
            liked_by: List of usernames who liked this request (optional)
            original_tweet_url: URL to the original PRE-BANKR request tweet (optional)
            preloaded_from: Pre-loaded enriched data for FROM user (from cache)
            preloaded_to: Pre-loaded enriched data for TO users (from cache)
        """
        self.client = client
        self.message_id: Optional[int] = None
        self._lock = asyncio.Lock()
        
        # Static data (doesn't change)
        self.token_name = token_name
        self.token_symbol = token_symbol
        self.contract = contract
        self.tweet_url = tweet_url  # @bankrbot deploy tweet
        self.original_tweet_url = original_tweet_url  # Original PRE-BANKR request
        self.tweet_text = tweet_text
        self.fee_recipient = fee_recipient
        self.fee_wallet = fee_wallet
        self.liked_by = liked_by or []  # Usernames who liked the PRE-BANKR request
        
        # Creator info (for on-chain data)
        self.creator_wallet = ""
        self.creator_holding = ""
        self.creator_balance = ""
        self.creator_txs = ""
        
        # Platform (X or farcaster)
        self.platform = "X"
        
        # Dynamic data (updated progressively)
        # If preloaded_from is provided, use it directly (no loading state)
        self.from_user = {
            "username": from_username,
            "loading": preloaded_from is None,  # False if cache exists
            "data": preloaded_from,  # Cached data or None
        }
        
        # If preloaded_to is provided, use cached data for matching indices
        self.to_users = []
        for i, u in enumerate(to_usernames):
            has_cache = preloaded_to and i < len(preloaded_to) and preloaded_to[i]
            self.to_users.append({
                "username": u,
                "loading": not has_cache,  # False if cache exists for this user
                "data": preloaded_to[i] if has_cache else None,
            })
        
        # Track what's been enriched
        self._enrichment_complete = False
    
    async def send_initial(self) -> Optional[int]:
        """
        Send the initial message with loading indicators.
        
        Returns:
            message_id if successful
        """
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        self.message_id = await self.client.send_message(text, parse_mode="HTML", reply_markup=reply_markup)
        return self.message_id
    
    async def update_from_user(self, user_data: Dict[str, Any]):
        """
        Update FROM user section with enriched data.
        
        Args:
            user_data: Enriched user data dict
        """
        async with self._lock:
            self.from_user["loading"] = False
            self.from_user["data"] = user_data
            await self._edit_message()
    
    async def update_to_user(self, index: int, user_data: Dict[str, Any]):
        """
        Update a TO user section with enriched data.
        
        Args:
            index: Index in to_users list
            user_data: Enriched user data dict
        """
        if index >= len(self.to_users):
            return
        
        async with self._lock:
            self.to_users[index]["loading"] = False
            self.to_users[index]["data"] = user_data
            await self._edit_message()
    
    async def update_smart_followers(self, target: str, smart_followers: List[Dict]):
        """
        Update smart followers for a user.
        
        Args:
            target: "from" or "to_0", "to_1", etc.
            smart_followers: List of smart follower dicts
        """
        async with self._lock:
            if target == "from":
                if self.from_user["data"]:
                    self.from_user["data"]["smart_followers"] = len(smart_followers)
                    self.from_user["data"]["smart_followers_list"] = smart_followers
            elif target.startswith("to_"):
                idx = int(target.split("_")[1])
                if idx < len(self.to_users) and self.to_users[idx]["data"]:
                    self.to_users[idx]["data"]["smart_followers"] = len(smart_followers)
                    self.to_users[idx]["data"]["smart_followers_list"] = smart_followers
            
            await self._edit_message()
    
    async def mark_complete(self):
        """Mark enrichment as complete and do final edit."""
        async with self._lock:
            self._enrichment_complete = True
            await self._edit_message()
    
    def set_platform(self, platform: str):
        """Set the platform (X or farcaster)."""
        self.platform = platform
    
    async def update_creator_info(
        self,
        wallet: str,
        holding: str = "",
        balance: str = "",
        txs: str = ""
    ):
        """Update creator wallet info."""
        async with self._lock:
            self.creator_wallet = wallet
            self.creator_holding = holding
            self.creator_balance = balance
            self.creator_txs = txs
            await self._edit_message()
    
    async def _edit_message(self):
        """Rebuild and edit the message."""
        if not self.message_id:
            return
        
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        await self.client.edit_message(self.message_id, text, parse_mode="HTML", reply_markup=reply_markup)
    
    def _build_message(self) -> str:
        """Build the full message with HTML formatting."""
        lines = []
        
        # Header: 🟠 DEPLOYED BANKR (SYMBOL)
        token_name_escaped = _escape_html(self.token_name)
        token_symbol_escaped = _escape_html(self.token_symbol)
        lines.append(f"🟠 <b>DEPLOYED BANKR ({token_symbol_escaped})</b>")
        lines.append(" ")
        
        # Token name line: TokenName ($SYMBOL) - same format as PRE BANKR
        lines.append(f"<code><b>{token_name_escaped}</b> (${token_symbol_escaped})</code>")
        lines.append("")
        
        # Likes: ❤️ QQuante, Yoda (show who liked)
        if self.liked_by:
            hearts = "❤️" * min(len(self.liked_by), 5)
            # Show usernames who liked
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
            lines.append("")
        
        # FROM Section
        lines.append("━━━━━━━💼 FROM━━━━━━━")
        lines.append("")
        lines.append(self._format_user_section_html(self.from_user))
        
        # Platform indicator (if farcaster)
        if self.platform.lower() == "farcaster":
            lines.append("")
            lines.append("🌐 Farcaster")
        
        # TO Section
        if self.to_users:
            lines.append("")
            lines.append("━━━━━━━📬 TO━━━━━━━━")
            lines.append("")  # Space after TO header
            for i, to_user in enumerate(self.to_users):
                if i > 0:
                    lines.append("")  # Space between TO users
                lines.append(self._format_user_section_html(to_user))
        
        # Contract
        lines.append("")
        lines.append(f"🔖 <b>Contract:</b> <code>{self.contract}</code>")
        
        # Creator info (if available)
        if self.creator_wallet:
            lines.append("")
            lines.append(f"👤 <b>Creator:</b> <code>{self.creator_wallet}</code>")
            if self.creator_holding:
                lines.append(f"           ├─ Holding → {self.creator_holding}")
            if self.creator_balance:
                lines.append(f"           ├─ Balance → {self.creator_balance}")
            if self.creator_txs:
                lines.append(f"           └─ Txs → {self.creator_txs}")
        
        # Footer
        lines.append("")
        lines.append(f"<b>{token_name_escaped} ({token_symbol_escaped}) - BANKR</b>")
        
        # FROM/TO wallets
        from_data = self.from_user.get("data") or {}
        from_wallet = from_data.get("wallet", "")
        if from_wallet:
            lines.append(f"💼 FROM: <code>{from_wallet}</code>")
        
        if self.to_users:
            to_data = self.to_users[0].get("data") or {}
            to_wallet = to_data.get("wallet", "")
            if to_wallet:
                lines.append(f"📬 TO: <code>{to_wallet}</code>")
        
        # Tweet links (with space before)
        tweet_links = []
        if self.original_tweet_url:
            tweet_links.append(f'<a href="{self.original_tweet_url}">Tweet</a>')
        if self.tweet_url:
            tweet_links.append(f'<a href="{self.tweet_url}">Tweet Bankr</a>')
        if tweet_links:
            lines.append("")
            lines.append(" | ".join(tweet_links))
        
        # Likes at footer too (hearts with usernames)
        if self.liked_by:
            lines.append("")
            hearts = "❤️" * min(len(self.liked_by), 5)
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
        
        return "\n".join(lines)
    
    def _build_reply_markup(self) -> Optional[Dict]:
        """Build inline keyboard buttons for trading bots."""
        if not self.contract:
            return None
        
        buttons = [
            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{self.contract}"},
            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{self.contract}"},
            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{self.contract}"},
            {"text": "Clanker", "url": f"https://www.clanker.world/clanker/{self.contract}"},
        ]
        
        return {"inline_keyboard": [buttons]}
    
    def _format_user_section(self, user_state: Dict) -> str:
        """Format a user section based on current state (plain text)."""
        username = user_state.get("username", "?")
        loading = user_state.get("loading", True)
        data = user_state.get("data")
        
        lines = []
        twitter_url = f"https://x.com/{username}"
        
        if loading:
            # Show loading state
            lines.append(f"👑 Twitter: @{username}")
            lines.append("⏳ Loading...")
        else:
            # Show full data
            lines.extend(self._format_enriched_user(data or {"username": username}, is_from_user=True))
        
        return "\n".join(lines)
    
    def _format_user_section_html(self, user_state: Dict) -> str:
        """Format a user section with HTML formatting."""
        username = user_state.get("username", "?")
        loading = user_state.get("loading", True)
        data = user_state.get("data")
        
        if loading:
            # Show loading state
            lines = []
            lines.append(f'👑 <b>Twitter:</b> @{_escape_html(username)}')
            lines.append("⏳ Loading...")
            return "\n".join(lines)
        else:
            # Show full data with HTML
            return self._format_enriched_user_html(data or {"username": username}, is_from_user=True)
    
    def _format_enriched_user(self, user: Dict[str, Any], is_from_user: bool = False) -> List[str]:
        """Format enriched user data for Telegram (HTML mode)."""
        lines = []
        
        username = user.get("username", "?")
        user_id = user.get("user_id", "")
        followers = int(user.get("followers", 0) or 0)
        is_verified = user.get("is_blue_verified", False)
        verified_type = user.get("verified_type", None)
        created_at = user.get("created_at", "")
        bio = user.get("bio", "")
        ens = user.get("ens", "")
        wallet = user.get("wallet", "")
        smart_followers_count = int(user.get("smart_followers", 0) or 0)
        
        # Badge: ✅ for blue verified, ☑️ for business/government
        if is_verified:
            if verified_type == "Business":
                badge = "☑️"  # Business/Government (blue/gray)
            else:
                badge = "✅"  # Regular verified (green)
        else:
            badge = ""
        
        # Format followers with dot separator (Brazilian format)
        if followers >= 1_000_000:
            followers_str = f"{followers/1_000_000:.2f}M"
        elif followers >= 1_000:
            followers_str = f"{followers:,.0f}".replace(",", ".")
        else:
            followers_str = str(followers)
        
        # Format date
        date_str = ""
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00").replace(".000000", ""))
                date_str = dt.strftime("%d %b %Y")
            except:
                date_str = created_at[:10] if len(created_at) >= 10 else created_at
        
        # Twitter links
        twitter_url = f"https://x.com/{username}"
        
        # User line with links - no user_id
        user_line = f"👑 Twitter: @{username} ({twitter_url})"
        if badge:
            user_line += badge
        lines.append(user_line)
        
        # ENS
        lines.append(f"📗 ENS: {ens if ens else ''}")
        
        # Wallet
        lines.append(f"📁 Wallet: {wallet if wallet else ''}")
        
        # Followers
        lines.append(f"👤 Followers: {followers_str} 💎")
        
        # Smart followers count with alert emoji
        # FROM: ≥30 triggers alert, TO: ≥15 triggers alert
        sf_threshold = 15 if is_from_user else 30
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if smart_followers_count >= sf_threshold else ""
        lines.append(f"⭐️ Smart Followers: {smart_followers_count}+{sf_emoji}")
        
        # Created date
        if date_str:
            lines.append(f"📅 Criado em: {date_str}")
        
        # Bio
        if bio:
            bio_preview = bio[:150] + "..." if len(bio) > 150 else bio
            lines.append(f"📕 Bio: {bio_preview}")
        
        return lines
    
    def _format_enriched_user_html(self, user: Dict[str, Any], is_from_user: bool = False) -> str:
        """Format enriched user data with HTML formatting (R:, C:, affiliate_label, MOLT Agent)."""
        lines = []
        
        username = user.get("username", "?")
        user_id = user.get("user_id", "")
        followers = int(user.get("followers", 0) or 0)
        is_verified = user.get("is_blue_verified", False)
        verified_type = user.get("verified_type", None)
        created_at = user.get("created_at", "")
        bio = user.get("bio", "")
        ens = user.get("ens", "")
        wallet = user.get("wallet", "")
        smart_followers_count = int(user.get("smart_followers", 0) or 0)
        
        # MOLT Agent check
        is_molt_agent = user.get("is_molt_agent", False)
        molt_verified = user.get("molt_verified", False)
        
        # R and C from AlphaGate
        r_count = int(user.get("prev_usernames_count", 0) or 0)
        c_count = int(user.get("contracts_count", 0) or 0)
        
        # Affiliate label from Twitter
        affiliate_label = user.get("affiliate_label", "")
        
        # MOLT KOL filter data
        passes_molt_filter = user.get("passes_molt_filter", False)
        vip_followers = user.get("vip_followers", [])
        
        # =================================================================
        # MOLT AGENT FILTER BANNER (if passes KOL filter)
        # =================================================================
        if is_molt_agent and passes_molt_filter:
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("   <b>MOLT AGENT FILTER</b> ✅")
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("")
        
        # =================================================================
        # MOLT AGENT BANNER (if applicable)
        # =================================================================
        if is_molt_agent:
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            if molt_verified:
                lines.append("   <b>MOLT AGENT VERIFIED</b> ✅")
            else:
                lines.append("   <b>MOLT AGENT</b>")
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            lines.append("")  # Empty line for spacing
        
        # Badge: ✅ for blue verified, ☑️ for business/government
        if is_verified:
            if verified_type == "Business":
                badge = " ☑️"  # Business/Government (blue/gray)
            else:
                badge = " ✅"  # Regular verified (green)
        else:
            badge = ""
        
        # Label (Xai, APPLE, X, Cua, etc.)
        label_str = f" {affiliate_label}" if affiliate_label else ""
        
        # Format followers with dot separator (Brazilian format)
        if followers >= 1_000_000:
            followers_str = f"{followers/1_000_000:.2f}M"
        elif followers >= 1_000:
            followers_str = f"{followers:,.0f}".replace(",", ".")
        else:
            followers_str = str(followers)
        
        # Format date
        date_str = ""
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00").replace(".000000", ""))
                date_str = dt.strftime("%d %b %Y")
            except:
                date_str = created_at[:10] if len(created_at) >= 10 else created_at
        
        # Twitter links with HTML
        twitter_url = f"https://x.com/{username}"
        user_url = f"https://x.com/i/user/{user_id}" if user_id else ""
        
        # User line with HTML links: badge ✅/☑️ after (R: X | C: Y), then label (no user_id)
        user_line = f'👑 <b>Twitter:</b> <a href="{twitter_url}">@{_escape_html(username)}</a>'
        user_line += f" (R: {r_count} | C: {c_count})"
        user_line += badge
        user_line += label_str
        lines.append(user_line)
        
        # ENS
        lines.append(f"📗 <b>ENS:</b> {_escape_html(ens) if ens else ''}")
        
        # Wallet
        if wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{wallet}</code>")
        else:
            lines.append(f"📁 <b>Wallet:</b>")
        
        # Followers
        lines.append(f"👤 <b>Followers:</b> {followers_str} 💎")
        
        # Smart Followers with alert emoji
        # FROM: ≥30 triggers alert, TO: ≥15 triggers alert
        sf_threshold = 15 if is_from_user else 30
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if smart_followers_count >= sf_threshold else ""
        lines.append(f"⭐️ <b>Smart Followers:</b> {smart_followers_count}+{sf_emoji}")
        
        # VIP followers (if any)
        if vip_followers:
            for vip in vip_followers[:5]:  # Limit to 5 VIPs
                lines.append(f"└ @{_escape_html(vip)} 🔑")
        
        # Created date
        if date_str:
            lines.append(f"📅 <b>Criado em:</b> {date_str}")
        
        # Bio
        if bio:
            bio_preview = _escape_html(bio[:150] + "..." if len(bio) > 150 else bio)
            lines.append(f"📕 <b>Bio:</b> {bio_preview}")
        
        return "\n".join(lines)


def _escape_md(text: str) -> str:
    """Escape Markdown special characters for Telegram."""
    if not text:
        return ""
    # Characters that need escaping in MarkdownV2
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!', '\\']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


# =============================================================================
# Module-level convenience
# =============================================================================

_client: Optional[AsyncTelegramClient] = None


async def get_client() -> AsyncTelegramClient:
    """Get or create singleton async client."""
    global _client
    if _client is None:
        _client = AsyncTelegramClient()
    await _client._ensure_session()
    return _client


async def send_notification(text: str) -> Optional[int]:
    """Send notification and return message_id."""
    client = await get_client()
    return await client.send_message(text)


# =============================================================================
# PRE-BANKR Message Class (HTML Formatting)
# =============================================================================

def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    if not text:
        return ""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class PreBankrMessage:
    """
    Progressive message for PRE-BANKR deployment requests.
    Uses HTML formatting for bold text and clickable links.
    Includes inline keyboard buttons for [Tweet] [From] [To] [Like] [BUY].
    
    Shows:
    - FROM: Who is requesting (requester)
    - TO: All fee recipients/tagged users with full details
    - Request text
    - Token info
    """
    
    def __init__(
        self,
        client: AsyncTelegramClient,
        from_username: str,
        to_usernames: List[str],
        tweet_url: str,
        tweet_text: str,
        token_name: Optional[str] = None,
        token_symbol: Optional[str] = None,
        tweet_id: Optional[str] = None,
    ):
        self.client = client
        self.message_id: Optional[int] = None
        self._lock = asyncio.Lock()
        
        # Static data
        self.tweet_url = tweet_url
        self.tweet_text = tweet_text
        self.token_name = token_name
        self.token_symbol = token_symbol
        self.tweet_id = tweet_id
        
        # FROM = requester
        self.from_user = {
            "username": from_username,
            "loading": True,
            "data": None,
        }
        
        # TO = list of fee recipients/tagged users
        self.to_users: List[Dict] = []
        for username in to_usernames:
            if username:
                self.to_users.append({
                    "username": username.lstrip("@"),
                    "loading": True,
                    "data": None,
                })
        
        self._enrichment_complete = False
    
    def _build_keyboard(self) -> Dict:
        """Build inline keyboard with [Tweet] [From] [Like] [BUY] in single row."""
        from_u = self.from_user.get("username", "")
        
        # Single row: Tweet | From | Like | BUY
        row = [
            {"text": "Tweet", "url": self.tweet_url},
            {"text": "From", "url": f"https://x.com/{from_u}"},
        ]
        
        if self.tweet_id:
            row.append({"text": "❤️ Like", "callback_data": f"like:{self.tweet_id}"})
            row.append({"text": "💰 BUY", "callback_data": f"buy:{self.tweet_id}"})
        
        return {"inline_keyboard": [row]}
    
    async def send_initial(self) -> Optional[int]:
        """Send initial message with HTML formatting and inline buttons."""
        text = self._build_message()
        keyboard = self._build_keyboard()
        self.message_id = await self.client.send_message(
            text, parse_mode="HTML", reply_markup=keyboard
        )
        return self.message_id
    
    async def update_from_user(self, user_data: Dict[str, Any]):
        """Update FROM user with enriched data."""
        async with self._lock:
            self.from_user["loading"] = False
            self.from_user["data"] = user_data
            await self._edit_message()
    
    async def update_to_user(self, index: int, user_data: Dict[str, Any]):
        """Update TO user at index with enriched data."""
        if index >= len(self.to_users):
            return
        async with self._lock:
            self.to_users[index]["loading"] = False
            self.to_users[index]["data"] = user_data
            await self._edit_message()
    
    async def update_smart_followers(self, target: str, smart_followers: List[Dict]):
        """Update smart followers count."""
        async with self._lock:
            if target == "from" and self.from_user.get("data"):
                self.from_user["data"]["smart_followers"] = len(smart_followers)
            elif target.startswith("to_"):
                try:
                    idx = int(target.split("_")[1])
                    if idx < len(self.to_users) and self.to_users[idx].get("data"):
                        self.to_users[idx]["data"]["smart_followers"] = len(smart_followers)
                except:
                    pass
            await self._edit_message()
    
    async def mark_complete(self):
        """Mark enrichment as complete."""
        async with self._lock:
            self._enrichment_complete = True
            await self._edit_message()
    
    async def _edit_message(self):
        """Rebuild and edit with HTML and keyboard."""
        if not self.message_id:
            return
        text = self._build_message()
        keyboard = self._build_keyboard()
        await self.client.edit_message(
            self.message_id, text, parse_mode="HTML", reply_markup=keyboard
        )
    
    def _build_message(self) -> str:
        """Build HTML message matching exact format."""
        lines = []
        
        # Header: 🟢 NEW PRE BANKR [SYMBOL]
        symbol_display = f"[{self.token_symbol}]" if self.token_symbol else ""
        lines.append(f"🟢 <b>NEW PRE BANKR</b>  {symbol_display}")
        lines.append("")
        
        # Token line: NAME (SYMBOL)
        if self.token_name:
            token_line = f"<code><b>{_escape_html(self.token_name)}</b>"
            if self.token_symbol:
                token_line += f" (${_escape_html(self.token_symbol)})"
            token_line += "</code>"
            lines.append(token_line)
            lines.append("")
        
        # FROM Section
        lines.append("━━━━━━━💼 <b>FROM</b>━━━━━━━")
        lines.append("")
        lines.append(self._format_user(self.from_user, is_from_user=True))
        
        # Tweet text
        lines.append("")
        tweet_clean = _escape_html(self.tweet_text)
        lines.append(f"📖 <b>Tweet:</b> {tweet_clean}")
        
        # TO Section (all TO users)
        if self.to_users:
            lines.append("")
            lines.append("━━━━━━━📬 <b>TO</b>━━━━━━━━")
            for to_user in self.to_users:
                lines.append("")
                lines.append(self._format_user(to_user, is_from_user=False))
        
        # Footer: TOKEN (SYMBOL) - BANKR
        lines.append("")
        if self.token_name:
            footer_token = f"{_escape_html(self.token_name)}"
            if self.token_symbol:
                footer_token += f" ({_escape_html(self.token_symbol)})"
            lines.append(f"<b>{footer_token} - BANKR</b>")
        
        return "\n".join(lines)
    
    def _format_user(self, user_state: Dict, is_from_user: bool = False) -> str:
        """Format user section with HTML."""
        username = user_state.get("username", "?")
        loading = user_state.get("loading", True)
        data = user_state.get("data")
        
        if loading:
            lines = []
            lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{username}">@{username}</a>')
            lines.append("📁 <b>Wallet:</b>")
            lines.append("👤 <b>Followers:</b> ⏳")
            lines.append("⭐️ <b>Smart Followers:</b> ⏳")
            return "\n".join(lines)
        
        if not data:
            return f'👑 <b>Twitter:</b> <a href="https://x.com/{username}">@{username}</a>'
        
        return self._format_enriched_user(data, is_from_user)
    
    def _format_enriched_user(self, user: Dict[str, Any], is_from_user: bool = False) -> str:
        """Format enriched user data with R, C, Label, and MOLT Agent indicator."""
        lines = []
        
        username = user.get("username", "?")
        followers = int(user.get("followers", 0) or 0)
        is_verified = user.get("is_blue_verified", False)
        verified_type = user.get("verified_type", None)
        created_at = user.get("created_at", "")
        bio = user.get("bio", "")
        wallet = user.get("wallet", "")
        smart_followers = int(user.get("smart_followers", 0) or 0)
        
        # MOLT Agent check
        is_molt_agent = user.get("is_molt_agent", False)
        molt_verified = user.get("molt_verified", False)
        
        # R and C from AlphaGate
        r_count = int(user.get("prev_usernames_count", 0) or 0)
        c_count = int(user.get("contracts_count", 0) or 0)
        
        # Affiliate label from Twitter
        affiliate_label = user.get("affiliate_label", "")
        
        # MOLT KOL filter data
        passes_molt_filter = user.get("passes_molt_filter", False)
        vip_followers = user.get("vip_followers", [])
        
        # =================================================================
        # MOLT AGENT FILTER BANNER (if passes KOL filter)
        # =================================================================
        if is_molt_agent and passes_molt_filter:
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("   <b>MOLT AGENT FILTER</b> ✅")
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("")
        
        # =================================================================
        # MOLT AGENT BANNER (if applicable)
        # =================================================================
        if is_molt_agent:
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            if molt_verified:
                lines.append("   <b>MOLT AGENT VERIFIED</b> ✅")
            else:
                lines.append("   <b>MOLT AGENT</b>")
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            lines.append("")  # Empty line for spacing
        
        # Badge: ✅ for blue verified, ☑️ for business/government
        if is_verified:
            if verified_type == "Business":
                badge = " ☑️"  # Business/Government (blue/gray)
            else:
                badge = " ✅"  # Regular verified (green)
        else:
            badge = ""
        
        # Label (Xai, APPLE, X, Cua, etc.)
        label_str = f" {affiliate_label}" if affiliate_label else ""
        
        # Format followers with dot separator (Brazilian format)
        if followers >= 1_000_000:
            f_str = f"{followers/1_000_000:.2f}M"
        elif followers >= 1_000:
            f_str = f"{followers:,.0f}".replace(",", ".")
        else:
            f_str = str(followers)
        
        # Format date: DD MMM YYYY
        date_str = ""
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00").replace(".000000", ""))
                date_str = dt.strftime("%d %b %Y")
            except:
                date_str = created_at[:10] if len(created_at) >= 10 else created_at
        
        # Twitter line: badge ✅/☑️ after (R: X | C: Y), then label
        twitter_url = f"https://x.com/{username}"
        
        twitter_line = f'👑 <b>Twitter:</b> <a href="{twitter_url}">@{username}</a>'
        twitter_line += f" (R: {r_count} | C: {c_count})"
        twitter_line += badge
        twitter_line += label_str
        lines.append(twitter_line)
        
        # Wallet (no ENS)
        lines.append(f"📁 <b>Wallet:</b> {_escape_html(wallet) if wallet else ''}")
        
        # Followers (no 💎)
        lines.append(f"👤 <b>Followers:</b> {f_str}")
        
        # Smart Followers with alert emoji
        # FROM: ≥30 triggers alert, TO: ≥15 triggers alert
        sf_threshold = 15 if is_from_user else 30
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if smart_followers >= sf_threshold else ""
        lines.append(f"⭐️ <b>Smart Followers:</b> {smart_followers}+{sf_emoji}")
        
        # VIP followers (if any)
        if vip_followers:
            for vip in vip_followers[:5]:  # Limit to 5 VIPs
                lines.append(f"└ @{_escape_html(vip)} 🔑")
        
        # Created date
        if date_str:
            lines.append(f"📅 <b>Criado em:</b> {date_str}")
        
        # Bio
        if bio:
            bio_clean = _escape_html(bio[:200])
            lines.append(f"📕 <b>Bio:</b> {bio_clean}")
        
        return "\n".join(lines)


# =============================================================================
# ON-CHAIN FIRST Message Class
# =============================================================================

class OnchainDeploymentMessage:
    """
    Progressive message for on-chain first flow.
    
    Flow:
    1. Send initial message with on-chain data (contract, creator)
    2. Update with Twitter data when @bankrbot tweet arrives
    3. Link likes from PRE-BANKR
    
    This provides faster notifications since on-chain events
    are detected before Twitter confirmations.
    """
    
    def __init__(
        self,
        client: AsyncTelegramClient,
        token_name: str,
        token_symbol: str,
        contract: str,
        creator_wallet: str,
        platform: str = "X",
        tweet_url: str = "",
        tweet_id: str = "",
        liked_by: Optional[List[str]] = None,
    ):
        """
        Initialize on-chain deployment message.
        
        Args:
            client: Telegram client
            token_name: Token name
            token_symbol: Token symbol
            contract: Contract address
            creator_wallet: Creator/admin wallet address
            platform: Platform (X, farcaster, etc.)
            tweet_url: URL to original tweet request
            tweet_id: Tweet ID for Like/Buy buttons
            liked_by: List of usernames who liked the PRE-BANKR
        """
        self.client = client
        self.token_name = token_name
        self.token_symbol = token_symbol
        self.contract = contract
        self.creator_wallet = creator_wallet
        self.platform = platform
        self.tweet_url = tweet_url
        self.tweet_id = tweet_id
        self.liked_by = liked_by or []
        
        # Message state
        self.message_id: Optional[int] = None
        self._lock = asyncio.Lock()
        
        # Debounce state - batch multiple updates into single edit
        self._pending_edit: Optional[asyncio.Task] = None
        self._debounce_delay = 0.3  # 300ms debounce
        
        # Creator info (fetched via RPC)
        self.creator_balance = ""
        self.creator_txs = ""
        self.creator_holding = ""
        
        # Twitter data (added later when tweet arrives)
        self.twitter_loaded = False
        self.from_user: Dict[str, Any] = {}
        self.to_users: List[Dict[str, Any]] = []
        self.bankr_tweet_url = ""
        
        # Thread lookup fallback flag
        self.maybe_bug = False
    
    async def send_initial(self) -> Optional[int]:
        """Send initial message with on-chain data."""
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        self.message_id = await self.client.send_message(text, parse_mode="HTML", reply_markup=reply_markup)
        
        if self.message_id:
            logger.info(f"[ONCHAIN MSG] Sent initial message: {self.message_id}")
        
        return self.message_id
    
    async def update_creator_info(
        self,
        balance: str = "",
        txs: str = "",
        holding: str = "",
    ):
        """Update creator wallet info."""
        async with self._lock:
            self.creator_balance = balance
            self.creator_txs = txs
            self.creator_holding = holding
            await self._edit_message()
    
    async def add_twitter_data(
        self,
        from_user: Dict[str, Any],
        to_users: List[Dict[str, Any]],
        bankr_tweet_url: str = "",
    ):
        """Add Twitter enrichment data when tweet arrives (legacy - single update)."""
        async with self._lock:
            self.from_user = from_user
            self.to_users = to_users
            self.bankr_tweet_url = bankr_tweet_url
            self.twitter_loaded = True
            await self._edit_message()
            logger.info(f"[ONCHAIN MSG] Added Twitter data to message {self.message_id}")
    
    async def update_from_user(self, enriched: Dict[str, Any]):
        """Progressive update: set FROM user data and edit message."""
        async with self._lock:
            self.from_user = enriched
            self.twitter_loaded = True
            await self._edit_message()
    
    async def update_to_user(self, index: int, enriched: Dict[str, Any]):
        """Progressive update: set TO user data at index and edit message."""
        async with self._lock:
            # Ensure list is big enough
            while len(self.to_users) <= index:
                self.to_users.append({})
            self.to_users[index] = enriched
            await self._edit_message()
    
    async def update_bankr_tweet_url(self, url: str):
        """Progressive update: set bankr tweet URL and edit message."""
        async with self._lock:
            self.bankr_tweet_url = url
            await self._edit_message()
    
    async def update_smart_followers(self, target: str, data: Dict[str, Any]):
        """Progressive update: update smart followers count for FROM or TO user."""
        async with self._lock:
            count = data.get("count", 0)
            if target == "from" and self.from_user:
                self.from_user["smart_followers"] = count
            elif target.startswith("to_"):
                try:
                    index = int(target.split("_")[1])
                    if index < len(self.to_users) and self.to_users[index]:
                        self.to_users[index]["smart_followers"] = count
                except (ValueError, IndexError):
                    pass
            await self._edit_message()
    
    async def _edit_message(self, immediate: bool = False):
        """
        Schedule a debounced edit of the message.
        
        Multiple rapid updates are batched into a single edit to avoid rate limiting.
        
        Args:
            immediate: If True, skip debouncing and edit immediately
        """
        if not self.message_id:
            return
        
        if immediate:
            # Cancel any pending edit and do it now
            if self._pending_edit:
                self._pending_edit.cancel()
                self._pending_edit = None
            await self._do_edit()
        else:
            # Debounce: cancel previous pending edit and schedule new one
            if self._pending_edit:
                self._pending_edit.cancel()
            
            async def debounced_edit():
                await asyncio.sleep(self._debounce_delay)
                self._pending_edit = None
                await self._do_edit()
            
            self._pending_edit = asyncio.create_task(debounced_edit())
    
    async def _do_edit(self):
        """Actually perform the edit."""
        if not self.message_id:
            return
        
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        await self.client.edit_message(self.message_id, text, parse_mode="HTML", reply_markup=reply_markup)
    
    async def flush_edit(self):
        """Force any pending edit to happen immediately."""
        if self._pending_edit:
            self._pending_edit.cancel()
            self._pending_edit = None
            await self._do_edit()
    
    def _build_message(self) -> str:
        """Build the message with current state."""
        lines = []
        
        # Header: 🟠 DEPLOYED BANKR (SYMBOL)
        token_name = _escape_html(self.token_name)
        token_symbol = _escape_html(self.token_symbol)
        lines.append(f"🟠 <b>DEPLOYED BANKR ({token_symbol})</b>")
        lines.append(" ")
        
        # Token name line: TokenName ($SYMBOL) - same format as PRE BANKR
        lines.append(f"<code><b>{token_name}</b> (${token_symbol})</code>")
        lines.append("")
        
        # Likes (show who liked)
        if self.liked_by:
            hearts = "❤️" * min(len(self.liked_by), 5)
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
            lines.append("")
        
        # FROM section
        if self.twitter_loaded and self.from_user:
            lines.append("━━━━━━━💼 FROM━━━━━━━")
            lines.append("")  # Space after FROM header
            lines.append(self._format_user_html(self.from_user, is_from_user=True))
        else:
            lines.append("━━━━━━━💼 FROM━━━━━━━")
            lines.append("")  # Space after FROM header
            lines.append("⏳ <i>Loading Twitter data...</i>")
        
        # Platform indicator
        if self.platform.lower() == "farcaster":
            lines.append("🌐 Farcaster")
        
        lines.append("")
        
        # TO section
        if self.twitter_loaded and self.to_users:
            lines.append("━━━━━━━📬 TO━━━━━━━━")
            lines.append("")  # Space after TO header
            for i, to_user in enumerate(self.to_users):
                if i > 0:
                    lines.append("")  # Space between TO users
                lines.append(self._format_user_html(to_user, is_from_user=False))
            lines.append("")
        
        # Contract
        lines.append(f"🔖 <b>Contract:</b> <code>{self.contract}</code>")
        lines.append("")
        
        # Creator info
        lines.append(f"👤 <b>Creator:</b> <code>{self.creator_wallet}</code>")
        if self.creator_holding:
            lines.append(f"           ├─ Holding → {self.creator_holding}")
        if self.creator_balance:
            lines.append(f"           ├─ Balance → {self.creator_balance}")
        if self.creator_txs:
            lines.append(f"           └─ Txs → {self.creator_txs}")
        
        lines.append("")
        
        # Footer
        lines.append(f"<b>{token_name} ({token_symbol}) - BANKR</b>")
        
        # Wallet addresses
        if self.twitter_loaded and self.from_user:
            from_wallet = self.from_user.get("wallet", "")
            if from_wallet:
                lines.append(f"💼 FROM: <code>{from_wallet}</code>")
            
            if self.to_users:
                to_wallet = self.to_users[0].get("wallet", "")
                if to_wallet:
                    lines.append(f"📬 TO: <code>{to_wallet}</code>")
        
        # Tweet links (with space before)
        tweet_links = []
        if self.tweet_url:
            tweet_links.append(f'<a href="{self.tweet_url}">Tweet</a>')
        if self.bankr_tweet_url:
            tweet_links.append(f'<a href="{self.bankr_tweet_url}">Tweet Bankr</a>')
        if tweet_links:
            lines.append("")
            lines.append(" | ".join(tweet_links))
        
        # Likes footer (show who liked)
        if self.liked_by:
            lines.append("")
            hearts = "❤️" * min(len(self.liked_by), 5)
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
        
        # Maybe Bug indicator (thread lookup fallback was used)
        if self.maybe_bug:
            lines.append("")
            lines.append("⚠️ <i>BANKR MAYBE BUG</i>")
        
        return "\n".join(lines)
    
    def _build_reply_markup(self) -> Optional[Dict]:
        """Build inline keyboard buttons for trading bots (NO Like/Buy - those are on PRE-BANKR only)."""
        if not self.contract:
            return None
        
        # Only trading bot links on BANKR messages
        buttons = [
            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{self.contract}"},
            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{self.contract}"},
            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{self.contract}"},
            {"text": "Clanker", "url": f"https://www.clanker.world/clanker/{self.contract}"},
        ]
        
        return {"inline_keyboard": [buttons]}
    
    def _format_user_html(self, user: Dict[str, Any], is_from_user: bool = False) -> str:
        """Format user data with HTML and MOLT Agent indicator."""
        lines = []
        
        username = user.get("username", "?")
        user_id = user.get("user_id", "")
        followers = int(user.get("followers", 0) or 0)
        is_verified = user.get("is_blue_verified", False)
        verified_type = user.get("verified_type", None)
        created_at = user.get("created_at", "")
        bio = user.get("bio", "")
        ens = user.get("ens", "")
        wallet = user.get("wallet", "")
        smart_followers = int(user.get("smart_followers", 0) or 0)
        
        # MOLT Agent check
        is_molt_agent = user.get("is_molt_agent", False)
        molt_verified = user.get("molt_verified", False)
        
        # R and C counts
        r_count = int(user.get("prev_usernames_count", 0) or 0)
        c_count = int(user.get("contracts_count", 0) or 0)
        
        # Affiliate label
        affiliate_label = user.get("affiliate_label", "")
        
        # MOLT KOL filter data
        passes_molt_filter = user.get("passes_molt_filter", False)
        vip_followers = user.get("vip_followers", [])
        
        # =================================================================
        # MOLT AGENT FILTER BANNER (if passes KOL filter)
        # =================================================================
        if is_molt_agent and passes_molt_filter:
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("   <b>MOLT AGENT FILTER</b> ✅")
            lines.append("🛑🛑🛑🛑🛑🛑")
            lines.append("")
        
        # =================================================================
        # MOLT AGENT BANNER (if applicable)
        # =================================================================
        if is_molt_agent:
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            if molt_verified:
                lines.append("   <b>MOLT AGENT VERIFIED</b> ✅")
            else:
                lines.append("   <b>MOLT AGENT</b>")
            lines.append("🟠🟠🟠🟠🟠🟠🟠🟠🟠")
            lines.append("")  # Empty line for spacing
        
        # Badge
        if is_verified:
            if verified_type == "Business":
                badge = " ☑️"
            else:
                badge = " ✅"
        else:
            badge = ""
        
        label_str = f" {affiliate_label}" if affiliate_label else ""
        
        # Format followers
        if followers >= 1_000_000:
            f_str = f"{followers/1_000_000:.2f}M"
        elif followers >= 1_000:
            f_str = f"{followers:,.0f}".replace(",", ".")
        else:
            f_str = str(followers)
        
        # Format date
        date_str = ""
        if created_at:
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00").replace(".000000", ""))
                date_str = dt.strftime("%d %b %Y")
            except:
                date_str = created_at[:10] if len(created_at) >= 10 else created_at
        
        # Twitter line - badge ✅/☑️ after (R: X | C: Y), then label (no user_id)
        twitter_url = f"https://x.com/{username}"
        user_line = f'👑 <b>Twitter:</b> <a href="{twitter_url}">@{_escape_html(username)}</a>'
        user_line += f" (R: {r_count} | C: {c_count})"
        user_line += badge
        user_line += label_str
        lines.append(user_line)
        
        # ENS
        lines.append(f"📗 <b>ENS:</b> {_escape_html(ens) if ens else ''}")
        
        # Wallet
        if wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{wallet}</code>")
        else:
            lines.append("📁 <b>Wallet:</b>")
        
        # Followers
        lines.append(f"👤 <b>Followers:</b> {f_str} 💎")
        
        # Smart Followers with alert emoji
        # FROM: ≥30 triggers alert, TO: ≥15 triggers alert
        sf_threshold = 15 if is_from_user else 30
        sf_emoji = " ⛔️⚠️⛔️⚠️⛔️⚠️" if smart_followers >= sf_threshold else ""
        lines.append(f"⭐️ <b>Smart Followers:</b> {smart_followers}+{sf_emoji}")
        
        # VIP followers (if any)
        if vip_followers:
            for vip in vip_followers[:5]:  # Limit to 5 VIPs
                lines.append(f"└ @{_escape_html(vip)} 🔑")
        
        # Date
        if date_str:
            lines.append(f"📅 <b>Criado em:</b> {date_str}")
        
        # Bio
        if bio:
            bio_clean = _escape_html(bio[:150] + "..." if len(bio) > 150 else bio)
            lines.append(f"📕 <b>Bio:</b> {bio_clean}")
        
        return "\n".join(lines)


# =============================================================================
# CLAWNCH DEPLOYMENT MESSAGE (moltbook, 4claw, moltx)
# =============================================================================

class ClawnchDeploymentMessage:
    """
    Message for Clawnch token deployments (moltbook, 4claw, moltx).
    
    Enhanced version with full agent/owner/token data from PRE-CLAWNCH cache.
    """
    
    def __init__(
        self,
        client: AsyncTelegramClient,
        token_name: str,
        token_symbol: str,
        contract: str,
        deployer: str,
        platform: str,          # moltbook, 4claw, moltx
        agent_name: str,        # context.id
        post_id: str,           # context.messageId
        description: str,       # metadata.description
        social_urls: List[Dict], # metadata.socialMediaUrls
        # Enriched data from PRE-CLAWNCH cache
        liked_by: Optional[List[str]] = None,
        token_wallet: str = "",
        x_handle: str = "",
        molt_agent: Optional[Dict[str, Any]] = None,
        twitter_data: Optional[Dict[str, Any]] = None,
        alphagate_data: Optional[Dict[str, Any]] = None,
    ):
        self.client = client
        self.token_name = token_name
        self.token_symbol = token_symbol
        self.contract = contract
        self.deployer = deployer
        self.platform = platform
        self.agent_name = agent_name
        self.post_id = post_id
        self.description = description
        self.social_urls = social_urls or []
        
        # Enriched data
        self.liked_by = liked_by or []
        self.token_wallet = token_wallet
        self.x_handle = x_handle
        self.molt_agent = molt_agent or {}
        self.twitter_data = twitter_data or {}
        self.alphagate_data = alphagate_data or {}
        
        self.message_id: Optional[int] = None
        self._lock = asyncio.Lock()
        
        # Deployer info (fetched via RPC)
        self.deployer_balance = ""
        self.deployer_txs = ""
    
    async def send_initial(self) -> Optional[int]:
        """Send initial message with on-chain data."""
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        self.message_id = await self.client.send_message(
            text, parse_mode="HTML", reply_markup=reply_markup
        )
        
        if self.message_id:
            logger.info(f"[CLAWNCH MSG] Sent initial message: {self.message_id}")
        
        return self.message_id
    
    async def update_deployer_info(self, balance: str = "", txs: str = ""):
        """Update deployer wallet info."""
        async with self._lock:
            self.deployer_balance = balance
            self.deployer_txs = txs
            await self._edit_message()
    
    async def _edit_message(self):
        """Edit the message with updated data."""
        if not self.message_id:
            return
        text = self._build_message()
        reply_markup = self._build_reply_markup()
        await self.client.edit_message(
            self.message_id, text, parse_mode="HTML", reply_markup=reply_markup
        )
    
    def _build_message(self) -> str:
        """Build the enhanced HTML message."""
        lines = []
        
        token_name = _escape_html(self.token_name)
        token_symbol = _escape_html(self.token_symbol)
        
        # Header
        lines.append(f"🦞 <b>DEPLOYED CLAWNCH</b>")
        lines.append("")
        
        # Likes section (if any)
        if self.liked_by:
            hearts = "❤️" * min(len(self.liked_by), 5)
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
            lines.append("")
        
        # Token name with platform
        platform_emoji = {"moltbook": "📖", "moltx": "🦞", "4claw": "🦀"}.get(self.platform.lower(), "🌐")
        lines.append(f"<code><b>{token_name}</b> (${token_symbol})</code> by [{platform_emoji} {self.platform.title()}]")
        lines.append("")
        
        # =========================================================================
        # AGENT SECTION
        # =========================================================================
        lines.append("━━━━━━━🤖 <b>AGENT</b>━━━━━━━")
        lines.append("")
        
        # Agent name with link
        agent_link = self._get_agent_profile_url()
        if agent_link:
            lines.append(f'🤖 <b>Agent:</b> <a href="{agent_link}">{_escape_html(self.agent_name)}</a>')
        else:
            lines.append(f"🤖 <b>Agent:</b> {_escape_html(self.agent_name)}")
        
        # Karma (from molt_agent cache)
        karma = self.molt_agent.get("karma", 0)
        lines.append(f"⭐️ <b>Karma:</b> {karma}")
        
        # Agent wallet (from molt_agent cache)
        agent_wallet = self.molt_agent.get("wallet", "")
        lines.append(f"📁 <b>Wallet:</b> <code>{agent_wallet}</code>" if agent_wallet else "📁 <b>Wallet:</b>")
        
        # Agent followers (from molt_agent cache)
        agent_followers = self.molt_agent.get("followers", 0)
        followers_alert = " ⛔️⚠️⛔️⚠️⛔️⚠️" if agent_followers >= 5 else ""
        lines.append(f"👥 <b>Moltbook/MoltX Followers:</b> {self._format_number(agent_followers)}{followers_alert}")
        
        # Other Social Profile (same as agent link)
        if agent_link:
            lines.append(f'🖥 <b>Other Social Profile:</b> <a href="{agent_link}">{_escape_html(self.agent_name)}</a>')
        lines.append("")
        
        # =========================================================================
        # OWNER SECTION (if we have x_handle)
        # =========================================================================
        if self.x_handle:
            lines.append("━━━━━━━👨🏻 <b>OWNER</b>━━━━━━━")
            lines.append("")
            
            # Twitter handle with R/C and verification
            r_count = self.alphagate_data.get("prev_usernames_count", 0)
            c_count = self.alphagate_data.get("contracts_count", 0)
            is_verified = self.twitter_data.get("is_blue_verified", False)
            affiliate_label = self.twitter_data.get("affiliate_label", "")
            
            verified_icon = ""
            if is_verified:
                verified_icon = " ✅"
            if affiliate_label:
                verified_icon += f" 🏢{affiliate_label}"
            
            lines.append(f'👑 <b>Twitter:</b> <a href="https://x.com/{self.x_handle}">@{_escape_html(self.x_handle)}</a> (R:{r_count} | C:{c_count}){verified_icon}')
            
            # Owner Wallet (placeholder - not available from current APIs)
            lines.append(f"📁 <b>Wallet:</b>")
            
            # Twitter followers
            twitter_followers = self.twitter_data.get("followers", 0)
            lines.append(f"👤 <b>Followers:</b> {self._format_number(twitter_followers)}")
            
            # Smart followers (from alphagate)
            sf_count = self.alphagate_data.get("smart_followers", 0)
            sf_alert = " ⛔️⚠️⛔️⚠️⛔️⚠️" if sf_count >= 15 else ""
            lines.append(f"⭐️ <b>Smart Followers:</b> {sf_count}{sf_alert}")
            
            # Twitter created at (if available)
            created_at = self.twitter_data.get("created_at", "")
            if created_at:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    lines.append(f"📅 <b>Criado em:</b> {dt.strftime('%d %b %Y')}")
                except:
                    pass
            
            # Bio (if available, truncate)
            bio = self.twitter_data.get("bio", "") or self.twitter_data.get("description", "")
            if bio:
                bio_clean = _escape_html(bio[:100] + "..." if len(bio) > 100 else bio)
                lines.append(f"📕 <b>Bio:</b> {bio_clean}")
            
            lines.append("")
        
        # =========================================================================
        # TOKEN SECTION
        # =========================================================================
        lines.append("━━━━━━━🪙 <b>TOKEN</b>━━━━━━━")
        lines.append("")
        
        lines.append(f"📛 <b>Name:</b> {token_name} (${token_symbol})")
        
        if self.token_wallet:
            lines.append(f"📁 <b>Wallet:</b> <code>{self.token_wallet}</code>")
        
        # Website from social_urls
        website_url = self._get_website_url()
        if website_url:
            lines.append(f'🌐 <b>Web:</b> <a href="{website_url}">{self._truncate_url(website_url)}</a>')
        
        # Image from social_urls
        image_url = self._get_image_url()
        if image_url:
            lines.append(f'🖼 <b>Image:</b> <a href="{image_url}">Link</a>')
        
        # Description - limit to 200 chars
        if self.description:
            desc_clean = _escape_html(self.description[:200] + "..." if len(self.description) > 200 else self.description)
            lines.append(f"📕 <b>Description:</b> {desc_clean}")
        lines.append("")
        
        # Contract
        lines.append(f"🔖 <b>Contract:</b> <code>{self.contract}</code>")
        lines.append("")
        
        lines.append(f"👤 <b>Deployer:</b> <code>{self.deployer}</code>")
        
        # Footer with likes
        if self.liked_by:
            lines.append("")
            hearts = "❤️" * min(len(self.liked_by), 5)
            usernames = ", ".join(self.liked_by[:5])
            if len(self.liked_by) > 5:
                usernames += f" +{len(self.liked_by) - 5}"
            lines.append(f"{hearts} {usernames}")
        
        # Footer ticker with platform
        lines.append("")
        platform_emoji = {"moltbook": "📖", "moltx": "🦞", "4claw": "🦀"}.get(self.platform.lower(), "🌐")
        lines.append(f"<code><b>{token_name} (${token_symbol})</b> by {platform_emoji} {self.platform.upper()}</code>")
        
        return "\n".join(lines)
    
    def _build_reply_markup(self) -> Dict:
        """Build inline keyboard with trading bot links and info buttons."""
        # Row 1: Trading bots only (NO Like/Buy - those are in PRE-CLAWNCH)
        row1 = [
            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{self.contract}"},
            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{self.contract}"},
            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{self.contract}"},
        ]
        
        # Row 2: Agent Profile, Post, Owner
        row2 = []
        
        # Agent Profile link
        agent_url = self._get_agent_profile_url()
        if agent_url:
            row2.append({"text": "👤 Agent", "url": agent_url})
        
        # Post link
        post_url = self._get_post_url()
        if post_url:
            row2.append({"text": "📄 Post", "url": post_url})
        
        # Owner Twitter link
        if self.x_handle:
            row2.append({"text": "🐦 Owner", "url": f"https://x.com/{self.x_handle}"})
        
        keyboard = [row1]
        if row2:
            keyboard.append(row2)
        
        return {"inline_keyboard": keyboard}
    
    def _get_agent_profile_url(self) -> str:
        """Get agent profile URL based on platform."""
        platform_lower = self.platform.lower()
        if platform_lower == "moltbook":
            return f"https://www.moltbook.com/u/{self.agent_name}"
        elif platform_lower == "moltx":
            return f"https://moltx.io/{self.agent_name}"
        elif platform_lower == "4claw":
            return f"https://4claw.com/u/{self.agent_name}"
        return ""
    
    def _get_post_url(self) -> str:
        """Get post URL based on platform."""
        platform_lower = self.platform.lower()
        if platform_lower == "moltbook":
            return f"https://www.moltbook.com/m/clawnch/post/{self.post_id}"
        elif platform_lower == "moltx":
            return f"https://moltx.io/post/{self.post_id}"
        elif platform_lower == "4claw":
            # 4claw uses thread:uuid format
            thread_id = self.post_id.replace("thread:", "") if self.post_id.startswith("thread:") else self.post_id
            return f"https://4claw.com/thread/{thread_id}"
        return ""
    
    def _get_website_url(self) -> str:
        """Extract website URL from social_urls."""
        for url_obj in self.social_urls:
            platform = url_obj.get("platform", "").lower()
            url = url_obj.get("url", "")
            if platform in ["website", "web", "link"] and url:
                return url
        # Fallback: return first non-twitter URL
        for url_obj in self.social_urls:
            url = url_obj.get("url", "")
            if url and "twitter.com" not in url and "x.com" not in url:
                return url
        return ""
    
    def _get_image_url(self) -> str:
        """Extract image URL from social_urls or metadata."""
        for url_obj in self.social_urls:
            platform = url_obj.get("platform", "").lower()
            url = url_obj.get("url", "")
            if platform in ["image", "img", "logo"] and url:
                return url
        return ""
    
    def _truncate_url(self, url: str, max_len: int = 40) -> str:
        """Truncate URL for display."""
        if len(url) <= max_len:
            return url
        return url[:max_len] + "..."
    
    def _format_number(self, n: int) -> str:
        """Format number with K/M suffix."""
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f}M"
        elif n >= 1_000:
            return f"{n/1_000:.1f}K"
        return str(n)


# =============================================================================
# TRADING CONFIG MENU BUILDERS
# =============================================================================

# Fee schedule: block offset -> fee percentage
# Auction dura 2 blocos. Depois de expirar, fee decai em ~8 blocos.
FEE_SCHEDULE = {
    0: 66.7,   # Auction acabou de expirar
    1: 58.3,   # Primeiro bloco decay
    2: 50.0,   # 
    3: 41.7,   #
    4: 33.3,   #
    5: 25.0,   #
    6: 16.7,   #
    7: 8.3,    #
    8: 4.2,    # Fee minima
    9: 4.2,
    10: 4.2,
}


def get_fee_for_block(blocks_after_expire: int) -> float:
    """Get fee % for X blocks after auction expires."""
    if blocks_after_expire in FEE_SCHEDULE:
        return FEE_SCHEDULE[blocks_after_expire]
    elif blocks_after_expire >= 8:
        return 4.2
    else:
        return 66.7


def build_main_menu_text(config) -> str:
    """Build main trading config menu text."""
    wallet_status = f"0x...{config.wallet_address[-6:]}" if config.wallet_address else "NAO CONFIGURADA"
    fee = get_fee_for_block(config.block_offset)
    auto_status = "ON" if config.auto_buy else "OFF"
    
    return f"""<b>⚙️ TRADING BOT</b>

<b>Config:</b>
Wallet: <code>{wallet_status}</code>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Gwei: <b>{config.gwei}</b>
Auto-buy: <b>{auto_status}</b>

<b>Fee Schedule:</b>
<code>+0=66.7% +1=58.3% +2=50%
+3=41.7% +4=33.3% +5=25%
+6=16.7% +7=8.3%  +8=4.2%</code>

Clique para configurar:"""


def build_main_menu_keyboard() -> Dict:
    """Build main menu inline keyboard."""
    return {
        "inline_keyboard": [
            [
                {"text": "📁 Wallet", "callback_data": "cfg:wallet"},
                {"text": "💰 ETH", "callback_data": "cfg:eth"},
            ],
            [
                {"text": "📦 Bloco/Fee", "callback_data": "cfg:block"},
                {"text": "⛽ Gwei", "callback_data": "cfg:gwei"},
            ],
            [
                {"text": "🔄 Auto-buy ON/OFF", "callback_data": "cfg:autobuy"},
            ],
            [
                {"text": "📊 Ver Fees", "callback_data": "cfg:fees"},
                {"text": "📈 Status", "callback_data": "cfg:status"},
            ],
        ]
    }


def build_block_menu_text(config) -> str:
    """Build block selection menu text."""
    lines = ["<b>📦 SELECIONAR BLOCO</b>\n"]
    lines.append("Escolha quantos blocos esperar após auction expirar.\n")
    lines.append("<code>BLOCO    FEE</code>")
    
    for blk in range(0, 9):
        fee = FEE_SCHEDULE[blk]
        marker = " <--" if blk == config.block_offset else ""
        lines.append(f"<code>  +{blk}   {fee:>5.1f}%{marker}</code>")
    
    lines.append(f"\nSelecionado: <b>+{config.block_offset}</b> ({get_fee_for_block(config.block_offset):.1f}%)")
    return "\n".join(lines)


def build_block_menu_keyboard(config) -> Dict:
    """Build block selection keyboard with current selection marked."""
    buttons = []
    
    # Row 1: blocks 0-3
    row1 = []
    for blk in [0, 1, 2, 3]:
        marker = ">" if blk == config.block_offset else ""
        row1.append({"text": f"{marker}+{blk}", "callback_data": f"setblk:{blk}"})
    buttons.append(row1)
    
    # Row 2: blocks 4-7
    row2 = []
    for blk in [4, 5, 6, 7]:
        marker = ">" if blk == config.block_offset else ""
        row2.append({"text": f"{marker}+{blk}", "callback_data": f"setblk:{blk}"})
    buttons.append(row2)
    
    # Row 3: block 8 + back
    buttons.append([
        {"text": ">+8" if config.block_offset == 8 else "+8", "callback_data": "setblk:8"},
        {"text": "< Voltar", "callback_data": "cfg:back"},
    ])
    
    return {"inline_keyboard": buttons}


def build_eth_menu_text(config) -> str:
    """Build ETH amount selection menu text."""
    return f"""<b>💰 QUANTIDADE ETH</b>

Quanto ETH usar em cada compra.

Atual: <b>{config.eth_amount} ETH</b>

Selecione:"""


def build_eth_menu_keyboard(config) -> Dict:
    """Build ETH amount selection keyboard."""
    amounts = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]
    buttons = []
    row = []
    
    for eth in amounts:
        marker = ">" if eth == config.eth_amount else ""
        row.append({"text": f"{marker}{eth}", "callback_data": f"seteth:{eth}"})
        
        if len(row) == 4:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([{"text": "✏️ Custom...", "callback_data": "custometh"}])
    buttons.append([{"text": "< Voltar", "callback_data": "cfg:back"}])
    
    return {"inline_keyboard": buttons}


def build_gwei_menu_text(config) -> str:
    """Build gwei selection menu text."""
    return f"""<b>⛽ GAS PRICE (GWEI)</b>

Gas price para transações.

Atual: <b>{config.gwei} gwei</b>

Selecione:"""


def build_gwei_menu_keyboard(config) -> Dict:
    """Build gwei selection keyboard."""
    gweis = [0.1, 0.5, 1, 2, 5, 10, 50, 100, 500, 1000]
    buttons = []
    row = []
    
    for g in gweis:
        marker = ">" if g == config.gwei else ""
        row.append({"text": f"{marker}{g}", "callback_data": f"setgwei:{g}"})
        
        if len(row) == 5:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([{"text": "✏️ Custom...", "callback_data": "customgwei"}])
    buttons.append([{"text": "< Voltar", "callback_data": "cfg:back"}])
    
    return {"inline_keyboard": buttons}


def build_fees_text() -> str:
    """Build full fee schedule text."""
    lines = ["<b>📊 FEE SCHEDULE COMPLETO</b>\n"]
    lines.append("Quando token é criado:\n")
    lines.append("1. Auction ativa por 2 blocos (66.7%)")
    lines.append("2. Auction expira (bloco +0)")
    lines.append("3. Fee decai por ~8 blocos\n")
    lines.append("<code>BLOCO     FEE    RISCO</code>")
    
    risks = {
        0: "EXTREMO", 1: "MUITO ALTO", 2: "ALTO",
        3: "MEDIO-ALTO", 4: "MEDIO", 5: "MEDIO",
        6: "BAIXO", 7: "MUITO BAIXO", 8: "MINIMO"
    }
    
    for blk in range(0, 9):
        fee = FEE_SCHEDULE[blk]
        risk = risks.get(blk, "MINIMO")
        lines.append(f"<code>  +{blk}    {fee:>5.1f}%   {risk}</code>")
    
    lines.append("\nRecomendado: <b>+6 a +8</b> (fee baixa)")
    return "\n".join(lines)


def build_buy_confirm_text(config, token_symbol: str, tweet_id: str = "") -> str:
    """Build buy confirmation text."""
    fee = get_fee_for_block(config.block_offset)
    
    if not config.is_configured():
        missing = []
        if not config.wallet_address:
            missing.append("wallet")
        if not hasattr(config, 'wallet_private_key') or not config.wallet_private_key:
            missing.append("private_key")
        
        return f"""<b>⚠️ CONFIG INCOMPLETA</b>

Falta configurar: {', '.join(missing)}

Use /config para configurar antes de comprar."""
    
    return f"""<b>💰 CONFIRMAR COMPRA</b>

Token: <b>{_escape_html(token_symbol) or 'Novo Token'}</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Gwei: <b>{config.gwei}</b>

Confirmar?"""


def build_buy_confirm_keyboard(tweet_id: str, is_configured: bool = False) -> Dict:
    """Build buy confirmation keyboard."""
    if not is_configured:
        return {
            "inline_keyboard": [
                [{"text": "⚙️ Configurar", "callback_data": "cfg:main"}],
            ]
        }
    
    return {
        "inline_keyboard": [
            [
                {"text": "✅ CONFIRMAR", "callback_data": f"buyconfirm:{tweet_id}"},
                {"text": "❌ Cancelar", "callback_data": "buycancel"},
            ],
            [{"text": "⚙️ Mudar Config", "callback_data": "cfg:main"}],
        ]
    }


def build_status_text(config, executor_stats: Dict = None) -> str:
    """Build status display text."""
    wallet_status = f"0x...{config.wallet_address[-6:]}" if config.wallet_address else "NAO"
    fee = get_fee_for_block(config.block_offset)
    auto_status = "ON" if config.auto_buy else "OFF"
    
    lines = [
        "<b>📈 STATUS</b>",
        "",
        f"Wallet: <code>{wallet_status}</code>",
        f"Config OK: {'SIM' if config.is_configured() else 'NAO'}",
        f"ETH: {config.eth_amount}",
        f"Bloco: +{config.block_offset} ({fee:.1f}%)",
        f"Gwei: {config.gwei}",
        f"Auto-buy: {auto_status}",
    ]
    
    if executor_stats:
        lines.append("")
        lines.append(f"Pending: {executor_stats.get('total_pending', 0)}")
        lines.append(f"Block: {executor_stats.get('current_block', 0)}")
    
    return "\n".join(lines)


def build_wallet_config_text() -> str:
    """Build wallet configuration instructions text."""
    return """<b>📁 CONFIGURAR WALLET</b>

Envie sua wallet no formato:
<code>/setwallet 0x...</code>

Envie sua private key:
<code>/setkey sua_private_key</code>

<i>A private key é salva localmente.</i>"""


# =============================================================================
# TRADING CALLBACK HANDLER
# =============================================================================

class TradingCallbackHandler:
    """
    Handler for trading-related Telegram callbacks.
    
    Handles:
    - Like button (like:{tweet_id})
    - Buy button (buy:{tweet_id})
    - Buy confirm/cancel (buyconfirm:{tweet_id}, buycancel)
    - Config menus (cfg:main, cfg:block, cfg:eth, cfg:gwei, cfg:fees, cfg:autobuy, cfg:status, cfg:wallet)
    - Set values (setblk:{n}, seteth:{n}, setgwei:{n})
    """
    
    def __init__(
        self,
        telegram_client: AsyncTelegramClient,
        get_user_config_fn,
        save_user_config_fn,
        db=None,
        trading_executor=None,
        token_cache: Dict = None,
    ):
        """
        Initialize callback handler.
        
        Args:
            telegram_client: Telegram client for sending messages
            get_user_config_fn: Function to get user config (user_id, username) -> config
            save_user_config_fn: Function to save user config (user_id) -> None
            db: Database instance for likes
            trading_executor: Trading executor instance
            token_cache: Dict mapping tweet_id -> token info
        """
        self.client = telegram_client
        self.get_user_config = get_user_config_fn
        self.save_user_config = save_user_config_fn
        self.db = db
        self.executor = trading_executor
        self.token_cache = token_cache or {}
        self._logger = logging.getLogger(__name__)
    
    async def handle_callback(self, callback_query: Dict[str, Any]) -> bool:
        """
        Handle a callback query.
        
        Args:
            callback_query: Telegram callback query object
            
        Returns:
            True if handled, False if not recognized
        """
        callback_id = callback_query.get("id")
        data = callback_query.get("data", "")
        user = callback_query.get("from", {})
        user_id = user.get("id", 0)
        user_id_str = str(user_id)
        username = user.get("username", "")
        message = callback_query.get("message", {})
        message_id = message.get("message_id")
        chat_id = message.get("chat", {}).get("id", user_id)
        
        self._logger.info(f"[CALLBACK] @{username}: {data}")
        
        # LIKE
        if data.startswith("like:"):
            return await self._handle_like(callback_id, data, user_id_str, username, message_id)
        
        # BUY
        elif data.startswith("buy:"):
            return await self._handle_buy(callback_id, data, user_id, username)
        
        # BUY CONFIRM
        elif data.startswith("buyconfirm:"):
            return await self._handle_buy_confirm(callback_id, data, user_id, username, message_id)
        
        # BUY CANCEL
        elif data == "buycancel":
            await self.client.answer_callback_query(callback_id, text="Cancelado")
            await self.client.edit_message(message_id, "Cancelado", parse_mode="HTML")
            return True
        
        # CONFIG MAIN
        elif data in ["cfg:main", "cfg:back"]:
            return await self._handle_config_main(callback_id, user_id, username, message_id)
        
        # CONFIG BLOCK
        elif data == "cfg:block":
            return await self._handle_config_block(callback_id, user_id, username, message_id)
        
        # CONFIG ETH
        elif data == "cfg:eth":
            return await self._handle_config_eth(callback_id, user_id, username, message_id)
        
        # CONFIG GWEI
        elif data == "cfg:gwei":
            return await self._handle_config_gwei(callback_id, user_id, username, message_id)
        
        # CONFIG FEES
        elif data == "cfg:fees":
            await self.client.answer_callback_query(callback_id)
            await self.client.edit_message(
                message_id, build_fees_text(), parse_mode="HTML",
                reply_markup={"inline_keyboard": [[{"text": "< Voltar", "callback_data": "cfg:main"}]]}
            )
            return True
        
        # CONFIG AUTOBUY TOGGLE
        elif data == "cfg:autobuy":
            return await self._handle_config_autobuy(callback_id, user_id, username, message_id)
        
        # CONFIG STATUS
        elif data == "cfg:status":
            return await self._handle_config_status(callback_id, user_id, username, message_id)
        
        # CONFIG WALLET
        elif data == "cfg:wallet":
            await self.client.answer_callback_query(callback_id)
            await self.client.edit_message(
                message_id, build_wallet_config_text(), parse_mode="HTML",
                reply_markup={"inline_keyboard": [[{"text": "< Voltar", "callback_data": "cfg:main"}]]}
            )
            return True
        
        # SET BLOCK
        elif data.startswith("setblk:"):
            return await self._handle_set_block(callback_id, data, user_id, username, message_id)
        
        # SET ETH
        elif data.startswith("seteth:"):
            return await self._handle_set_eth(callback_id, data, user_id, username, message_id)
        
        # SET GWEI
        elif data.startswith("setgwei:"):
            return await self._handle_set_gwei(callback_id, data, user_id, username, message_id)
        
        # Unknown callback
        else:
            await self.client.answer_callback_query(callback_id)
            return False
    
    async def _handle_like(self, callback_id: str, data: str, user_id_str: str, username: str, message_id: int) -> bool:
        """Handle like button."""
        if not self.db:
            await self.client.answer_callback_query(callback_id, text="DB não disponível")
            return True
        
        tweet_id = data.split(":", 1)[1]
        
        if self.db.has_liked(tweet_id, user_id_str):
            self.db.remove_like(tweet_id, user_id_str)
            like_count = self.db.get_like_count(tweet_id)
            await self.client.answer_callback_query(callback_id, text=f"Removed like ({like_count})", show_alert=False)
            self._logger.info(f"[UNLIKE] @{username}")
        else:
            self.db.add_like(tweet_id, user_id_str, username, message_id)
            like_count = self.db.get_like_count(tweet_id)
            await self.client.answer_callback_query(callback_id, text=f"Liked! ({like_count})", show_alert=False)
            self._logger.info(f"[LIKE] @{username}")
        
        return True
    
    async def _handle_buy(self, callback_id: str, data: str, user_id: int, username: str) -> bool:
        """Handle buy button."""
        if not self.executor:
            await self.client.answer_callback_query(callback_id, text="Trading não disponível", show_alert=True)
            return True
        
        tweet_id = data.split(":", 1)[1]
        config = self.get_user_config(user_id, username)
        
        token_info = self.token_cache.get(tweet_id, {})
        token_symbol = token_info.get("token_symbol", "Token")
        
        text = build_buy_confirm_text(config, token_symbol, tweet_id)
        keyboard = build_buy_confirm_keyboard(tweet_id, config.is_configured())
        
        await self.client.answer_callback_query(callback_id)
        await self.client.send_message(text, parse_mode="HTML", reply_markup=keyboard)
        
        return True
    
    async def _handle_buy_confirm(self, callback_id: str, data: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle buy confirmation."""
        if not self.executor:
            await self.client.answer_callback_query(callback_id, text="Trading não disponível", show_alert=True)
            return True
        
        tweet_id = data.split(":", 1)[1]
        config = self.get_user_config(user_id, username)
        
        if not config.is_configured():
            await self.client.answer_callback_query(callback_id, text="Configure wallet primeiro!", show_alert=True)
            return True
        
        token_info = self.token_cache.get(tweet_id, {})
        token_symbol = token_info.get("token_symbol", "Token")
        
        # Approve buy
        self.executor.approve_buy(tweet_id, user_id, username, token_symbol)
        fee = get_fee_for_block(config.block_offset)
        
        await self.client.answer_callback_query(callback_id, text="BUY aprovado!", show_alert=True)
        
        await self.client.edit_message(
            message_id,
            f"""<b>✅ BUY APROVADO</b>

Token: <b>{_escape_html(token_symbol)}</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)

⏳ Aguardando token ir on-chain...""",
            parse_mode="HTML"
        )
        
        self._logger.info(f"[BUY APPROVED] @{username} -> {token_symbol}")
        return True
    
    async def _handle_config_main(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle main config menu."""
        config = self.get_user_config(user_id, username)
        await self.client.answer_callback_query(callback_id)
        await self.client.edit_message(
            message_id, build_main_menu_text(config), parse_mode="HTML",
            reply_markup=build_main_menu_keyboard()
        )
        return True
    
    async def _handle_config_block(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle block selection menu."""
        config = self.get_user_config(user_id, username)
        await self.client.answer_callback_query(callback_id)
        await self.client.edit_message(
            message_id, build_block_menu_text(config), parse_mode="HTML",
            reply_markup=build_block_menu_keyboard(config)
        )
        return True
    
    async def _handle_config_eth(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle ETH selection menu."""
        config = self.get_user_config(user_id, username)
        await self.client.answer_callback_query(callback_id)
        await self.client.edit_message(
            message_id, build_eth_menu_text(config), parse_mode="HTML",
            reply_markup=build_eth_menu_keyboard(config)
        )
        return True
    
    async def _handle_config_gwei(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle gwei selection menu."""
        config = self.get_user_config(user_id, username)
        await self.client.answer_callback_query(callback_id)
        await self.client.edit_message(
            message_id, build_gwei_menu_text(config), parse_mode="HTML",
            reply_markup=build_gwei_menu_keyboard(config)
        )
        return True
    
    async def _handle_config_autobuy(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle auto-buy toggle."""
        config = self.get_user_config(user_id, username)
        config.auto_buy = not config.auto_buy
        self.save_user_config(user_id)
        
        status = "ON ✅" if config.auto_buy else "OFF ❌"
        await self.client.answer_callback_query(callback_id, text=f"Auto-buy: {status}", show_alert=True)
        await self.client.edit_message(
            message_id, build_main_menu_text(config), parse_mode="HTML",
            reply_markup=build_main_menu_keyboard()
        )
        return True
    
    async def _handle_config_status(self, callback_id: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle status display."""
        config = self.get_user_config(user_id, username)
        stats = self.executor.get_stats() if self.executor else {}
        
        text = build_status_text(config, stats)
        
        await self.client.answer_callback_query(callback_id)
        await self.client.edit_message(
            message_id, text, parse_mode="HTML",
            reply_markup={"inline_keyboard": [[{"text": "< Voltar", "callback_data": "cfg:main"}]]}
        )
        return True
    
    async def _handle_set_block(self, callback_id: str, data: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle block offset setting."""
        blk = int(data.split(":")[1])
        config = self.get_user_config(user_id, username)
        config.block_offset = blk
        self.save_user_config(user_id)
        
        fee = get_fee_for_block(blk)
        await self.client.answer_callback_query(callback_id, text=f"+{blk} = {fee:.1f}%")
        await self.client.edit_message(
            message_id, build_block_menu_text(config), parse_mode="HTML",
            reply_markup=build_block_menu_keyboard(config)
        )
        return True
    
    async def _handle_set_eth(self, callback_id: str, data: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle ETH amount setting."""
        eth = float(data.split(":")[1])
        config = self.get_user_config(user_id, username)
        config.eth_amount = eth
        self.save_user_config(user_id)
        
        await self.client.answer_callback_query(callback_id, text=f"{eth} ETH")
        await self.client.edit_message(
            message_id, build_eth_menu_text(config), parse_mode="HTML",
            reply_markup=build_eth_menu_keyboard(config)
        )
        return True
    
    async def _handle_set_gwei(self, callback_id: str, data: str, user_id: int, username: str, message_id: int) -> bool:
        """Handle gwei setting."""
        gwei = float(data.split(":")[1])
        config = self.get_user_config(user_id, username)
        config.gwei = gwei
        self.save_user_config(user_id)
        
        await self.client.answer_callback_query(callback_id, text=f"{gwei} gwei")
        await self.client.edit_message(
            message_id, build_gwei_menu_text(config), parse_mode="HTML",
            reply_markup=build_gwei_menu_keyboard(config)
        )
        return True


# =============================================================================
# TRADING MESSAGE HANDLER
# =============================================================================

class TradingMessageHandler:
    """
    Handler for trading-related Telegram text commands.
    
    Handles:
    - /start, /config, /menu - show config menu
    - /setwallet 0x... - set wallet address
    - /setkey ... - set private key
    - /fees - show fee schedule
    - /status - show current status
    """
    
    def __init__(
        self,
        telegram_client: AsyncTelegramClient,
        get_user_config_fn,
        save_user_config_fn,
        trading_executor=None,
    ):
        """
        Initialize message handler.
        
        Args:
            telegram_client: Telegram client for sending messages
            get_user_config_fn: Function to get user config
            save_user_config_fn: Function to save user config
            trading_executor: Trading executor instance
        """
        self.client = telegram_client
        self.get_user_config = get_user_config_fn
        self.save_user_config = save_user_config_fn
        self.executor = trading_executor
        self._logger = logging.getLogger(__name__)
    
    async def handle_message(self, message: Dict[str, Any]) -> bool:
        """
        Handle a text message.
        
        Args:
            message: Telegram message object
            
        Returns:
            True if handled, False otherwise
        """
        chat_id = message.get("chat", {}).get("id")
        user_id = message.get("from", {}).get("id")
        username = message.get("from", {}).get("username", "")
        text = message.get("text", "")
        
        if not chat_id or not user_id:
            return False
        
        config = self.get_user_config(user_id, username)
        
        # /start, /config, /menu
        if text in ["/start", "/config", "/menu"]:
            await self._send_to_chat(chat_id, build_main_menu_text(config), reply_markup=build_main_menu_keyboard())
            return True
        
        # /setwallet 0x...
        elif text.startswith("/setwallet "):
            wallet = text.split(" ", 1)[1].strip()
            if wallet.startswith("0x") and len(wallet) == 42:
                config.wallet_address = wallet
                self.save_user_config(user_id)
                await self._send_to_chat(chat_id, f"✅ Wallet configurada: <code>{wallet[:10]}...{wallet[-6:]}</code>")
            else:
                await self._send_to_chat(chat_id, "❌ Wallet inválida. Use formato 0x...")
            return True
        
        # /setkey private_key
        elif text.startswith("/setkey "):
            key = text.split(" ", 1)[1].strip()
            if key.startswith("0x"):
                key = key[2:]
            if len(key) == 64:
                config.wallet_private_key = key
                self.save_user_config(user_id)
                await self._send_to_chat(chat_id, "✅ Private key configurada.")
                self._logger.info(f"[CONFIG] @{username} set private key")
            else:
                await self._send_to_chat(chat_id, "❌ Private key inválida (deve ter 64 caracteres hex)")
            return True
        
        # /fees
        elif text == "/fees":
            await self._send_to_chat(chat_id, build_fees_text())
            return True
        
        # /status
        elif text == "/status":
            stats = self.executor.get_stats() if self.executor else {}
            await self._send_to_chat(chat_id, build_status_text(config, stats))
            return True
        
        return False
    
    async def _send_to_chat(self, chat_id: int, text: str, reply_markup: Dict = None):
        """Send message to specific chat."""
        original_channel = self.client.channel_id
        self.client.channel_id = str(chat_id)
        
        try:
            await self.client.send_message(text, parse_mode="HTML", reply_markup=reply_markup)
        finally:
            self.client.channel_id = original_channel
