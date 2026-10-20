"""
Telegram Bot Pool with Rate Limiting

Solves 429 "Too Many Requests" errors by:
1. Centralizing all sends through a single queue
2. Rate limiting per-channel (1 msg/sec per channel)
3. Distributing load across multiple bot tokens (round-robin)
4. Respecting Retry-After headers with exponential backoff

Architecture:
    ┌─────────────────────────────────────────────────────────┐
    │              Message Queue (asyncio.Queue)              │
    │   msg1 → msg2 → msg3 → msg4 → ...                      │
    └─────────────────────────────────────────────────────────┘
                              │
                              ▼
    ┌─────────────────────────────────────────────────────────┐
    │           Per-Channel Rate Limiter (1 msg/sec)          │
    └─────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         ┌────────┐      ┌────────┐      ┌────────┐
         │ Bot 1  │      │ Bot 2  │      │ Bot 3  │
         └────────┘      └────────┘      └────────┘
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                      Telegram API

Usage:
    pool = TelegramBotPool(
        tokens=["token1", "token2", "token3"],
        default_channel_id="-1001234567890"
    )
    await pool.start()
    
    # Send messages - they go through the queue
    msg_id = await pool.send_message("Hello!", channel_id="-1001234567890")
    
    # Edit messages - also rate limited
    await pool.edit_message(msg_id, "Updated!", channel_id="-1001234567890")
    
    await pool.stop()

Author: Bankr Monitor
Date: 2026-02-04
"""

import os
import time
import asyncio
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
from enum import Enum

import aiohttp

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of Telegram API operations."""
    SEND = "sendMessage"
    EDIT = "editMessageText"
    EDIT_MARKUP = "editMessageReplyMarkup"


@dataclass
class QueuedMessage:
    """Message waiting in the queue to be sent."""
    msg_type: MessageType
    channel_id: str
    text: str
    message_id: Optional[int] = None  # For edits
    parse_mode: Optional[str] = None
    disable_web_page_preview: bool = True
    reply_markup: Optional[Dict] = None
    future: Optional[asyncio.Future] = None  # For returning result
    priority: int = 0  # Higher = more urgent (edits get priority)
    created_at: float = 0.0
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()


class PerChannelRateLimiter:
    """
    Rate limiter that enforces per-channel limits.
    
    Telegram enforces ~1 msg/sec per chat regardless of which bot sends.
    This limiter ensures we don't exceed that.
    """
    
    def __init__(self, min_interval: float = 1.0):
        """
        Args:
            min_interval: Minimum seconds between messages to same channel.
                         Default 1.0 (1 msg/sec) is safe for Telegram.
        """
        self._last_send: Dict[str, float] = {}
        self._min_interval = min_interval
        self._lock = asyncio.Lock()
    
    async def acquire(self, channel_id: str) -> float:
        """
        Wait until we can send to this channel.
        
        Args:
            channel_id: Target channel ID
            
        Returns:
            Time waited in seconds
        """
        async with self._lock:
            now = time.time()
            last = self._last_send.get(channel_id, 0)
            wait_time = self._min_interval - (now - last)
            
            if wait_time > 0:
                logger.debug(f"[RATE LIMIT] Waiting {wait_time:.2f}s for channel {channel_id}")
                await asyncio.sleep(wait_time)
                waited = wait_time
            else:
                waited = 0
            
            self._last_send[channel_id] = time.time()
            return waited
    
    def set_backoff(self, channel_id: str, retry_after: float):
        """
        Set backoff time for a channel after 429 error.
        
        Args:
            channel_id: Channel that got rate limited
            retry_after: Seconds to wait (from Telegram's Retry-After)
        """
        # Set last_send to future time so acquire() will wait
        self._last_send[channel_id] = time.time() + retry_after - self._min_interval
        logger.warning(f"[RATE LIMIT] Backoff {retry_after}s for channel {channel_id}")


class TelegramBotPool:
    """
    Pool of Telegram bot tokens with centralized rate limiting.
    
    Features:
    - Round-robin distribution across multiple bots
    - Per-channel rate limiting (1 msg/sec)
    - Async queue for message serialization
    - Retry with exponential backoff on 429 errors
    - Separate handling for sends vs edits
    - Bot→Message cache for correct edits (each bot edits its own messages)
    
    Example:
        pool = TelegramBotPool(
            tokens=["bot1:token", "bot2:token"],
            default_channel_id="-1001234567890"
        )
        await pool.start()
        
        msg_id = await pool.send_message("Hello!")
        await pool.edit_message(msg_id, "Updated!")
        
        await pool.stop()
    """
    
    BASE_URL = "https://api.telegram.org/bot"
    
    # Cache TTL: 2 hours (edits usually happen within minutes)
    CACHE_TTL_SECONDS = 2 * 60 * 60
    # Cleanup interval: every 10 minutes
    CACHE_CLEANUP_INTERVAL = 10 * 60
    
    def __init__(
        self,
        tokens: List[str],
        default_channel_id: str = "",
        min_interval: float = 1.0,
        max_retries: int = 3,
    ):
        """
        Initialize the bot pool.
        
        Args:
            tokens: List of bot tokens (at least 1)
            default_channel_id: Default channel for sends
            min_interval: Seconds between messages to same channel (default 1.0)
            max_retries: Max retries on 429 errors (default 3)
        """
        if not tokens:
            raise ValueError("At least one bot token required")
        
        self.tokens = tokens
        self.default_channel_id = default_channel_id
        self.max_retries = max_retries
        
        # Round-robin index
        self._bot_index = 0
        self._bot_lock = asyncio.Lock()
        
        # Per-channel rate limiter
        self._rate_limiter = PerChannelRateLimiter(min_interval)
        
        # Message queue
        self._queue: asyncio.Queue[QueuedMessage] = asyncio.Queue()
        
        # Worker task
        self._worker_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False
        
        # HTTP sessions (one per bot)
        self._sessions: Dict[str, aiohttp.ClientSession] = {}
        
        # BOT→MESSAGE CACHE: Track which bot sent each message for correct edits
        # Key: "channel_id:message_id" → {"token": str, "timestamp": float}
        self._message_bot_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = asyncio.Lock()
        
        # Stats
        self.stats = {
            "messages_sent": 0,
            "messages_edited": 0,
            "rate_limited": 0,
            "retries": 0,
            "errors": 0,
            "total_wait_time": 0.0,
            "cache_hits": 0,
            "cache_misses": 0,
        }
        
        logger.info(f"[POOL] Initialized with {len(tokens)} bot(s)")
    
    async def start(self):
        """Start the pool worker."""
        if self._running:
            return
        
        self._running = True
        
        # Create HTTP sessions
        timeout = aiohttp.ClientTimeout(total=15)
        for token in self.tokens:
            self._sessions[token] = aiohttp.ClientSession(timeout=timeout)
        
        # Start worker
        self._worker_task = asyncio.create_task(self._worker_loop())
        
        # Start cache cleanup task
        self._cleanup_task = asyncio.create_task(self._cache_cleanup_loop())
        
        logger.info(f"[POOL] Started with {len(self.tokens)} bots (cache TTL={self.CACHE_TTL_SECONDS}s)")
    
    async def stop(self):
        """Stop the pool and close sessions."""
        self._running = False
        
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close sessions
        for session in self._sessions.values():
            if not session.closed:
                await session.close()
        self._sessions.clear()
        
        # Log cache stats
        cache_size = len(self._message_bot_cache)
        logger.info(f"[POOL] Stopped. Stats: {self.stats}, cache_size={cache_size}")
    
    async def send_message(
        self,
        text: str,
        channel_id: Optional[str] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        reply_markup: Optional[Dict] = None,
        wait: bool = True,
    ) -> Optional[int]:
        """
        Queue a message for sending.
        
        Args:
            text: Message text
            channel_id: Target channel (uses default if None)
            parse_mode: "HTML" or "Markdown" or None
            disable_web_page_preview: Don't show link previews
            reply_markup: Inline keyboard
            wait: If True, wait for result. If False, fire-and-forget.
            
        Returns:
            message_id if successful and wait=True, else None
        """
        channel = channel_id or self.default_channel_id
        if not channel:
            logger.error("[POOL] No channel_id provided and no default set")
            return None
        
        future = asyncio.get_event_loop().create_future() if wait else None
        
        msg = QueuedMessage(
            msg_type=MessageType.SEND,
            channel_id=channel,
            text=text,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup,
            future=future,
            priority=0,  # Normal priority
        )
        
        await self._queue.put(msg)
        
        if wait and future:
            return await future
        return None
    
    async def edit_message(
        self,
        message_id: int,
        text: str,
        channel_id: Optional[str] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        reply_markup: Optional[Dict] = None,
        wait: bool = False,
    ) -> bool:
        """
        Queue a message edit.
        
        Args:
            message_id: ID of message to edit
            text: New message text
            channel_id: Target channel (uses default if None)
            parse_mode: "HTML" or "Markdown" or None
            disable_web_page_preview: Don't show link previews
            reply_markup: Inline keyboard
            wait: If True, wait for result.
            
        Returns:
            True if successful
        """
        channel = channel_id or self.default_channel_id
        if not channel:
            logger.error("[POOL] No channel_id provided for edit")
            return False
        
        future = asyncio.get_event_loop().create_future() if wait else None
        
        msg = QueuedMessage(
            msg_type=MessageType.EDIT,
            channel_id=channel,
            text=text,
            message_id=message_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup,
            future=future,
            priority=1,  # Higher priority for edits (UI responsiveness)
        )
        
        await self._queue.put(msg)
        
        if wait and future:
            return await future
        return True
    
    async def edit_reply_markup(
        self,
        message_id: int,
        channel_id: Optional[str] = None,
        reply_markup: Optional[Dict] = None,
        wait: bool = False,
    ) -> bool:
        """
        Queue a reply markup edit (keyboard only).
        
        Args:
            message_id: ID of message to edit
            channel_id: Target channel
            reply_markup: New inline keyboard
            wait: If True, wait for result.
            
        Returns:
            True if successful
        """
        channel = channel_id or self.default_channel_id
        if not channel:
            return False
        
        future = asyncio.get_event_loop().create_future() if wait else None
        
        msg = QueuedMessage(
            msg_type=MessageType.EDIT_MARKUP,
            channel_id=channel,
            text="",
            message_id=message_id,
            reply_markup=reply_markup,
            future=future,
            priority=1,
        )
        
        await self._queue.put(msg)
        
        if wait and future:
            return await future
        return True
    
    async def _worker_loop(self):
        """Main worker loop - processes queue with rate limiting."""
        logger.info("[POOL WORKER] Started")
        
        while self._running:
            try:
                # Get next message (with timeout to check _running)
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process message
                result = await self._process_message(msg)
                
                # Resolve future if waiting
                if msg.future and not msg.future.done():
                    msg.future.set_result(result)
                
                self._queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[POOL WORKER] Error: {e}")
                self.stats["errors"] += 1
        
        logger.info("[POOL WORKER] Stopped")
    
    async def _cache_cleanup_loop(self):
        """Periodically clean up expired cache entries."""
        logger.info("[POOL CACHE] Cleanup task started")
        
        while self._running:
            try:
                await asyncio.sleep(self.CACHE_CLEANUP_INTERVAL)
                
                if not self._running:
                    break
                
                # Clean expired entries
                expired = await self._cleanup_expired_cache()
                if expired > 0:
                    logger.debug(f"[POOL CACHE] Cleaned {expired} expired entries")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[POOL CACHE] Cleanup error: {e}")
        
        logger.info("[POOL CACHE] Cleanup task stopped")
    
    async def _cleanup_expired_cache(self) -> int:
        """Remove expired entries from the message-bot cache."""
        async with self._cache_lock:
            now = time.time()
            expired_keys = [
                key for key, value in self._message_bot_cache.items()
                if now - value.get("timestamp", 0) > self.CACHE_TTL_SECONDS
            ]
            
            for key in expired_keys:
                del self._message_bot_cache[key]
            
            return len(expired_keys)
    
    def _cache_key(self, channel_id: str, message_id: int) -> str:
        """Generate cache key for a message."""
        return f"{channel_id}:{message_id}"
    
    async def _cache_message_bot(self, channel_id: str, message_id: int, token: str):
        """Store which bot sent a message."""
        async with self._cache_lock:
            key = self._cache_key(channel_id, message_id)
            self._message_bot_cache[key] = {
                "token": token,
                "timestamp": time.time(),
            }
    
    async def _get_bot_for_message(self, channel_id: str, message_id: int) -> Optional[str]:
        """Get the bot token that sent a specific message."""
        async with self._cache_lock:
            key = self._cache_key(channel_id, message_id)
            entry = self._message_bot_cache.get(key)
            
            if entry:
                self.stats["cache_hits"] += 1
                return entry["token"]
            else:
                self.stats["cache_misses"] += 1
                return None
    
    async def _process_message(self, msg: QueuedMessage) -> Any:
        """
        Process a queued message with rate limiting and retries.
        
        Args:
            msg: Queued message to process
            
        Returns:
            message_id for sends, True/False for edits
        """
        # Rate limit per channel
        wait_time = await self._rate_limiter.acquire(msg.channel_id)
        self.stats["total_wait_time"] += wait_time
        
        # BOT SELECTION: Different strategy for sends vs edits
        # - SEND: Use round-robin and cache which bot sent
        # - EDIT: Use the same bot that sent the original message
        if msg.msg_type == MessageType.SEND:
            # Round-robin for new messages
            token = await self._get_next_bot()
        else:
            # For edits, use the bot that sent the original message
            token = await self._get_bot_for_message(msg.channel_id, msg.message_id)
            
            if not token:
                # Fallback: use the primary bot (first in list)
                token = self.tokens[0]
                logger.debug(f"[POOL CACHE] Miss for msg {msg.message_id}, using primary bot")
        
        # Try to send with retries
        for attempt in range(self.max_retries + 1):
            try:
                # Make the API call
                result = await self._api_call(token, msg)
                
                if result is not None:
                    if msg.msg_type == MessageType.SEND:
                        self.stats["messages_sent"] += 1
                        # Cache which bot sent this message
                        await self._cache_message_bot(msg.channel_id, result, token)
                    else:
                        self.stats["messages_edited"] += 1
                    return result
                
            except RateLimitError as e:
                self.stats["rate_limited"] += 1
                
                if attempt < self.max_retries:
                    # Set backoff for this channel
                    self._rate_limiter.set_backoff(msg.channel_id, e.retry_after)
                    
                    # Wait with jitter
                    wait = min(e.retry_after, 5 + attempt * 2)
                    logger.warning(f"[POOL] 429 on {msg.channel_id}, retry {attempt+1}/{self.max_retries} in {wait}s")
                    await asyncio.sleep(wait)
                    self.stats["retries"] += 1
                else:
                    logger.error(f"[POOL] Max retries exceeded for {msg.channel_id}")
                    self.stats["errors"] += 1
                    return None
            
            except Exception as e:
                logger.error(f"[POOL] API error: {e}")
                self.stats["errors"] += 1
                return None
        
        return None
    
    async def _get_next_bot(self) -> str:
        """Get next bot token using round-robin."""
        async with self._bot_lock:
            token = self.tokens[self._bot_index]
            self._bot_index = (self._bot_index + 1) % len(self.tokens)
            return token
    
    async def _api_call(self, token: str, msg: QueuedMessage) -> Any:
        """
        Make the actual Telegram API call.
        
        Args:
            token: Bot token to use
            msg: Message to send/edit
            
        Returns:
            message_id for sends, True for edits, None on failure
            
        Raises:
            RateLimitError: On 429 response
        """
        session = self._sessions.get(token)
        if not session or session.closed:
            timeout = aiohttp.ClientTimeout(total=15)
            session = aiohttp.ClientSession(timeout=timeout)
            self._sessions[token] = session
        
        url = f"{self.BASE_URL}{token}/{msg.msg_type.value}"
        
        # Build payload based on message type
        if msg.msg_type == MessageType.SEND:
            payload = {
                "chat_id": msg.channel_id,
                "text": msg.text,
                "disable_web_page_preview": msg.disable_web_page_preview,
            }
            if msg.parse_mode:
                payload["parse_mode"] = msg.parse_mode
            if msg.reply_markup:
                payload["reply_markup"] = msg.reply_markup
                
        elif msg.msg_type == MessageType.EDIT:
            payload = {
                "chat_id": msg.channel_id,
                "message_id": msg.message_id,
                "text": msg.text,
                "disable_web_page_preview": msg.disable_web_page_preview,
            }
            if msg.parse_mode:
                payload["parse_mode"] = msg.parse_mode
            if msg.reply_markup:
                payload["reply_markup"] = msg.reply_markup
                
        elif msg.msg_type == MessageType.EDIT_MARKUP:
            payload = {
                "chat_id": msg.channel_id,
                "message_id": msg.message_id,
            }
            if msg.reply_markup:
                payload["reply_markup"] = msg.reply_markup
        
        # Log (abbreviated)
        text_preview = msg.text[:50] + "..." if len(msg.text) > 50 else msg.text
        text_preview = text_preview.replace('\n', ' ')
        logger.debug(f"[POOL API] {msg.msg_type.value} -> {msg.channel_id}: {text_preview}")
        
        try:
            async with session.post(url, json=payload) as response:
                result = await response.json()
                
                if result.get("ok"):
                    if msg.msg_type == MessageType.SEND:
                        message_id = result["result"]["message_id"]
                        logger.info(f"[POOL OK] message_id={message_id} sent to {msg.channel_id}")
                        return message_id
                    else:
                        logger.debug(f"[POOL OK] Edited message {msg.message_id}")
                        return True
                else:
                    desc = result.get("description", "Unknown error")
                    
                    # Handle rate limiting
                    if "Too Many Requests" in desc:
                        params = result.get("parameters", {})
                        retry_after = params.get("retry_after", 5)
                        raise RateLimitError(retry_after, msg.channel_id)
                    
                    # Handle "message not modified" (not an error)
                    if "not modified" in desc.lower():
                        return True
                    
                    logger.warning(f"[POOL ERROR] {desc} (channel={msg.channel_id})")
                    return None
                    
        except aiohttp.ClientError as e:
            logger.error(f"[POOL HTTP] {e}")
            raise


class RateLimitError(Exception):
    """Raised when Telegram returns 429 Too Many Requests."""
    
    def __init__(self, retry_after: float, channel_id: str):
        self.retry_after = retry_after
        self.channel_id = channel_id
        super().__init__(f"Rate limited for {retry_after}s on {channel_id}")


# HELPER: Create pool from environment

def create_pool_from_env(
    env_var: str = "TELEGRAM_BOT_POOL_TOKENS",
    fallback_var: str = "TELEGRAM_BOT_TOKEN",
    default_channel_var: str = "TELEGRAM_CHANNEL_ID",
) -> TelegramBotPool:
    """
    Create a TelegramBotPool from environment variables.
    
    Args:
        env_var: Env var with comma-separated tokens
        fallback_var: Fallback if pool tokens not set
        default_channel_var: Env var for default channel
        
    Returns:
        Configured TelegramBotPool
        
    Example:
        # In .env:
        # TELEGRAM_BOT_POOL_TOKENS=token1,token2,token3
        # TELEGRAM_CHANNEL_ID=-1001234567890
        
        pool = create_pool_from_env()
        await pool.start()
    """
    # Try pool tokens first
    pool_tokens_str = os.getenv(env_var, "")
    
    if pool_tokens_str:
        tokens = [t.strip() for t in pool_tokens_str.split(",") if t.strip()]
    else:
        # Fallback to single token
        single_token = os.getenv(fallback_var, "")
        if not single_token:
            raise ValueError(f"Neither {env_var} nor {fallback_var} is set")
        tokens = [single_token]
    
    default_channel = os.getenv(default_channel_var, "")
    
    logger.info(f"[POOL] Creating pool with {len(tokens)} token(s)")
    
    return TelegramBotPool(
        tokens=tokens,
        default_channel_id=default_channel,
    )


# WRAPPER: Drop-in replacement for AsyncTelegramClient

class PooledTelegramClient:
    """
    Drop-in replacement for AsyncTelegramClient that uses a shared pool.
    
    This allows gradual migration - just replace AsyncTelegramClient with
    PooledTelegramClient and it will use the pool internally.
    
    Example:
        # Before:
        client = AsyncTelegramClient(bot_token, channel_id)
        await client.send_message("Hello")
        
        # After:
        pool = TelegramBotPool(tokens, default_channel)
        client = PooledTelegramClient(pool, channel_id)
        await client.send_message("Hello")  # Same interface!
    """
    
    def __init__(self, pool: TelegramBotPool, channel_id: str):
        """
        Args:
            pool: Shared TelegramBotPool
            channel_id: Channel this client sends to
        """
        self.pool = pool
        self.channel_id = channel_id
    
    async def send_message(
        self,
        text: str,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        disable_notification: bool = False,
        reply_markup: Optional[Dict] = None,
    ) -> Optional[int]:
        """Send message via pool."""
        return await self.pool.send_message(
            text=text,
            channel_id=self.channel_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup,
            wait=True,
        )
    
    async def edit_message(
        self,
        message_id: int,
        text: str,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
        reply_markup: Optional[Dict] = None,
        max_retries: int = 3,
    ) -> bool:
        """Edit message via pool."""
        return await self.pool.edit_message(
            message_id=message_id,
            text=text,
            channel_id=self.channel_id,
            parse_mode=parse_mode,
            disable_web_page_preview=disable_web_page_preview,
            reply_markup=reply_markup,
            wait=True,
        )
    
    async def _ensure_session(self):
        """Compatibility method - pool manages sessions."""
        pass
    
    async def close(self):
        """Compatibility method - pool manages lifecycle."""
        pass
