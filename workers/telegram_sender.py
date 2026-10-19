"""
Telegram Sender Worker

Sends messages to Telegram channels/chats.

Responsibilities:
- Subscribe to telegram:send channel
- Subscribe to telegram:edit channel
- Send messages to appropriate channels (deployed, prebankr, trading)
- Edit existing messages
- Handle rate limiting

Input Channels:
- telegram:send - Send message requests
- telegram:edit - Edit message requests
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional

from workers.base import BaseWorker
from shared.pubsub import Channels


class TelegramSenderWorker(BaseWorker):
    """
    Sends and edits Telegram messages.
    """
    
    WORKER_NAME = "telegram-sender"
    
    # Telegram API base URL
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self):
        super().__init__()
        
        # Bot tokens
        self.deployed_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.prebankr_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")  # Same bot for now
        self.trading_bot_token = os.getenv("TELEGRAM_TRADING_BOT_TOKEN", "")
        self.preclawnch_bot_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")
        self.clawnch_bot_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")  # Same bot as preclawnch
        # NOTE: MoltX uses same bot/channel as Moltbook (preclawnch)
        
        # Channel IDs
        self.deployed_channel = os.getenv("TELEGRAM_CHANNEL_ID", "")
        self.prebankr_channel = os.getenv("TELEGRAM_PREBANKR_CHANNEL_ID", "")
        self.preclawnch_channel = os.getenv("TELEGRAM_CHAT_ID_PRECLAWNCH", "")
        self.clawnch_channel = os.getenv("TELEGRAM_CHAT_ID_CLAWNCH", "")
        
        if not self.deployed_bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN not set!")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Rate limiting
        self._last_send_time = 0
        self._min_interval = 0.05  # 50ms between sends (20 msg/sec)
        
        # Stats
        self.stats.update({
            "messages_sent": 0,
            "messages_edited": 0,
            "rate_limited": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        )
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Subscribe to send and edit channels."""
        self.logger.info("Subscribing to telegram channels...")
        
        async for channel, message in self.redis.subscribe(
            Channels.TELEGRAM_SEND,
            Channels.TELEGRAM_EDIT,
        ):
            if not self._running:
                break
            
            try:
                if channel == Channels.TELEGRAM_SEND:
                    await self._handle_send(message)
                elif channel == Channels.TELEGRAM_EDIT:
                    await self._handle_edit(message)
            except Exception as e:
                self.logger.error(f"Handler error: {e}")
                self.stats["errors"] += 1
    
    async def _handle_send(self, event: Dict[str, Any]):
        """Handle send message request."""
        chat_id = event.get("chat_id", "")
        text = event.get("text", "")
        parse_mode = event.get("parse_mode", "HTML")
        reply_markup = event.get("reply_markup")
        channel_type = event.get("channel", "deployed")  # deployed, prebankr, trading
        correlation_id = event.get("correlation_id", "")
        
        if not chat_id and not text:
            return
        
        # Determine bot token and chat_id
        bot_token = self._get_bot_token(channel_type)
        if not chat_id:
            chat_id = self._get_default_chat(channel_type)
        
        if not bot_token or not chat_id:
            self.logger.warning(f"Missing config for channel: {channel_type}")
            return
        
        # Rate limiting
        await self._rate_limit()
        
        # Send message
        message_id = await self._send_message(
            bot_token,
            chat_id,
            text,
            parse_mode,
            reply_markup,
        )
        
        if message_id:
            self.stats["messages_sent"] += 1
            self.logger.info(f"Sent message {message_id} to {channel_type}")
            
            # Store message_id for correlation if needed
            if correlation_id:
                await self.redis.set(
                    f"telegram_msg:{correlation_id}",
                    str(message_id),
                    ex=3600,  # 1 hour TTL
                )
        
        self.stats["messages_processed"] += 1
    
    async def _handle_edit(self, event: Dict[str, Any]):
        """Handle edit message request."""
        chat_id = event.get("chat_id", "")
        message_id = event.get("message_id", 0)
        text = event.get("text", "")
        parse_mode = event.get("parse_mode", "HTML")
        reply_markup = event.get("reply_markup")
        correlation_id = event.get("correlation_id", "")
        
        # Get message_id from correlation if not provided
        if not message_id and correlation_id:
            stored = await self.redis.get(f"telegram_msg:{correlation_id}")
            if stored:
                message_id = int(stored)
        
        if not chat_id or not message_id or not text:
            return
        
        # Determine bot token (use deployed by default for edits)
        bot_token = self.deployed_bot_token
        
        # Rate limiting
        await self._rate_limit()
        
        # Edit message
        success = await self._edit_message(
            bot_token,
            chat_id,
            message_id,
            text,
            parse_mode,
            reply_markup,
        )
        
        if success:
            self.stats["messages_edited"] += 1
        
        self.stats["messages_processed"] += 1
    
    def _get_bot_token(self, channel_type: str) -> str:
        """Get bot token for channel type."""
        if channel_type == "trading":
            return self.trading_bot_token
        elif channel_type == "prebankr":
            return self.prebankr_bot_token
        elif channel_type == "preclawnch":
            return self.preclawnch_bot_token
        elif channel_type == "clawnch":
            return self.clawnch_bot_token
        else:
            return self.deployed_bot_token
    
    def _get_default_chat(self, channel_type: str) -> str:
        """Get default chat ID for channel type."""
        if channel_type == "prebankr":
            return self.prebankr_channel
        elif channel_type == "deployed":
            return self.deployed_channel
        elif channel_type == "preclawnch":
            return self.preclawnch_channel
        elif channel_type == "clawnch":
            return self.clawnch_channel
        return ""
    
    async def _rate_limit(self):
        """Apply rate limiting."""
        import time
        now = time.time()
        elapsed = now - self._last_send_time
        
        if elapsed < self._min_interval:
            wait_time = self._min_interval - elapsed
            self.stats["rate_limited"] += 1
            await asyncio.sleep(wait_time)
        
        self._last_send_time = time.time()
    
    async def _send_message(
        self,
        bot_token: str,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Dict = None,
    ) -> Optional[int]:
        """Send message via Telegram API."""
        url = f"{self.API_BASE}{bot_token}/sendMessage"
        
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                
                if data.get("ok"):
                    return data["result"]["message_id"]
                else:
                    self.logger.error(f"Send failed: {data.get('description')}")
                    return None
        
        except asyncio.TimeoutError:
            self.logger.error("Send timeout")
            return None
        except Exception as e:
            self.logger.error(f"Send error: {e}")
            return None
    
    async def _edit_message(
        self,
        bot_token: str,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Dict = None,
    ) -> bool:
        """Edit message via Telegram API."""
        url = f"{self.API_BASE}{bot_token}/editMessageText"
        
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                
                if data.get("ok"):
                    return True
                else:
                    desc = data.get("description", "")
                    # Ignore "message not modified" error
                    if "not modified" not in desc.lower():
                        self.logger.warning(f"Edit failed: {desc}")
                    return False
        
        except asyncio.TimeoutError:
            self.logger.error("Edit timeout")
            return False
        except Exception as e:
            self.logger.error(f"Edit error: {e}")
            return False


# Entry point
if __name__ == "__main__":
    TelegramSenderWorker.main()
