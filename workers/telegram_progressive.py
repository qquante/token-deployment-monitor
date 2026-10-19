"""
Telegram Progressive Worker

Sends progressive Telegram messages with live editing as user data is enriched.

Uses the same flow as websocket_async.py:
1. Send initial message with "loading" placeholders
2. Enrich users in parallel
3. Edit message as each user's data arrives
4. Mark complete when all data loaded

Input Channels:
- twitter:prebankr - PRE-BANKR request detected
- twitter:deployed - @bankrbot DEPLOYED notification

This worker replaces the static telegram_sender for PRE-BANKR and DEPLOYED messages.
"""

import os
import sys
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime

from workers.base import BaseWorker, project_root
from shared.pubsub import Channels, Events
from shared.models import TokenInfo

# Import async clients from src
sys.path.insert(0, str(project_root))
from src.async_clients.telegram import (
    AsyncTelegramClient,
    PreBankrMessage,
    OnchainDeploymentMessage,
    ClawnchDeploymentMessage,
    ProgressiveMessage,
)
from src.async_clients.telegram_pool import (
    TelegramBotPool,
    PooledTelegramClient,
)


class TelegramProgressiveWorker(BaseWorker):
    """
    Handles progressive Telegram messages with live editing.
    
    Exactly like websocket_async.py but as a worker.
    
    Supports OnChain-first flow:
    1. Token detected on-chain -> send OnchainDeploymentMessage
    2. Twitter confirms -> update existing message with Twitter data
    """
    
    WORKER_NAME = "telegram-progressive"
    
    def __init__(self):
        super().__init__()
        
        # API configs
        self.twitter_api_key = os.getenv("TWITTER_API_KEY", "")
        self.alphagate_user_id = os.getenv("ALPHAGATE_USER_ID", "")
        self.alphagate_session_id = os.getenv("ALPHAGATE_SESSION_ID", "")
        
        # Telegram clients (now using pool for rate limiting)
        self.telegram_deployed: Optional[PooledTelegramClient] = None
        self.telegram_prebankr: Optional[PooledTelegramClient] = None
        self.telegram_clawnch: Optional[AsyncTelegramClient] = None  # Clawnch uses separate bot
        
        # Bot pool for rate-limited sending (Bankr channels only)
        self._bot_pool: Optional[TelegramBotPool] = None
        
        # HTTP session for API calls
        self._session: Optional[aiohttp.ClientSession] = None
        
        # User cache (prevents redundant API calls)
        self._user_cache: Dict[str, Dict[str, Any]] = {}
        
        # Processed tweets (dedupe)
        self._processed: set = set()
        
        # Pending on-chain messages (for OnChain-first flow)
        # Maps tweet_id -> OnchainDeploymentMessage
        self._pending_onchain_msgs: Dict[str, OnchainDeploymentMessage] = {}
        
        # Stats
        self.stats.update({
            "prebankr_sent": 0,
            "deployed_sent": 0,
            "onchain_sent": 0,
            "onchain_updated": 0,
            "clawnch_sent": 0,
            "users_enriched": 0,
            "edits_made": 0,
        })
        
        # Known Clawnch platforms (won't trigger "new platform" alert)
        self._known_clawnch_platforms = {"moltbook", "4claw", "moltx"}
        # Track platforms we've seen in this session
        self._seen_clawnch_platforms: set = set()
        
        # For command polling (blacklist)
        self._last_update_id: Optional[int] = None
    
    async def setup(self):
        """Initialize clients."""
        await super().setup()
        
        # HTTP session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        
        # =======================================================================
        # BOT POOL SETUP (for rate-limited Bankr channel sends)
        # =======================================================================
        # Get pool tokens (comma-separated) or fallback to single token
        pool_tokens_str = os.getenv("TELEGRAM_BOT_POOL_TOKENS", "")
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        
        if pool_tokens_str:
            # Use pool tokens
            pool_tokens = [t.strip() for t in pool_tokens_str.split(",") if t.strip()]
            self.logger.info(f"[POOL] Using {len(pool_tokens)} bot tokens from TELEGRAM_BOT_POOL_TOKENS")
        elif bot_token:
            # Fallback to single token
            pool_tokens = [bot_token]
            self.logger.info(f"[POOL] Using single bot token from TELEGRAM_BOT_TOKEN")
        else:
            raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_POOL_TOKENS not set!")
        
        deployed_channel = os.getenv("TELEGRAM_CHANNEL_ID", "")
        prebankr_channel = os.getenv("TELEGRAM_PREBANKR_CHANNEL_ID", "")
        
        # Create bot pool with rate limiting (1 msg/sec per channel)
        self._bot_pool = TelegramBotPool(
            tokens=pool_tokens,
            default_channel_id=deployed_channel,
            min_interval=1.0,  # 1 second between msgs to same channel
            max_retries=3,
        )
        await self._bot_pool.start()
        self.logger.info(f"[POOL] Started with {len(pool_tokens)} bots, rate limit: 1 msg/sec per channel")
        
        # Create pooled clients (drop-in replacement for AsyncTelegramClient)
        self.telegram_deployed = PooledTelegramClient(
            pool=self._bot_pool,
            channel_id=deployed_channel,
        )
        
        self.telegram_prebankr = PooledTelegramClient(
            pool=self._bot_pool,
            channel_id=prebankr_channel,
        )
        
        # =======================================================================
        # CLAWNCH SETUP (separate bot, not pooled - lower volume)
        # =======================================================================
        clawnch_bot_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")
        clawnch_channel = os.getenv("TELEGRAM_CLAWNCH_CHANNEL_ID", "") or "-1003726736391"
        if clawnch_bot_token:
            self.telegram_clawnch = AsyncTelegramClient(
                bot_token=clawnch_bot_token,
                channel_id=clawnch_channel,
            )
            await self.telegram_clawnch._ensure_session()
            self.logger.info(f"  Clawnch channel: {clawnch_channel} (separate bot, no pool)")
        else:
            self.telegram_clawnch = None
            self.logger.warning("TELEGRAM_BOT_TOKEN_CLAWNCH not set - Clawnch notifications disabled")
        
        # New Interface Detection channel - uses same bot as Clawnch
        newinterface_channel = os.getenv("TELEGRAM_NEWINTERFACE_CHANNEL_ID", "") or "-1003670396729"
        if clawnch_bot_token:
            self.telegram_newinterface = AsyncTelegramClient(
                bot_token=clawnch_bot_token,
                channel_id=newinterface_channel,
            )
            await self.telegram_newinterface._ensure_session()
            self.logger.info(f"  NewInterface channel: {newinterface_channel}")
        else:
            self.telegram_newinterface = None
        
        # Load seen interfaces from Redis (for new interface detection)
        await self.storage.init_default_interfaces()
        self._seen_interfaces = await self.storage.get_seen_interfaces()
        self.logger.info(f"  Loaded {len(self._seen_interfaces)} seen interfaces from Redis")
        
        self.logger.info(f"Telegram clients initialized (with bot pool)")
        self.logger.info(f"  Deployed channel: {deployed_channel}")
        self.logger.info(f"  PreBankr channel: {prebankr_channel}")
    
    async def teardown(self):
        """Close clients and pool."""
        if self._session:
            await self._session.close()
        
        # Stop bot pool (handles deployed/prebankr)
        if self._bot_pool:
            await self._bot_pool.stop()
            self.logger.info(f"[POOL] Stopped. Stats: {self._bot_pool.stats}")
        
        # Close Clawnch client (not pooled)
        if self.telegram_clawnch:
            await self.telegram_clawnch.close()
        if hasattr(self, 'telegram_newinterface') and self.telegram_newinterface:
            await self.telegram_newinterface.close()
        
        await super().teardown()
    
    async def run(self):
        """Subscribe to prebankr, deployed, and onchain token channels."""
        self.logger.info("Subscribing to twitter:prebankr, twitter:deployed, and onchain:token...")
        
        # Start command polling in background (for blacklist commands)
        asyncio.create_task(self._poll_blacklist_commands())
        
        # Track background tasks to prevent garbage collection
        self._background_tasks: set = set()
        
        async for channel, message in self.redis.subscribe(
            Channels.TWITTER_PREBANKR,
            Channels.TWITTER_DEPLOYED,
            Channels.ONCHAIN_TOKEN,
        ):
            if not self._running:
                break
            
            # Process events in PARALLEL using create_task
            # This prevents blocking while waiting for enrichment APIs
            try:
                if channel == Channels.TWITTER_PREBANKR:
                    task = asyncio.create_task(self._handle_prebankr_safe(message))
                elif channel == Channels.TWITTER_DEPLOYED:
                    task = asyncio.create_task(self._handle_deployed_safe(message))
                elif channel == Channels.ONCHAIN_TOKEN:
                    task = asyncio.create_task(self._handle_onchain_token_safe(message))
                else:
                    continue
                
                # Track task to prevent GC and handle completion
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                
            except Exception as e:
                self.logger.error(f"Task creation error: {e}")
                self.stats["errors"] += 1
    
    async def _handle_prebankr_safe(self, message: Dict[str, Any]):
        """Safe wrapper for _handle_prebankr with error handling."""
        try:
            await self._handle_prebankr(message)
        except Exception as e:
            self.logger.error(f"PRE-BANKR handler error: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    async def _handle_deployed_safe(self, message: Dict[str, Any]):
        """Safe wrapper for _handle_deployed with error handling."""
        try:
            await self._handle_deployed(message)
        except Exception as e:
            self.logger.error(f"DEPLOYED handler error: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    async def _handle_onchain_token_safe(self, message: Dict[str, Any]):
        """Safe wrapper for _handle_onchain_token with error handling."""
        try:
            await self._handle_onchain_token(message)
        except Exception as e:
            self.logger.error(f"ONCHAIN handler error: {e}")
            import traceback
            traceback.print_exc()
            self.stats["errors"] += 1
    
    # =========================================================================
    # BLACKLIST COMMAND POLLING (PRE-BANKR channel)
    # =========================================================================
    
    async def _poll_blacklist_commands(self):
        """Poll Telegram updates for blacklist commands in PRE-BANKR channel."""
        if not self.telegram_prebankr:
            self.logger.warning("[BLACKLIST] No PRE-BANKR client - skipping command polling")
            return
        
        self.logger.info(f"[ADMIN CMD] Starting command poll loop for admin group")
        
        while self._running:
            try:
                # Get updates from Telegram
                updates = await self._get_blacklist_updates()
                
                for update in updates:
                    self._last_update_id = update.get("update_id", self._last_update_id)
                    
                    # Handle message
                    message = update.get("message")
                    if message:
                        await self._handle_blacklist_command(message)
                
                await asyncio.sleep(2)  # Poll every 2 seconds
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"[BLACKLIST] Poll error: {e}")
                await asyncio.sleep(5)
    
    async def _get_blacklist_updates(self) -> List[Dict[str, Any]]:
        """Get updates from Telegram for admin group commands."""
        bot_token = os.getenv("TELEGRAM_ADMIN_BOT_TOKEN", "")
        if not bot_token:
            return []
        
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        payload = {
            "timeout": 30,
            "allowed_updates": ["message"],
        }
        
        if self._last_update_id:
            payload["offset"] = self._last_update_id + 1
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data.get("result", [])
        except Exception as e:
            self.logger.debug(f"[BLACKLIST] Get updates error: {e}")
        
        return []
    
    async def _handle_blacklist_command(self, message: Dict[str, Any]):
        """Handle blacklist commands from ADMIN GROUP."""
        chat_id = message.get("chat", {}).get("id", 0)
        text = message.get("text", "").strip().lower()
        
        # Get ADMIN GROUP ID (supergroup)
        admin_group = os.getenv("TELEGRAM_ADMIN_GROUP_ID", "")
        if not admin_group:
            return
        
        # Only process from ADMIN GROUP
        if str(chat_id) != admin_group:
            return
        
        if not text or not text.startswith("/"):
            return
        
        self.logger.info(f"[ADMIN CMD] Received: {text}")
        
        # =====================================================================
        # NEW COMMAND FORMAT:
        # /blacklist bankr @handle1 @handle2       - Add handles to blacklist
        # /blacklist bankr word xxx yyy            - Add words to blacklist  
        # /listblacklist bankr                     - List handle blacklist
        # /listblacklist bankr word                - List word blacklist
        # /removeblacklist bankr {id}              - Remove handle by ID
        # /removeblacklist bankr word {id}         - Remove word by ID
        # =====================================================================
        
        # Parse command parts
        parts = text.split()
        cmd = parts[0] if parts else ""
        
        # /blacklist bankr ...
        if cmd == "/blacklist" and len(parts) >= 3 and parts[1] == "bankr":
            if parts[2] == "word" and len(parts) >= 4:
                # /blacklist bankr word xxx yyy
                words = parts[3:]
                await self._cmd_bankr_word_add(words, chat_id)
            else:
                # /blacklist bankr @handle1 @handle2
                handles = parts[2:]
                await self._cmd_bankr_handle_add(handles, chat_id)
        
        # /listblacklist bankr [word]
        elif cmd == "/listblacklist" and len(parts) >= 2 and parts[1] == "bankr":
            if len(parts) >= 3 and parts[2] == "word":
                await self._cmd_bankr_word_list(chat_id)
            else:
                await self._cmd_bankr_handle_list(chat_id)
        
        # /removeblacklist bankr [word] {id}
        elif cmd == "/removeblacklist" and len(parts) >= 3 and parts[1] == "bankr":
            if parts[2] == "word" and len(parts) >= 4:
                # /removeblacklist bankr word {id}
                try:
                    blacklist_id = int(parts[3])
                    await self._cmd_bankr_word_remove(blacklist_id, chat_id)
                except ValueError:
                    await self._send_admin_message(chat_id, f"❌ ID inválido: {parts[3]}")
            else:
                # /removeblacklist bankr {id}
                try:
                    blacklist_id = int(parts[2])
                    await self._cmd_bankr_handle_remove(blacklist_id, chat_id)
                except ValueError:
                    await self._send_admin_message(chat_id, f"❌ ID inválido: {parts[2]}")
        
        # /help - show available commands
        elif cmd == "/help":
            await self._cmd_help(chat_id)
    
    # =========================================================================
    # BANKR BLACKLIST COMMANDS (Admin Group)
    # =========================================================================
    
    async def _cmd_bankr_handle_add(self, handles: List[str], chat_id: int):
        """Handle /blacklist bankr @handle1 @handle2 command."""
        if not handles:
            await self._send_admin_message(
                chat_id,
                "❌ Uso: <code>/blacklist bankr @handle1 @handle2</code>"
            )
            return
        
        added = []
        already = []
        
        for handle in handles:
            # Clean up handle (remove @ and URLs)
            clean = handle.strip().lstrip("@")
            
            # Handle URL format (https://x.com/handle)
            if "x.com/" in clean or "twitter.com/" in clean:
                clean = clean.split("/")[-1].split("?")[0]
            
            if not clean:
                continue
            
            # Check if already blacklisted
            entries = await self.storage.list_prebankr_blacklist()
            existing = [e for e in entries if e["handle"] == clean.lower()]
            
            if existing:
                already.append(clean.lower())
            else:
                # Add to blacklist
                blacklist_id = await self.storage.add_prebankr_blacklist(clean)
                if blacklist_id > 0:
                    added.append((blacklist_id, clean.lower()))
        
        # Build response
        lines = []
        if added:
            lines.append(f"✅ <b>BANKR Handles adicionados:</b>")
            for bid, h in added:
                lines.append(f"  • #{bid} @{h}")
        if already:
            lines.append(f"\n⚠️ <b>Já estavam na blacklist:</b>")
            for h in already:
                lines.append(f"  • @{h}")
        
        if lines:
            entries = await self.storage.list_prebankr_blacklist()
            lines.append(f"\n📊 Total handles: <b>{len(entries)}</b>")
            await self._send_admin_message(chat_id, "\n".join(lines))
    
    async def _cmd_bankr_handle_list(self, chat_id: int):
        """Handle /listblacklist bankr command."""
        blacklist = await self.storage.list_prebankr_blacklist()
        
        if not blacklist:
            await self._send_admin_message(
                chat_id,
                "📋 <b>BANKR Handle Blacklist vazia</b>\n\nUse <code>/blacklist bankr @handle</code> para adicionar"
            )
            return
        
        lines = [f"📋 <b>BANKR Handle Blacklist ({len(blacklist)})</b>\n"]
        for entry in blacklist:
            lines.append(f"  #{entry['id']}. @{entry['handle']}")
        
        lines.append(f"\n💡 Remover: <code>/removeblacklist bankr {{id}}</code>")
        await self._send_admin_message(chat_id, "\n".join(lines))
    
    async def _cmd_bankr_handle_remove(self, blacklist_id: int, chat_id: int):
        """Handle /removeblacklist bankr {id} command."""
        removed_handle = await self.storage.remove_prebankr_blacklist_by_id(blacklist_id)
        
        if removed_handle:
            entries = await self.storage.list_prebankr_blacklist()
            await self._send_admin_message(
                chat_id,
                f"✅ <b>Removido:</b> #{blacklist_id} @{removed_handle}\n\n📊 Total: <b>{len(entries)}</b>"
            )
        else:
            await self._send_admin_message(
                chat_id,
                f"❌ ID #{blacklist_id} não encontrado"
            )
    
    async def _cmd_bankr_word_add(self, words: List[str], chat_id: int):
        """Handle /blacklist bankr word xxx yyy command."""
        if not words:
            await self._send_admin_message(
                chat_id,
                "❌ Uso: <code>/blacklist bankr word palavra1 palavra2</code>"
            )
            return
        
        added = []
        already = []
        
        for word in words:
            word = word.strip().lower()
            if not word:
                continue
            
            # Check if already blacklisted
            entries = await self.storage.list_prebankr_word_blacklist()
            existing = [e for e in entries if e["word"] == word]
            
            if existing:
                already.append(word)
            else:
                blacklist_id = await self.storage.add_prebankr_word_blacklist(word)
                if blacklist_id > 0:
                    added.append((blacklist_id, word))
        
        lines = []
        if added:
            lines.append(f"✅ <b>BANKR Palavras adicionadas:</b>")
            for bid, w in added:
                lines.append(f"  • #{bid} '{w}'")
        if already:
            lines.append(f"\n⚠️ <b>Já estavam na blacklist:</b>")
            for w in already:
                lines.append(f"  • '{w}'")
        
        if lines:
            entries = await self.storage.list_prebankr_word_blacklist()
            lines.append(f"\n📊 Total palavras: <b>{len(entries)}</b>")
            await self._send_admin_message(chat_id, "\n".join(lines))
    
    async def _cmd_bankr_word_list(self, chat_id: int):
        """Handle /listblacklist bankr word command."""
        words = await self.storage.list_prebankr_word_blacklist()
        
        if not words:
            await self._send_admin_message(
                chat_id,
                "📋 <b>BANKR Word Blacklist vazia</b>\n\nUse <code>/blacklist bankr word xxx</code> para adicionar"
            )
            return
        
        lines = [f"📋 <b>BANKR Word Blacklist ({len(words)})</b>\n"]
        for entry in words:
            lines.append(f"  #{entry['id']}. '{entry['word']}'")
        
        lines.append(f"\n💡 Remover: <code>/removeblacklist bankr word {{id}}</code>")
        await self._send_admin_message(chat_id, "\n".join(lines))
    
    async def _cmd_bankr_word_remove(self, blacklist_id: int, chat_id: int):
        """Handle /removeblacklist bankr word {id} command."""
        removed_word = await self.storage.remove_prebankr_word_blacklist_by_id(blacklist_id)
        
        if removed_word:
            entries = await self.storage.list_prebankr_word_blacklist()
            await self._send_admin_message(
                chat_id,
                f"✅ <b>Removido:</b> #{blacklist_id} '{removed_word}'\n\n📊 Total: <b>{len(entries)}</b>"
            )
        else:
            await self._send_admin_message(
                chat_id,
                f"❌ ID #{blacklist_id} não encontrado"
            )
    
    async def _cmd_help(self, chat_id: int):
        """Handle /help command - show available commands."""
        help_text = """🤖 <b>ADMIN COMMANDS</b>

━━━━━ <b>BANKR BLACKLIST</b> ━━━━━

<b>Handles (Twitter @):</b>
<code>/blacklist bankr @handle1 @handle2</code>
<code>/listblacklist bankr</code>
<code>/removeblacklist bankr {id}</code>

<b>Palavras:</b>
<code>/blacklist bankr word xxx yyy</code>
<code>/listblacklist bankr word</code>
<code>/removeblacklist bankr word {id}</code>

━━━━━━━━━━━━━━━━━━━━

💡 IDs são mostrados no /listblacklist"""
        await self._send_admin_message(chat_id, help_text)
    
    async def _send_admin_message(self, chat_id: int, text: str):
        """Send a message to the admin group."""
        bot_token = os.getenv("TELEGRAM_ADMIN_BOT_TOKEN", "")
        if not bot_token:
            return
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    self.logger.error(f"[ADMIN] Send error: {data}")
                else:
                    self.logger.info(f"[ADMIN] Response sent to {chat_id}")
        except Exception as e:
            self.logger.error(f"[ADMIN] Send message error: {e}")
    
    # =========================================================================
    # ONCHAIN TOKEN HANDLER (OnChain-first flow)
    # =========================================================================
    
    async def _handle_onchain_token(self, event: Dict[str, Any]):
        """
        Handle on-chain TokenCreated event.
        
        OnChain-first flow (same as websocket_async.py):
        1. Token detected on-chain BEFORE Twitter confirms
        2. Check prebankr_data cache for instant display
        3. Send OnchainDeploymentMessage immediately (faster notification!)
        4. Fetch creator info in background
        5. Store in pending_onchain_msgs for Twitter linking
        6. Link with trading executor
        """
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        creator_wallet = event.get("creator_wallet", "")
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        platform = event.get("platform", "X")
        block_number = event.get("block_number", 0)
        
        if not token_address:
            return
        
        # =======================================================================
        # NEW INTERFACE DETECTION - Alert if unknown interface appears
        # =======================================================================
        interface = event.get("interface", "")
        self.logger.info(f"[ONCHAIN] Token {token_symbol} interface='{interface}' platform={platform}")
        
        # Check if this is a NEW interface
        if interface and interface not in self._seen_interfaces:
            # =======================================================================
            # FILTER: Skip @bankrbot deployments (spam from bankrbot deployer)
            # These have description "Deployed using @bankrbot on X"
            # =======================================================================
            metadata = event.get("metadata_parsed", {})
            description = metadata.get("description", "")
            
            if description == "Deployed using @bankrbot on X":
                self.logger.debug(f"[NEW INTERFACE] Skipping @bankrbot deployment: {interface}")
            else:
                await self._send_new_interface_alert(event, interface)
            
            # Add to memory and Redis (always track, even if skipped)
            self._seen_interfaces.add(interface)
            await self.storage.add_seen_interface(interface)
        
        # =======================================================================
        # ROUTE: Clawnch tokens (moltbook, 4claw, moltx) to separate channel
        # FIX: Also route by platform since 4claw tokens may not have interface="Clawnch"
        # =======================================================================
        CLAWNCH_PLATFORMS = {"4claw", "moltbook", "moltx", "clawstr"}
        is_clawnch = interface == "Clawnch" or platform.lower() in CLAWNCH_PLATFORMS
        
        if is_clawnch:
            await self._handle_clawnch_token(event)
            return  # Don't continue to Bankr flow
        
        # =======================================================================
        # FILTER: Only process tokens with valid tweet_id (Twitter platform)
        # Farcaster and other platforms without tweet links are SKIPPED
        # =======================================================================
        if not tweet_id:
            self.logger.debug(f"[ONCHAIN] Skipping {token_symbol} - no tweet link (platform={platform})")
            return
        
        # Dedupe by contract address (persistent)
        if await self.storage.has_notified_contract(token_address):
            self.logger.debug(f"[ONCHAIN] Contract {token_address[:10]}... already notified")
            return
        await self.storage.mark_contract_notified(token_address)
        
        self.logger.info(f"[ONCHAIN] Token created: {token_name} (${token_symbol}) tweet={tweet_id}")
        self.logger.info(f"  Contract: {token_address}")
        self.logger.info(f"  Creator: {creator_wallet[:10]}...")
        self.logger.info(f"  Block: {block_number}")
        if tweet_id:
            self.logger.info(f"  Tweet ID: {tweet_id}")
        
        # Get likes from PRE-BANKR if we have tweet_id
        liked_by = []
        if tweet_id:
            liked_by = await self.storage.get_like_usernames(tweet_id)
            if liked_by:
                self.logger.info(f"  Likes: {len(liked_by)}")
        
        # =======================================================================
        # CHECK PREBANKR_DATA for cached enriched users (INSTANT DISPLAY!)
        # =======================================================================
        cached_data = None
        cached_from_user = None
        cached_to_users = []
        maybe_bug = False  # Flag for thread lookup fallback
        
        if tweet_id:
            cached_data = await self.storage.get_prebankr_data(tweet_id)
            if cached_data:
                self.logger.info(f"  [CACHE HIT] Found PRE-BANKR data for instant display!")
                
                # Get enriched FROM user
                if cached_data.get("enriched_from"):
                    cached_from_user = cached_data["enriched_from"]
                    self.logger.info(f"    FROM: @{cached_from_user.get('username', '?')} (cached)")
                
                # Get enriched TO users
                if cached_data.get("enriched_to"):
                    cached_to_users = [t for t in cached_data["enriched_to"] if t]
                    self.logger.info(f"    TO: {len(cached_to_users)} users (cached)")
            
            else:
                # =======================================================================
                # THREAD LOOKUP FALLBACK: Navigate thread to find cache
                # =======================================================================
                self.logger.info(f"  [THREAD LOOKUP] Cache miss, navigating thread...")
                cached_data = await self._lookup_thread_for_cache(tweet_id, max_levels=3)
                
                if cached_data:
                    # Found cache in parent tweet
                    maybe_bug = True  # Indicate fallback was used
                    self.logger.info(f"  [THREAD] Using cache from parent tweet")
                    
                    if cached_data.get("enriched_from"):
                        cached_from_user = cached_data["enriched_from"]
                        self.logger.info(f"    FROM: @{cached_from_user.get('username', '?')} (from thread)")
                    
                    if cached_data.get("enriched_to"):
                        cached_to_users = [t for t in cached_data["enriched_to"] if t]
                        self.logger.info(f"    TO: {len(cached_to_users)} users (from thread)")
                
                else:
                    # =======================================================================
                    # USERNAME CACHE FALLBACK: Extract username from tweet_url
                    # and lookup individual twitter/alphagate caches
                    # =======================================================================
                    from_username = self._extract_username_from_url(tweet_url)
                    if from_username:
                        self.logger.info(f"  [USERNAME FALLBACK] Extracted @{from_username} from tweet_url")
                        
                        # Lookup individual caches
                        twitter_cache = await self.storage.get_twitter_user(from_username)
                        alphagate_cache = await self.storage.get_alphagate_data(from_username)
                        
                        if twitter_cache or alphagate_cache:
                            self.logger.info(f"  [CACHE HIT] twitter={bool(twitter_cache)} alphagate={bool(alphagate_cache)}")
                            
                            # Build enriched user from individual caches
                            cached_from_user = {"username": from_username}
                            
                            if twitter_cache:
                                cached_from_user.update({
                                    "followers": twitter_cache.get("public_metrics", {}).get("followers_count", 0),
                                    "created_at": twitter_cache.get("created_at", ""),
                                    "verified": twitter_cache.get("verified", False),
                                    "affiliate_label": twitter_cache.get("affiliate_label", ""),
                                    "bio": twitter_cache.get("description", ""),
                                    "wallet": twitter_cache.get("wallet", ""),
                                })
                                self.logger.info(f"    Twitter: {cached_from_user.get('followers', 0)} followers")
                            
                            if alphagate_cache:
                                cached_from_user.update({
                                    "r_score": alphagate_cache.get("r", 0),
                                    "c_score": alphagate_cache.get("c", 0),
                                    "smart_followers": alphagate_cache.get("smart_followers", 0),
                                })
                                self.logger.info(f"    AlphaGate: R={alphagate_cache.get('r', 0)} C={alphagate_cache.get('c', 0)}")
                            
                            maybe_bug = True  # Indicate fallback was used
                        else:
                            self.logger.info(f"  [CACHE MISS] No cached data for @{from_username}")
        
        # Create OnchainDeploymentMessage (faster than waiting for Twitter!)
        onchain_msg = OnchainDeploymentMessage(
            client=self.telegram_deployed,
            token_name=token_name,
            token_symbol=token_symbol,
            contract=token_address,
            creator_wallet=creator_wallet,
            platform=platform,
            tweet_url=tweet_url,
            tweet_id=tweet_id,  # For Like/Buy buttons
            liked_by=liked_by,
        )
        
        # Set maybe_bug flag for indicator
        onchain_msg.maybe_bug = maybe_bug
        
        # =======================================================================
        # USE CACHED DATA FOR INSTANT DISPLAY (like websocket_async.py)
        # =======================================================================
        if cached_from_user:
            onchain_msg.from_user = cached_from_user
            onchain_msg.twitter_loaded = True
            self.logger.info(f"  [INSTANT] Using cached FROM data")
        
        if cached_to_users:
            onchain_msg.to_users = cached_to_users
            self.logger.info(f"  [INSTANT] Using cached TO data ({len(cached_to_users)} users)")
        
        # Step 1: Send initial message WITH cached data if available
        message_id = await onchain_msg.send_initial()
        
        if not message_id:
            self.logger.error("Failed to send ONCHAIN message")
            return
        
        self.logger.info(f"[ONCHAIN] Sent message {message_id} (faster than Twitter!)")
        self.stats["onchain_sent"] += 1
        
        # =======================================================================
        # FETCH CREATOR INFO in background (balance, txs)
        # =======================================================================
        asyncio.create_task(self._fetch_and_update_creator_info(onchain_msg, creator_wallet))
        
        # Store for later update when Twitter confirms
        if tweet_id:
            self._pending_onchain_msgs[tweet_id] = onchain_msg
            # Also store by tweet URL for fallback matching
            if tweet_url:
                self._pending_onchain_msgs[tweet_url] = onchain_msg
            self.logger.info(f"[ONCHAIN] Stored pending message for tweet {tweet_id[:8]}...")
        
        # NOTE: Trading linking is handled by trading_executor worker
        # which subscribes to onchain:token and properly uses link_token_to_buy()
        
        # Limit pending messages cache
        if len(self._pending_onchain_msgs) > 100:
            # Remove oldest half
            keys = list(self._pending_onchain_msgs.keys())
            for k in keys[:50]:
                del self._pending_onchain_msgs[k]
        
        self.stats["messages_processed"] += 1
    
    # =========================================================================
    # CLAWNCH TOKEN HANDLER (moltbook, 4claw, moltx)
    # =========================================================================
    
    async def _handle_clawnch_token(self, event: Dict[str, Any]):
        """
        Handle Clawnch token (moltbook, 4claw, moltx).
        
        Unlike Bankr, Clawnch tokens don't have Twitter data.
        Instead they have agent info from Moltbook/4claw/Moltx platforms.
        """
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        deployer = event.get("msg_sender", "") or event.get("creator_wallet", "")
        block_number = event.get("block_number", 0)
        
        # Get context and metadata
        context = event.get("context_parsed", {})
        metadata = event.get("metadata_parsed", {})
        
        platform = context.get("platform", "unknown")
        agent_name_onchain = context.get("id", "")  # May be technical ID for 4claw
        post_id = context.get("messageId", "")
        description = metadata.get("description", "")
        social_urls = metadata.get("socialMediaUrls", [])
        
        if not token_address:
            return
        
        # Skip if Clawnch client not configured
        if not self.telegram_clawnch:
            self.logger.debug(f"[CLAWNCH] Skipping {token_symbol} - Clawnch notifications disabled")
            return
        
        # Dedupe by contract address
        if await self.storage.has_notified_contract(token_address):
            self.logger.debug(f"[CLAWNCH] Contract {token_address[:10]}... already notified")
            return
        await self.storage.mark_contract_notified(token_address)
        
        self.logger.info(f"[CLAWNCH] {token_name} (${token_symbol}) platform={platform} agent_onchain={agent_name_onchain}")
        self.logger.info(f"  Contract: {token_address}")
        self.logger.info(f"  Deployer: {deployer[:10]}...")
        self.logger.info(f"  Block: {block_number}")
        if post_id:
            self.logger.info(f"  Post ID (messageId): {post_id}")
        
        # =========================================================================
        # FETCH CACHED DATA FROM PRE-CLAWNCH (likes, agent, twitter, alphagate)
        # =========================================================================
        liked_by = []
        preclawnch_data = None
        molt_agent = None
        twitter_data = None
        alphagate_data = None
        x_handle = ""
        token_wallet = ""
        
        # Initialize agent_name with on-chain value as default
        agent_name = agent_name_onchain
        
        if post_id:
            # Get likes from PRE-CLAWNCH using post_id (must match exactly)
            self.logger.debug(f"  [LIKES] Looking up: likes:{post_id}")
            liked_by = await self.storage.get_like_usernames(post_id)
            if liked_by:
                self.logger.info(f"  Likes: {len(liked_by)} - {', '.join(liked_by[:3])}")
            else:
                self.logger.debug(f"  [LIKES] No likes found for post_id={post_id}")
            
            # Get PRE-CLAWNCH token data (saved by sync workers)
            preclawnch_data = await self.storage.get_preclawnch_token(post_id)
            if preclawnch_data:
                x_handle = preclawnch_data.get("x_handle", "")
                token_wallet = preclawnch_data.get("token_wallet", "")
                
                # CRITICAL FIX: Use cached agent_name (real name) over on-chain ID
                # For 4claw, on-chain id is like "4claw_agent_xxx" but real name is in cache
                cached_agent_name = preclawnch_data.get("agent_name", "")
                if cached_agent_name:
                    agent_name = cached_agent_name
                    self.logger.info(f"  [CACHE] Using cached agent_name: '{agent_name}' (on-chain was: '{agent_name_onchain}')")
                
                self.logger.info(f"  [CACHE] PRE-CLAWNCH data: x_handle=@{x_handle}, platform={preclawnch_data.get('platform')}")
                
                # Get MOLT agent data (karma, followers)
                if x_handle:
                    molt_agent = await self.storage.get_molt_agent(x_handle)
                    if molt_agent:
                        self.logger.info(f"  [CACHE] MOLT agent: karma={molt_agent.get('karma')}, followers={molt_agent.get('followers')}")
                    
                    # Get Twitter cache (verified, followers, bio)
                    twitter_data = await self.storage.get_twitter_user(x_handle)
                    if twitter_data:
                        self.logger.info(f"  [CACHE] Twitter: followers={twitter_data.get('followers')}, verified={twitter_data.get('is_blue_verified')}")
                    
                    # Get AlphaGate data (R, C, SF)
                    alphagate_data = await self.storage.get_alphagate_data(x_handle)
                    if alphagate_data:
                        self.logger.info(f"  [CACHE] AlphaGate: R={alphagate_data.get('prev_usernames_count')}, C={alphagate_data.get('contracts_count')}, SF={alphagate_data.get('smart_followers')}")
            else:
                self.logger.warning(f"  [CACHE MISS] No PRE-CLAWNCH data for post_id={post_id}")
        
        # Check for NEW platform (not in known list AND not seen before)
        platform_lower = platform.lower()
        if platform_lower and platform_lower not in self._known_clawnch_platforms:
            if platform_lower not in self._seen_clawnch_platforms:
                # NEW PLATFORM DETECTED! Send special alert
                self._seen_clawnch_platforms.add(platform_lower)
                self.logger.warning(f"[CLAWNCH] 🆕 NEW PLATFORM DETECTED: {platform}")
                
                # Send special alert message
                alert_text = (
                    "✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅\n"
                    "<b>NOVA PLATAFORMA INTEGRADA COM CLAWNCH</b>\n"
                    f"                      <b>PRIMEIRO TOKEN</b>\n"
                    f"                      <code>{platform}</code>\n"
                    "✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅"
                )
                try:
                    await self.telegram_clawnch.send_message(alert_text, parse_mode="HTML")
                except Exception as e:
                    self.logger.error(f"[CLAWNCH] Failed to send new platform alert: {e}")
            else:
                # Platform seen before, but not in known list - just log
                self.logger.debug(f"[CLAWNCH] Platform {platform} seen before (not original)")
        else:
            # Add known platforms to seen set on first encounter
            self._seen_clawnch_platforms.add(platform_lower)
        
        # Create ClawnchDeploymentMessage with enriched data
        msg = ClawnchDeploymentMessage(
            client=self.telegram_clawnch,
            token_name=token_name,
            token_symbol=token_symbol,
            contract=token_address,
            deployer=deployer,
            platform=platform,
            agent_name=agent_name,
            post_id=post_id,
            description=description,
            social_urls=social_urls,
            # Enriched data from PRE-CLAWNCH cache
            liked_by=liked_by,
            token_wallet=token_wallet,
            x_handle=x_handle,
            molt_agent=molt_agent,
            twitter_data=twitter_data,
            alphagate_data=alphagate_data,
        )
        
        # Send initial message
        message_id = await msg.send_initial()
        
        if message_id:
            self.logger.info(f"[CLAWNCH] Sent message {message_id}")
            self.stats["clawnch_sent"] += 1
            
            # Fetch deployer info in background
            asyncio.create_task(self._fetch_and_update_clawnch_info(msg, deployer))
        else:
            self.logger.error(f"[CLAWNCH] Failed to send message for {token_symbol}")
        
        self.stats["messages_processed"] += 1
    
    async def _fetch_and_update_clawnch_info(
        self,
        msg: ClawnchDeploymentMessage,
        deployer_wallet: str,
    ):
        """Fetch deployer info via RPC and update Clawnch message."""
        if not deployer_wallet or not self._session:
            return
        
        rpc_url = os.getenv("ANKR_RPC_HTTP", "")
        if not rpc_url:
            return
        
        try:
            # Fetch balance and tx count IN PARALLEL
            async def fetch_balance():
                resp = await self._session.post(rpc_url, json={
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [deployer_wallet, "latest"],
                    "id": 1
                })
                data = await resp.json()
                hex_balance = data.get("result", "0x0")
                wei = int(hex_balance, 16)
                eth = wei / 1e18
                return f"{eth:.4f} ETH"
            
            async def fetch_tx_count():
                resp = await self._session.post(rpc_url, json={
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionCount",
                    "params": [deployer_wallet, "latest"],
                    "id": 2
                })
                data = await resp.json()
                hex_count = data.get("result", "0x0")
                return str(int(hex_count, 16))
            
            balance, txs = await asyncio.gather(
                fetch_balance(),
                fetch_tx_count(),
                return_exceptions=True
            )
            
            if isinstance(balance, str) and isinstance(txs, str):
                await msg.update_deployer_info(balance=balance, txs=txs)
                self.logger.debug(f"[CLAWNCH] Updated deployer info: {balance}, {txs} txs")
        
        except Exception as e:
            self.logger.error(f"[CLAWNCH] Error fetching deployer info: {e}")
    
    async def _send_new_interface_alert(self, event: Dict[str, Any], interface: str):
        """
        Send alert when a NEW, unknown interface is detected.
        Sends full context and metadata to dedicated channel.
        """
        if not self.telegram_newinterface:
            self.logger.warning(f"[NEW INTERFACE] Detected '{interface}' but no channel configured")
            return
        
        import json
        from datetime import datetime
        
        # Extract all data
        token_address = event.get("token_address", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        deployer = event.get("msg_sender", "") or event.get("creator_wallet", "")
        block_number = event.get("block_number", 0)
        platform = event.get("platform", "")
        
        # Get full context and metadata
        context = event.get("context_parsed", {})
        metadata = event.get("metadata_parsed", {})
        
        # Format JSON with indentation
        context_json = json.dumps(context, indent=2, ensure_ascii=False) if context else "{}"
        metadata_json = json.dumps(metadata, indent=2, ensure_ascii=False) if metadata else "{}"
        
        # Build comprehensive message
        lines = [
            "🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕",
            f"<b>NOVA INTERFACE DETECTADA!</b>",
            "🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕🆕",
            "",
            f"🏷️ <b>Interface:</b> <code>{interface}</code>",
            "",
            "━━━━━━━🪙 <b>TOKEN</b>━━━━━━━",
            "",
            f"📛 <b>Name:</b> {token_name}",
            f"💲 <b>Symbol:</b> ${token_symbol}",
            f"📍 <b>Contract:</b> <code>{token_address}</code>",
            f"👤 <b>Deployer:</b> <code>{deployer}</code>",
            f"🔢 <b>Block:</b> {block_number}",
            f"🌐 <b>Platform:</b> {platform}",
            "",
            "━━━━━━━📋 <b>CONTEXT</b>━━━━━━━",
            "",
            f"<code>{context_json}</code>",
            "",
            "━━━━━━━📝 <b>METADATA</b>━━━━━━━",
            "",
            f"<code>{metadata_json}</code>",
            "",
            "━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"⏰ Detectado em: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        ]
        
        message = "\n".join(lines)
        
        # Build buttons with contract links
        buttons = [
            {"text": "Sigma", "url": f"https://t.me/SigmaTrading9_bot?start=xqquante-{token_address}"},
            {"text": "GMGN", "url": f"https://gmgn.ai/base/token/qquante_{token_address}"},
            {"text": "VIP", "url": f"https://t.me/SigmaTradingVIP_bot?start=xqquante-{token_address}"},
            {"text": "Clanker", "url": f"https://www.clanker.world/clanker/{token_address}"},
        ]
        
        try:
            self.logger.warning(f"[NEW INTERFACE] 🆕 Sending alert for: {interface}")
            # Convert buttons list to Telegram inline_keyboard format
            reply_markup = {"inline_keyboard": [buttons]}
            await self.telegram_newinterface.send_message(message, parse_mode="HTML", reply_markup=reply_markup)
            self.logger.info(f"[NEW INTERFACE] Alert sent successfully for: {interface}")
        except Exception as e:
            self.logger.error(f"[NEW INTERFACE] Failed to send alert: {e}")
    
    async def _fetch_and_update_creator_info(
        self,
        onchain_msg: OnchainDeploymentMessage,
        creator_wallet: str,
    ):
        """Fetch creator info via RPC and update message (PARALLEL calls)."""
        if not creator_wallet or not self._session:
            return
        
        rpc_url = os.getenv("ANKR_RPC_HTTP", "")
        if not rpc_url:
            return
        
        try:
            # Fetch balance and tx count IN PARALLEL
            async def fetch_balance():
                resp = await self._session.post(rpc_url, json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getBalance",
                    "params": [creator_wallet, "latest"]
                })
                return await resp.json()
            
            async def fetch_tx_count():
                resp = await self._session.post(rpc_url, json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_getTransactionCount",
                    "params": [creator_wallet, "latest"]
                })
                return await resp.json()
            
            # Execute both in parallel
            balance_data, tx_data = await asyncio.gather(
                fetch_balance(),
                fetch_tx_count(),
                return_exceptions=True,
            )
            
            # Handle exceptions from gather
            if isinstance(balance_data, Exception):
                self.logger.debug(f"[ONCHAIN] Balance fetch error: {balance_data}")
                balance_data = {"result": "0x0"}
            if isinstance(tx_data, Exception):
                self.logger.debug(f"[ONCHAIN] Tx count fetch error: {tx_data}")
                tx_data = {"result": "0x0"}
            
            balance_hex = balance_data.get("result", "0x0")
            balance_wei = int(balance_hex, 16)
            balance_eth = balance_wei / 1e18
            
            tx_hex = tx_data.get("result", "0x0")
            tx_count = int(tx_hex, 16)
            
            # Format balance
            if balance_eth >= 1:
                balance_str = f"{balance_eth:.2f} ETH"
            else:
                balance_str = f"{balance_eth:.4f} ETH"
            
            # Update message
            await onchain_msg.update_creator_info(
                balance=balance_str,
                txs=str(tx_count),
            )
            
            self.logger.info(f"[ONCHAIN] Creator info: {balance_str}, {tx_count} txs")
            self.stats["edits_made"] += 1
            
        except Exception as e:
            self.logger.debug(f"[ONCHAIN] Creator info error: {e}")
    
    # =========================================================================
    # THREAD LOOKUP FALLBACK (for cache miss on ONCHAIN)
    # =========================================================================
    
    async def _fetch_tweet_by_id(self, tweet_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch tweet by ID via Twitter API.
        
        Returns tweet with: inReplyToId, author, entities, conversationId
        """
        if not self._session or not self.twitter_api_key:
            return None
        
        url = "https://api.twitterapi.io/twitter/tweets"
        params = {"tweet_ids": tweet_id}
        headers = {"X-API-Key": self.twitter_api_key}
        
        try:
            async with self._session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    tweets = data.get("tweets", [])
                    if tweets and len(tweets) > 0:
                        return tweets[0]
        except Exception as e:
            self.logger.debug(f"[THREAD] fetch_tweet_by_id error: {e}")
        
        return None
    
    def _extract_username_from_url(self, tweet_url: str) -> Optional[str]:
        """
        Extract Twitter username from tweet URL.
        
        Examples:
            https://twitter.com/sekoweed/status/123 -> sekoweed
            https://x.com/elonmusk/status/456 -> elonmusk
        
        Returns:
            Username (without @) or None if extraction fails
        """
        if not tweet_url:
            return None
        
        try:
            # Parse URL: https://twitter.com/USERNAME/status/ID
            # or https://x.com/USERNAME/status/ID
            import re
            match = re.search(r'(?:twitter\.com|x\.com)/([^/]+)/status/', tweet_url)
            if match:
                username = match.group(1).lower().lstrip('@')
                # Skip non-user paths
                if username not in ['i', 'intent', 'share', 'search', 'hashtag']:
                    return username
        except Exception:
            pass
        
        return None
    
    async def _lookup_thread_for_cache(
        self, 
        tweet_id: str, 
        max_levels: int = 3
    ) -> Optional[Dict[str, Any]]:
        """
        Navigate thread upwards to find PRE-BANKR cache.
        
        Only checks cache at each level, does NOT extract FROM/TO from tweets.
        
        Args:
            tweet_id: ID of the deployment tweet
            max_levels: Maximum levels to navigate up (default 3)
        
        Returns:
            cached_data dict with enriched_from/enriched_to if found, None otherwise
        """
        current_id = tweet_id
        
        for level in range(max_levels):
            # 1. Check cache for this tweet ID
            cached = await self.storage.get_prebankr_data(current_id)
            if cached:
                self.logger.info(f"  [THREAD] Found cache at level {level}: {current_id[:12]}...")
                return cached
            
            # 2. Fetch tweet to get inReplyToId
            tweet = await self._fetch_tweet_by_id(current_id)
            if not tweet:
                self.logger.debug(f"  [THREAD] Could not fetch tweet at level {level}")
                break
            
            # 3. Navigate to parent
            parent_id = tweet.get("inReplyToId")
            if not parent_id:
                self.logger.debug(f"  [THREAD] No inReplyToId at level {level}")
                break
            
            current_id = parent_id
        
        self.logger.info(f"  [THREAD] No cache found in thread (checked {max_levels} levels)")
        return None
    
    # =========================================================================
    # PRE-BANKR HANDLER (Progressive)
    # =========================================================================
    
    async def _handle_prebankr(self, event: Dict[str, Any]):
        """
        Handle PRE-BANKR event with progressive message updates.
        
        Same flow as handle_prebankr_progressive() in websocket_async.py
        Caches enriched data in Redis for instant DEPLOYED display.
        """
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        from_username = event.get("from_username", "")
        to_usernames = event.get("to_usernames", []) or []
        token_name = event.get("token_name")
        token_symbol = event.get("token_symbol")
        raw_tweet = event.get("raw_tweet", {})
        
        # GLOBAL DEDUPE using Redis (works across multiple instances)
        dedupe_key = f"prebankr_processed:{tweet_id}"
        is_new = await self.redis.setnx(dedupe_key, "1")
        if not is_new:
            self.logger.debug(f"[PRE-BANKR] Tweet {tweet_id[:8]}... already processed")
            return
        await self.redis.expire(dedupe_key, 3600)  # 1 hour TTL
        
        # Also keep local cache for fast path
        self._processed.add(tweet_id)
        if len(self._processed) > 5000:
            self._processed = set(list(self._processed)[-2500:])
        
        # =======================================================================
        # BLACKLIST CHECK - Skip if FROM user is blacklisted
        # =======================================================================
        if from_username and await self.storage.is_prebankr_blacklisted(from_username):
            self.logger.info(f"[PRE-BANKR] ⛔ BLACKLISTED @{from_username} - skipping {token_symbol}")
            return
        
        # Get tweet text early for word blacklist check
        tweet_text = raw_tweet.get("text", "")
        
        # =======================================================================
        # WORD BLACKLIST CHECK - Skip if token name/symbol/text contains word
        # =======================================================================
        # Build combined text to check
        text_to_check = f"{token_name} {token_symbol} {tweet_text}"
        matched_word = await self.storage.check_prebankr_word_blacklist(text_to_check)
        if matched_word:
            self.logger.info(f"[PRE-BANKR] ⛔ WORD BLACKLISTED '{matched_word}' - skipping {token_symbol}")
            return
        
        self.logger.info(f"[PRE-BANKR] {token_name} (${token_symbol}) from @{from_username}")
        
        # Extract wallet_map from raw_tweet mentions (for ENS lookup)
        wallet_map = {}
        all_mentions = raw_tweet.get("entities", {}).get("user_mentions", [])
        for mention in all_mentions:
            username = mention.get("screen_name", "")
            wallet = mention.get("wallet", "")  # If present
            if username and wallet:
                wallet_map[username.lower()] = wallet
        
        # Also check parsed wallet data
        if event.get("wallet_map"):
            wallet_map.update(event.get("wallet_map", {}))
        
        # Initialize prebankr_data cache in Redis (like websocket_async.py)
        await self.storage.save_prebankr_data(
            tweet_id=tweet_id,
            from_username=from_username,
            to_usernames=to_usernames,
            token_name=token_name or "",
            token_symbol=token_symbol or "",
            wallet_map=wallet_map,
        )
        
        # Create PRE-BANKR message (same as websocket_async.py)
        pm = PreBankrMessage(
            client=self.telegram_prebankr,
            from_username=from_username,
            to_usernames=to_usernames,
            tweet_url=tweet_url,
            tweet_text=tweet_text,
            token_name=token_name,
            token_symbol=token_symbol,
            tweet_id=tweet_id,
        )
        
        # Step 1: Send initial message (with loading indicators)
        message_id = await pm.send_initial()
        
        if not message_id:
            self.logger.error("Failed to send PRE-BANKR message")
            return
        
        self.logger.info(f"[PRE-BANKR] Sent message {message_id}")
        self.stats["prebankr_sent"] += 1
        
        # Save token info for BUY button
        token_info = TokenInfo(
            tweet_id=tweet_id,
            token_symbol=token_symbol or "",
            token_name=token_name or "",
            tweet_url=tweet_url,
            from_username=from_username,
            to_usernames=to_usernames,
            prebankr_at=datetime.now().isoformat(),
        )
        await self.storage.save_token_info(token_info)
        
        # Track enriched data to cache
        enriched_from_data = None
        enriched_to_data = [None] * len(to_usernames)
        
        # Step 2: Enrich users in parallel and update progressively
        async def enrich_from():
            """Enrich FROM user and update message."""
            nonlocal enriched_from_data
            try:
                enriched = await self._enrich_user(
                    from_username,
                    wallet=wallet_map.get(from_username.lower(), ""),
                )
                await pm.update_from_user(enriched)
                self.stats["edits_made"] += 1
                
                # Store for cache
                enriched_from_data = enriched.copy()
                
                # Check if FROM user is a MOLT Agent
                molt_agent = await self.storage.get_molt_agent(from_username)
                if molt_agent:
                    enriched["is_molt_agent"] = True
                    enriched["molt_verified"] = molt_agent.get("is_verified", False)
                    enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                    enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                    enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                    enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                    enriched_from_data.update({
                        "is_molt_agent": True,
                        "molt_verified": molt_agent.get("is_verified", False),
                        "molt_agent_name": molt_agent.get("agent_name", ""),
                        "followers": molt_agent.get("followers", enriched.get("followers", 0)),
                        "vip_followers": molt_agent.get("vip_followers", []),
                        "passes_molt_filter": molt_agent.get("passes_kol_filter", False),
                    })
                    self.logger.info(f"[MOLT] FROM @{from_username} is MOLT agent: {molt_agent.get('agent_name')}")
                
                # Smart followers count already comes from _fetch_alphagate_child (key_followers_count)
                # Note: /api/v1/user/smartfollowers endpoint was removed from AlphaGate API
            except Exception as e:
                self.logger.warning(f"Enrich FROM error: {e}")
        
        async def enrich_to(index: int, username: str):
            """Enrich TO user and update message."""
            try:
                enriched = await self._enrich_user(
                    username,
                    wallet=wallet_map.get(username.lower(), ""),
                )
                
                # Check if TO user is a MOLT Agent
                molt_agent = await self.storage.get_molt_agent(username)
                if molt_agent:
                    enriched["is_molt_agent"] = True
                    enriched["molt_verified"] = molt_agent.get("is_verified", False)
                    enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                    enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                    enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                    enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                    self.logger.info(f"[MOLT] TO @{username} is MOLT agent: {molt_agent.get('agent_name')}")
                
                await pm.update_to_user(index, enriched)
                self.stats["edits_made"] += 1
                
                # Store for cache
                enriched_to_data[index] = enriched.copy()
                
                # Smart followers count already comes from _fetch_alphagate_child (key_followers_count)
                # Note: /api/v1/user/smartfollowers endpoint was removed from AlphaGate API
            except Exception as e:
                self.logger.warning(f"Enrich TO[{index}] error: {e}")
        
        # Run all enrichments in parallel
        tasks = [enrich_from()]
        for i, username in enumerate(to_usernames):
            tasks.append(enrich_to(i, username))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update Redis cache with enriched data (for DEPLOYED to use)
        # IMPORTANT: Keep None values to preserve indices! Don't filter!
        await self.storage.update_prebankr_enriched(
            tweet_id=tweet_id,
            enriched_from=enriched_from_data,
            enriched_to=enriched_to_data,  # Keep Nones to preserve indices
        )
        
        # Step 3: Mark complete
        await pm.mark_complete()
        
        self.logger.info(f"[PRE-BANKR] Complete (cached for DEPLOYED)")
        self.stats["messages_processed"] += 1
    
    # =========================================================================
    # DEPLOYED HANDLER (Progressive)
    # =========================================================================
    
    async def _handle_deployed(self, event: Dict[str, Any]):
        """
        Handle DEPLOYED event with progressive message updates.
        
        Same flow as handle_deployment_progressive() in websocket_async.py:
        1. Check if on-chain message already exists (sent faster via WSS)
        2. If exists: update it with Twitter data
        3. If not: send new message (fallback to old flow)
        
        Uses cached PRE-BANKR data for instant display when available.
        """
        tweet_id = event.get("tweet_id", "")
        tweet_url = event.get("tweet_url", "")
        original_tweet_id = event.get("original_tweet_id", "")
        token_name = event.get("token_name", "")
        token_symbol = event.get("token_symbol", "")
        contract_address = event.get("contract_address", "")
        from_username = event.get("from_username", "")
        to_usernames = event.get("to_usernames", []) or []
        raw_tweet = event.get("raw_tweet", {})
        
        # GLOBAL DEDUPE using Redis (prevents duplicate messages)
        dedupe_key = f"deployed_processed:{tweet_id}"
        is_new = await self.redis.setnx(dedupe_key, "1")
        if not is_new:
            self.logger.debug(f"[DEPLOYED] Tweet {tweet_id[:8]}... already processed")
            return
        await self.redis.expire(dedupe_key, 3600)  # 1 hour TTL
        
        # Check for cached PRE-BANKR data (like websocket_async.py prebankr_data)
        cached_data = None
        wallet_map = {}
        if original_tweet_id:
            cached_data = await self.storage.get_prebankr_data(original_tweet_id)
            if cached_data:
                self.logger.info(f"  [CACHE HIT] Using PRE-BANKR data for instant display")
                # Use cached FROM/TO if available (Agent's classification is more accurate)
                if cached_data.get("from_username"):
                    from_username = cached_data["from_username"]
                if cached_data.get("to_usernames"):
                    to_usernames = cached_data["to_usernames"]
                if cached_data.get("wallet_map"):
                    wallet_map = cached_data["wallet_map"]
        
        # =======================================================================
        # CHECK FOR PENDING ON-CHAIN MESSAGE (OnChain-first flow)
        # =======================================================================
        onchain_msg = None
        
        # Try direct match by original tweet ID
        if original_tweet_id and original_tweet_id in self._pending_onchain_msgs:
            onchain_msg = self._pending_onchain_msgs[original_tweet_id]
            # Remove all references to this message
            keys_to_remove = [k for k, v in self._pending_onchain_msgs.items() if v is onchain_msg]
            for k in keys_to_remove:
                del self._pending_onchain_msgs[k]
            self.logger.info(f"[DEPLOYED] Found pending on-chain message by tweet ID")
        
        # Fallback: search by URL contains
        if not onchain_msg:
            for stored_url, msg in list(self._pending_onchain_msgs.items()):
                if original_tweet_id and stored_url and original_tweet_id in stored_url:
                    onchain_msg = msg
                    keys_to_remove = [k for k, v in self._pending_onchain_msgs.items() if v is onchain_msg]
                    for k in keys_to_remove:
                        del self._pending_onchain_msgs[k]
                    self.logger.info(f"[DEPLOYED] Found pending on-chain message by URL match")
                    break
        
        # =======================================================================
        # CASE 1: UPDATE EXISTING ON-CHAIN MESSAGE
        # =======================================================================
        if onchain_msg:
            self.logger.info(f"[DEPLOYED] Updating existing on-chain message with Twitter data")
            
            # Update bankr tweet URL
            await onchain_msg.update_bankr_tweet_url(tweet_url)
            
            # Get cached enriched data if available
            cached_from = cached_data.get("enriched_from") if cached_data else None
            cached_to = cached_data.get("enriched_to", []) if cached_data else []
            
            # Enrich users and update the EXISTING on-chain message
            async def enrich_from():
                try:
                    if cached_from:
                        # Check MOLT even for cached data
                        cached_data = cached_from.copy()
                        molt_agent = await self.storage.get_molt_agent(from_username)
                        if molt_agent:
                            cached_data["is_molt_agent"] = True
                            cached_data["molt_verified"] = molt_agent.get("is_verified", False)
                            cached_data["molt_agent_name"] = molt_agent.get("agent_name", "")
                            cached_data["followers"] = molt_agent.get("followers", cached_data.get("followers", 0))
                            cached_data["vip_followers"] = molt_agent.get("vip_followers", [])
                            cached_data["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                        await onchain_msg.update_from_user(cached_data)
                        self.logger.info(f"  FROM @{from_username} loaded from cache")
                    else:
                        enriched = await self._enrich_user(
                            from_username,
                            wallet=wallet_map.get(from_username.lower(), ""),
                        )
                        
                        # Check if FROM user is a MOLT Agent
                        molt_agent = await self.storage.get_molt_agent(from_username)
                        if molt_agent:
                            enriched["is_molt_agent"] = True
                            enriched["molt_verified"] = molt_agent.get("is_verified", False)
                            enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                            enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                            enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                            enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                            self.logger.info(f"[MOLT] FROM @{from_username} is MOLT agent")
                        
                        await onchain_msg.update_from_user(enriched)
                        
                        # Smart followers count already comes from _fetch_alphagate_child
                    self.stats["edits_made"] += 1
                except Exception as e:
                    self.logger.warning(f"Enrich FROM error: {e}")
            
            async def enrich_to(index: int, username: str):
                try:
                    if index < len(cached_to) and cached_to[index]:
                        # Check MOLT even for cached data
                        cached_data = cached_to[index].copy()
                        molt_agent = await self.storage.get_molt_agent(username)
                        if molt_agent:
                            cached_data["is_molt_agent"] = True
                            cached_data["molt_verified"] = molt_agent.get("is_verified", False)
                            cached_data["molt_agent_name"] = molt_agent.get("agent_name", "")
                            cached_data["followers"] = molt_agent.get("followers", cached_data.get("followers", 0))
                            cached_data["vip_followers"] = molt_agent.get("vip_followers", [])
                            cached_data["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                        await onchain_msg.update_to_user(index, cached_data)
                        self.logger.info(f"  TO @{username} loaded from cache")
                    else:
                        enriched = await self._enrich_user(
                            username,
                            wallet=wallet_map.get(username.lower(), ""),
                        )
                        
                        # Check if TO user is a MOLT Agent
                        molt_agent = await self.storage.get_molt_agent(username)
                        if molt_agent:
                            enriched["is_molt_agent"] = True
                            enriched["molt_verified"] = molt_agent.get("is_verified", False)
                            enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                            enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                            enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                            enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                            self.logger.info(f"[MOLT] TO @{username} is MOLT agent")
                        
                        await onchain_msg.update_to_user(index, enriched)
                        
                        # Smart followers count already comes from _fetch_alphagate_child
                    self.stats["edits_made"] += 1
                except Exception as e:
                    self.logger.warning(f"Enrich TO[{index}] error: {e}")
            
            # Run all enrichments in parallel
            tasks = [enrich_from()]
            for i, username in enumerate(to_usernames):
                tasks.append(enrich_to(i, username))
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            self.stats["onchain_updated"] += 1
            self.stats["messages_processed"] += 1
            self.logger.info(f"[DEPLOYED] On-chain message updated with Twitter data")
            return
        
        # =======================================================================
        # CASE 2: NO ON-CHAIN MESSAGE - CREATE NEW PROGRESSIVE MESSAGE (fallback)
        # =======================================================================
        
        # Dedupe by contract using Redis (persistent across restarts)
        if contract_address:
            if await self.storage.has_notified_contract(contract_address):
                self.logger.info(f"[DEPLOYED] Contract {contract_address[:10]}... already notified - skipping")
                return
            await self.storage.mark_contract_notified(contract_address)
        
        self.logger.info(f"[DEPLOYED] {token_name} (${token_symbol})")
        self.logger.info(f"  Contract: {contract_address}")
        self.logger.info(f"  FROM: @{from_username} | TO: {to_usernames}")
        
        # Get likes from storage if we have original_tweet_id
        liked_by = []
        if original_tweet_id:
            liked_by = await self.storage.get_like_usernames(original_tweet_id)
            if liked_by:
                self.logger.info(f"  Likes: {len(liked_by)}")
        
        # Build original tweet URL
        original_tweet_url = ""
        if original_tweet_id:
            original_tweet_url = f"https://x.com/i/status/{original_tweet_id}"
        
        # Get tweet text
        tweet_text = raw_tweet.get("text", "")
        
        # =======================================================================
        # Get cached enriched data BEFORE creating message (INSTANT DISPLAY!)
        # =======================================================================
        cached_from = cached_data.get("enriched_from") if cached_data else None
        cached_to = cached_data.get("enriched_to", []) if cached_data else []
        
        if cached_from:
            self.logger.info(f"  [INSTANT] FROM @{from_username} pre-loaded from cache")
        if cached_to:
            self.logger.info(f"  [INSTANT] {len(cached_to)} TO users pre-loaded from cache")
        
        # Create Progressive Message with pre-loaded cache data
        # This sends the message ALREADY with enriched data (no "Loading...")
        pm = ProgressiveMessage(
            client=self.telegram_deployed,
            token_name=token_name,
            token_symbol=token_symbol,
            contract=contract_address,
            from_username=from_username,
            to_usernames=to_usernames,
            tweet_url=tweet_url,
            tweet_text=tweet_text,
            fee_recipient=event.get("fee_recipient"),
            fee_wallet=None,
            liked_by=liked_by,
            original_tweet_url=original_tweet_url,
            preloaded_from=cached_from,  # Pre-loaded cache - no "Loading..."!
            preloaded_to=cached_to,      # Pre-loaded cache - no "Loading..."!
        )
        
        # Step 1: Send initial message (already has cached data if available!)
        message_id = await pm.send_initial()
        
        if not message_id:
            self.logger.error("Failed to send DEPLOYED message")
            return
        
        self.logger.info(f"[DEPLOYED] Sent message {message_id}")
        self.stats["deployed_sent"] += 1
        
        # Step 2: Only enrich users that were NOT pre-loaded from cache
        async def enrich_from():
            """Enrich FROM user and update message (only if not pre-loaded)."""
            try:
                # If already pre-loaded, skip enrichment entirely
                if cached_from:
                    return
                
                # Otherwise enrich from scratch
                enriched = await self._enrich_user(
                    from_username,
                    wallet=wallet_map.get(from_username.lower(), ""),
                )
                
                # Check if FROM user is a MOLT Agent
                molt_agent = await self.storage.get_molt_agent(from_username)
                if molt_agent:
                    enriched["is_molt_agent"] = True
                    enriched["molt_verified"] = molt_agent.get("is_verified", False)
                    enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                    enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                    enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                    enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                    self.logger.info(f"[MOLT] FROM @{from_username} is MOLT agent")
                
                await pm.update_from_user(enriched)
                self.stats["edits_made"] += 1
                
                # Smart followers count already comes from _fetch_alphagate_child
            except Exception as e:
                self.logger.warning(f"Enrich FROM error: {e}")
        
        async def enrich_to(index: int, username: str):
            """Enrich TO user and update message (only if not pre-loaded)."""
            try:
                # If already pre-loaded, skip enrichment entirely
                if index < len(cached_to) and cached_to[index]:
                    return
                
                # Otherwise enrich from scratch
                enriched = await self._enrich_user(
                    username,
                    wallet=wallet_map.get(username.lower(), ""),
                )
                
                # Check if TO user is a MOLT Agent
                molt_agent = await self.storage.get_molt_agent(username)
                if molt_agent:
                    enriched["is_molt_agent"] = True
                    enriched["molt_verified"] = molt_agent.get("is_verified", False)
                    enriched["molt_agent_name"] = molt_agent.get("agent_name", "")
                    enriched["followers"] = molt_agent.get("followers", enriched.get("followers", 0))
                    enriched["vip_followers"] = molt_agent.get("vip_followers", [])
                    enriched["passes_molt_filter"] = molt_agent.get("passes_kol_filter", False)
                    self.logger.info(f"[MOLT] TO @{username} is MOLT agent")
                
                await pm.update_to_user(index, enriched)
                self.stats["edits_made"] += 1
                
                # Smart followers count already comes from _fetch_alphagate_child
            except Exception as e:
                self.logger.warning(f"Enrich TO[{index}] error: {e}")
        
        # Run enrichments only for non-cached users
        tasks = [enrich_from()]
        for i, username in enumerate(to_usernames):
            tasks.append(enrich_to(i, username))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Step 3: Mark complete
        await pm.mark_complete()
        
        cache_status = "instant (from cache)" if cached_from else "fresh enrichment"
        self.logger.info(f"[DEPLOYED] Complete ({cache_status})")
        self.stats["messages_processed"] += 1
    
    # =========================================================================
    # USER ENRICHMENT (Same as websocket_async.py)
    # =========================================================================
    
    async def _enrich_user(
        self,
        username: str,
        wallet: str = "",
    ) -> Dict[str, Any]:
        """
        Enrich a single user with PARALLEL API calls.
        
        Priority:
        1. Check local memory cache (fastest)
        2. Check Redis cache from user_enricher (shared)
        3. Call APIs directly (slowest, last resort)
        """
        username = username.lstrip("@")
        cache_key = username.lower()
        
        # Check local memory cache (fastest)
        if cache_key in self._user_cache:
            self.logger.debug(f"[CACHE] Memory hit: @{username}")
            return self._user_cache[cache_key].copy()
        
        # Check Redis cache from user_enricher (shared across workers)
        try:
            redis_cached = await self.storage.get_enriched_user(username)
            if redis_cached:
                # Convert EnrichedUser to dict
                user_data = redis_cached.to_redis()
                self._user_cache[cache_key] = user_data
                self.logger.debug(f"[CACHE] Redis hit: @{username}")
                return user_data.copy()
        except Exception as e:
            self.logger.debug(f"[CACHE] Redis error: {e}")
        
        self.stats["users_enriched"] += 1
        
        # Base user data
        user_data = {
            "username": username,
            "user_id": "",
            "name": username,
            "followers": 0,
            "following": 0,
            "is_blue_verified": False,
            "verified_type": None,
            "created_at": "",
            "bio": "",
            "ens": "",
            "wallet": wallet or "",
            "smart_followers": 0,
            "prev_usernames_count": 0,
            "contracts_count": 0,
            "affiliate_label": "",
        }
        
        # =====================================================================
        # CHECK CACHES FIRST (24h TTL each)
        # =====================================================================
        twitter_cached = False
        alphagate_cached = False
        
        # Check Twitter cache
        try:
            twitter_cache = await self.storage.get_twitter_user(username)
            if twitter_cache:
                self.logger.debug(f"[CACHE] Twitter HIT @{username}")
                user_data.update({
                    "user_id": twitter_cache.get("user_id", ""),
                    "name": twitter_cache.get("name", username),
                    "followers": twitter_cache.get("followers", 0),
                    "following": twitter_cache.get("following", 0),
                    "is_blue_verified": twitter_cache.get("is_blue_verified", False),
                    "verified_type": twitter_cache.get("verified_type"),
                    "created_at": twitter_cache.get("created_at", ""),
                    "bio": twitter_cache.get("bio", ""),
                    "affiliate_label": twitter_cache.get("affiliate_label", ""),
                })
                twitter_cached = True
        except Exception as e:
            self.logger.debug(f"[CACHE] Twitter check error: {e}")
        
        # Check AlphaGate cache
        try:
            alphagate_cache = await self.storage.get_alphagate_data(username)
            if alphagate_cache:
                self.logger.debug(f"[CACHE] AlphaGate HIT @{username}")
                user_data.update({
                    "prev_usernames_count": alphagate_cache.get("prev_usernames_count", 0),
                    "contracts_count": alphagate_cache.get("contracts_count", 0),
                    "smart_followers": alphagate_cache.get("smart_followers", 0),
                })
                alphagate_cached = True
        except Exception as e:
            self.logger.debug(f"[CACHE] AlphaGate check error: {e}")
        
        # =====================================================================
        # CALL APIs ONLY FOR MISSING DATA
        # =====================================================================
        tasks = []
        
        if not twitter_cached:
            # Need to call Twitter API
            tasks.append(("twitter", self._fetch_twitter_user(username)))
            tasks.append(("twitter_about", self._fetch_twitter_about(username)))
        
        if not alphagate_cached:
            # Need to call AlphaGate API (will check its own cache too)
            tasks.append(("alphagate_child", self._fetch_alphagate_child(username)))
        
        # ENS lookup (if wallet provided and not cached)
        if wallet and not user_data.get("ens"):
            tasks.append(("ens", self._resolve_ens(wallet)))
        
        # Execute in parallel
        if tasks:
            results = await asyncio.gather(
                *[t[1] for t in tasks],
                return_exceptions=True,
            )
            
            # Process results
            for i, (name, _) in enumerate(tasks):
                result = results[i]
                
                if isinstance(result, Exception):
                    self.logger.debug(f"API error ({name}): {result}")
                    continue
                
                if not result:
                    continue
                
                if name == "twitter":
                    user_data.update({
                        "user_id": str(result.get("id", "")),
                        "name": result.get("name", username),
                        "followers": result.get("followers", 0),
                        "following": result.get("following", 0),
                        "is_blue_verified": result.get("isBlueVerified", False),
                        "verified_type": result.get("verifiedType"),
                        "created_at": result.get("createdAt", ""),
                        "bio": result.get("description", ""),
                        "profile_image_url": result.get("profilePicture", ""),
                    })
                    # Store affiliate_label placeholder to be updated
                    twitter_api_data = result
                
                elif name == "twitter_about":
                    user_data["affiliate_label"] = result.get("affiliate_label", "")
                
                elif name == "alphagate_child":
                    user_data["prev_usernames_count"] = result.get("prev_usernames_count", 0)
                    user_data["contracts_count"] = result.get("contracts_count", 0)
                    user_data["smart_followers"] = result.get("smart_followers", 0)
                
                elif name == "ens":
                    user_data["ens"] = result
                    user_data["wallet"] = wallet
            
            # Save Twitter data to cache (if we called the API)
            if not twitter_cached and user_data.get("user_id"):
                try:
                    await self.storage.save_twitter_user(
                        username=username,
                        user_id=user_data.get("user_id", ""),
                        name=user_data.get("name", ""),
                        followers=user_data.get("followers", 0),
                        following=user_data.get("following", 0),
                        is_blue_verified=user_data.get("is_blue_verified", False),
                        verified_type=user_data.get("verified_type", ""),
                        created_at=user_data.get("created_at", ""),
                        bio=user_data.get("bio", ""),
                        affiliate_label=user_data.get("affiliate_label", ""),
                    )
                    self.logger.debug(f"[CACHE] Twitter SAVED @{username}")
                except Exception as e:
                    self.logger.debug(f"[CACHE] Twitter save error: {e}")
        
        # Cache in memory
        self._user_cache[cache_key] = user_data
        
        # Also save to Redis (shared with user_enricher)
        try:
            from shared.models import EnrichedUser
            enriched = EnrichedUser(
                username=username,
                user_id=user_data.get("user_id", ""),
                name=user_data.get("name", username),
                followers=user_data.get("followers", 0),
                following=user_data.get("following", 0),
                is_blue_verified=user_data.get("is_blue_verified", False),
                verified_type=user_data.get("verified_type"),
                created_at=user_data.get("created_at", ""),
                bio=user_data.get("bio", ""),
                ens=user_data.get("ens", ""),
                wallet=user_data.get("wallet", ""),
                smart_followers=user_data.get("smart_followers", 0),
                prev_usernames_count=user_data.get("prev_usernames_count", 0),
                contracts_count=user_data.get("contracts_count", 0),
                affiliate_label=user_data.get("affiliate_label", ""),
                cached_at=datetime.now().isoformat(),
            )
            await self.storage.save_enriched_user(enriched)
            self.logger.debug(f"[CACHE] Saved to Redis: @{username}")
        except Exception as e:
            self.logger.debug(f"[CACHE] Redis save error: {e}")
        
        # Limit memory cache size
        if len(self._user_cache) > 1000:
            keys = list(self._user_cache.keys())
            for k in keys[:500]:
                del self._user_cache[k]
        
        return user_data
    
    # =========================================================================
    # API CALLS (Same as user_enricher.py)
    # =========================================================================
    
    async def _fetch_twitter_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch user info from Twitter API."""
        if not self.twitter_api_key:
            return None
        
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/info?userName={username}",
                headers={"X-API-Key": self.twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {})
        except Exception as e:
            self.logger.debug(f"Twitter API error: {e}")
        
        return None
    
    async def _fetch_twitter_about(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch user about (affiliate label) from Twitter API."""
        if not self.twitter_api_key:
            return None
        
        try:
            async with self._session.get(
                f"https://api.twitterapi.io/twitter/user/about?userName={username}",
                headers={"X-API-Key": self.twitter_api_key},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    result = {}
                    
                    highlights = data.get("data", {}).get("highlightedLabel", {})
                    if highlights.get("label"):
                        result["affiliate_label"] = highlights["label"]
                    
                    return result
        except Exception as e:
            self.logger.debug(f"Twitter about API error: {e}")
        
        return None
    
    async def _fetch_alphagate_child(self, username: str) -> Optional[Dict[str, Any]]:
        """Fetch R and C counts from AlphaGate (with 24h cache)."""
        # Check Redis cache first (24 hours TTL)
        try:
            cached = await self.storage.get_alphagate_data(username)
            if cached:
                self.logger.debug(f"[ALPHAGATE] CACHE HIT @{username}: R={cached['prev_usernames_count']} C={cached['contracts_count']} SF={cached['smart_followers']}")
                return cached
        except Exception as e:
            self.logger.debug(f"[ALPHAGATE] Cache check error: {e}")
        
        if not self.alphagate_user_id or not self.alphagate_session_id:
            self.logger.warning(f"[ALPHAGATE] Skipping child @{username} - no credentials configured")
            return None
        
        try:
            url = f"https://api.alphagate.io/api/v1/child?username={username}"
            async with self._session.get(
                url,
                headers={
                    "Cookie": f"userId={self.alphagate_user_id}; extensionSessionId={self.alphagate_session_id}",
                    "Origin": "https://alphagate.io",
                    "Referer": "https://alphagate.io/",
                },
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Data is nested: data.data.child
                    child = data.get("data", {}).get("child", {}) or {}
                    
                    # Field is prev_usernames (with underscore), not prevUsernames
                    prev_usernames = child.get("prev_usernames", []) or []
                    contracts = child.get("contracts", []) or []
                    key_followers = child.get("key_followers_count", 0) or 0
                    
                    result = {
                        "prev_usernames_count": len(prev_usernames),
                        "contracts_count": len(contracts),
                        "smart_followers": key_followers,  # From child response
                    }
                    # Log the actual values for debugging
                    self.logger.info(f"[ALPHAGATE] API @{username}: R={result['prev_usernames_count']} C={result['contracts_count']} SF={result['smart_followers']}")
                    
                    # Save to Redis cache (24 hours)
                    try:
                        await self.storage.save_alphagate_data(
                            username,
                            result["prev_usernames_count"],
                            result["contracts_count"],
                            result["smart_followers"],
                        )
                    except Exception as e:
                        self.logger.debug(f"[ALPHAGATE] Cache save error: {e}")
                    
                    return result
                elif resp.status == 401 or resp.status == 403:
                    body = await resp.text()
                    self.logger.error(f"[ALPHAGATE] AUTH ERROR @{username}: status={resp.status} body={body[:200]}")
                else:
                    body = await resp.text()
                    self.logger.warning(f"[ALPHAGATE] child @{username} status={resp.status} body={body[:100]}")
        except Exception as e:
            self.logger.error(f"[ALPHAGATE] child @{username} exception: {e}")
        
        return None
    
    # Note: _fetch_smart_followers removed - /api/v1/user/smartfollowers endpoint no longer exists
    # Smart followers count (key_followers_count) comes from /api/v1/child endpoint instead
    
    async def _resolve_ens(self, wallet: str) -> Optional[str]:
        """Resolve ENS name for wallet address."""
        try:
            async with self._session.get(
                f"https://api.ensideas.com/ens/resolve/{wallet}",
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("name", "")
        except Exception as e:
            self.logger.debug(f"ENS resolve error: {e}")
        
        return None


# Entry point
if __name__ == "__main__":
    TelegramProgressiveWorker.main()
