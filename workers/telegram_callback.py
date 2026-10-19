"""
Telegram Callback Worker

Polls for Telegram updates and handles callbacks/commands.

Responsibilities:
- Poll updates from all bots (deployed, prebankr, trading)
- Handle callback queries (button clicks)
- Handle text commands (/start, /config, etc)
- Publish events for processing

Output Channels:
- telegram:callback - Callback query events
- telegram:message - Text message events
- trading:buy_approved - When user approves buy
- telegram:send - Response messages
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List

from eth_account import Account

from workers.base import BaseWorker
from shared.pubsub import Channels, Events
from shared.models import UserConfig, get_fee_for_block, FEE_SCHEDULE, PendingBuy


class TelegramCallbackWorker(BaseWorker):
    """
    Handles Telegram callbacks and commands.
    """
    
    WORKER_NAME = "telegram-callback"
    
    API_BASE = "https://api.telegram.org/bot"
    
    def __init__(self):
        super().__init__()
        
        # Bot tokens - poll ALL bots that send messages with buttons
        # IMPORTANT: For PRE-BANKR, we use a POOL of bots (round-robin for sending)
        # We need to poll ALL bots in the pool to receive callbacks!
        pool_tokens_str = os.getenv("TELEGRAM_BOT_POOL_TOKENS", "")
        if pool_tokens_str:
            self.prebankr_pool_tokens = [t.strip() for t in pool_tokens_str.split(",") if t.strip()]
        else:
            # Fallback to single token
            self.prebankr_pool_tokens = [os.getenv("TELEGRAM_BOT_TOKEN", "")]
        
        self.trading_token = os.getenv("TELEGRAM_TRADING_BOT_TOKEN", "")
        self.clawnch_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")
        
        if not self.prebankr_pool_tokens or not self.prebankr_pool_tokens[0]:
            raise ValueError("TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_POOL_TOKENS not set!")
        
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Track last update IDs per bot (prebankr_0, prebankr_1, prebankr_2, trading, clawnch)
        self._last_update_id = {
            "trading": 0,
            "clawnch": 0,
        }
        # Add entries for each pool bot
        for i in range(len(self.prebankr_pool_tokens)):
            self._last_update_id[f"prebankr_{i}"] = 0
        
        # Track users waiting to input wallet key (user_id -> True)
        self._waiting_for_key: Dict[int, bool] = {}
        
        # Track users waiting for custom ETH input (user_id -> True)
        self._waiting_for_eth: Dict[int, bool] = {}
        
        # Track users waiting for custom GWEI input (user_id -> True)
        self._waiting_for_gwei: Dict[int, bool] = {}
        
        # Track users waiting for trigger handle input (user_id -> True)
        self._waiting_for_trigger: Dict[int, bool] = {}
        
        # Stats
        self.stats.update({
            "callbacks_handled": 0,
            "commands_handled": 0,
            "likes_added": 0,
            "buys_approved": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=35)
        )
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Run polling loops for ALL prebankr pool bots and trading bot.
        
        NOTE: clawnch is handled by telegram_callback_clawnch worker!
        
        IMPORTANT: PRE-BANKR uses a pool of bots (round-robin for sending).
        We MUST poll ALL bots in the pool to receive callbacks from ALL messages!
        """
        tasks = []
        
        # Poll ALL bots in the PRE-BANKR pool
        for i, token in enumerate(self.prebankr_pool_tokens):
            tasks.append(self._poll_loop(f"prebankr_{i}", token))
        
        if self.trading_token:
            tasks.append(self._poll_loop("trading", self.trading_token))
        
        # NOTE: clawnch polling removed - handled by telegram_callback_clawnch
        
        self.logger.info(f"Starting polling for {len(tasks)} bots ({len(self.prebankr_pool_tokens)} prebankr + trading, clawnch handled separately)")
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _poll_loop(self, bot_name: str, bot_token: str):
        """Poll updates for a specific bot."""
        self.logger.info(f"Starting poll loop for {bot_name} bot")
        
        while self._running:
            try:
                updates = await self._get_updates(
                    bot_token,
                    offset=self._last_update_id[bot_name] + 1 if self._last_update_id[bot_name] else None,
                )
                
                for update in updates:
                    self._last_update_id[bot_name] = update.get("update_id", self._last_update_id[bot_name])
                    
                    # Handle callback query
                    callback = update.get("callback_query")
                    if callback:
                        await self._handle_callback(bot_name, bot_token, callback)
                        continue
                    
                    # Handle message
                    message = update.get("message")
                    if message:
                        await self._handle_message(bot_name, bot_token, message)
                
                await asyncio.sleep(1)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Poll error ({bot_name}): {e}")
                self.stats["errors"] += 1
                await asyncio.sleep(5)
    
    async def _get_updates(
        self,
        bot_token: str,
        offset: Optional[int] = None,
        timeout: int = 30,
    ) -> List[Dict[str, Any]]:
        """Get updates from Telegram."""
        url = f"{self.API_BASE}{bot_token}/getUpdates"
        
        payload = {
            "timeout": timeout,
            "allowed_updates": ["callback_query", "message"],
        }
        
        if offset:
            payload["offset"] = offset
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data.get("result", [])
                else:
                    # Log Telegram API errors
                    error_code = data.get("error_code", "unknown")
                    description = data.get("description", "unknown error")
                    self.logger.error(f"getUpdates API error: [{error_code}] {description}")
                    return []
        except asyncio.TimeoutError:
            # Normal timeout on long polling - not an error
            return []
        except Exception as e:
            self.logger.error(f"Get updates error: {type(e).__name__}: {e}")
            return []
    
    async def _handle_callback(
        self,
        bot_name: str,
        bot_token: str,
        callback: Dict[str, Any],
    ):
        """Handle callback query (button click)."""
        callback_id = callback.get("id", "")
        data = callback.get("data", "")
        user = callback.get("from", {})
        user_id = user.get("id", 0)
        username = user.get("username", "")
        message = callback.get("message", {})
        message_id = message.get("message_id", 0)
        chat_id = message.get("chat", {}).get("id", user_id)
        
        self.logger.info(f"[{bot_name}] Callback @{username}: {data}")
        self.stats["callbacks_handled"] += 1
        
        # Route to appropriate handler
        if data.startswith("like:"):
            await self._handle_like(bot_token, callback_id, data, user_id, username, message_id, chat_id)
        
        elif data.startswith("buy:"):
            await self._handle_buy_click(bot_token, callback_id, data, user_id, username, chat_id)
        
        elif data.startswith("buyconfirm:"):
            await self._handle_buy_confirm(bot_token, callback_id, data, user_id, username, message_id, chat_id)
        
        elif data.startswith("buy_preclawnch:"):
            await self._handle_buy_preclawnch(bot_token, callback_id, data, user_id, username, chat_id)
        
        elif data.startswith("buycancel:"):
            await self._handle_buy_cancel(bot_token, callback_id, data, user_id, message_id, chat_id)
        
        elif data == "buycancel":
            # Legacy format without tweet_id
            await self._answer_callback(bot_token, callback_id, "Cancelado")
            await self._edit_message(bot_token, chat_id, message_id, "Cancelado.")
        
        elif data.startswith("cfg:") or data.startswith("set") or data.startswith("cancel_sniper:") or data == "cancel_all_snipers" or data.startswith("remove_trigger:") or data in ("custometh", "customgwei", "add_trigger", "clear_triggers"):
            await self._handle_config(bot_token, callback_id, data, user_id, username, message_id, chat_id)
        
        elif data.startswith("cancel_order:"):
            # Order already executed - inform user
            await self._answer_callback(bot_token, callback_id, "Ordem já executada! Use os links para vender.", show_alert=True)
        
        else:
            await self._answer_callback(bot_token, callback_id)
        
        self.stats["messages_processed"] += 1
    
    async def _handle_message(
        self,
        bot_name: str,
        bot_token: str,
        message: Dict[str, Any],
    ):
        """Handle text message (command or wallet key input)."""
        chat_id = message.get("chat", {}).get("id", 0)
        user_id = message.get("from", {}).get("id", 0)
        username = message.get("from", {}).get("username", "")
        text = message.get("text", "").strip()
        message_id = message.get("message_id", 0)
        
        if not text:
            return
        
        # Get user config
        config = await self.storage.get_user_config(user_id, username)
        
        # =====================================================================
        # CHECK IF USER IS WAITING TO INPUT WALLET KEY
        # =====================================================================
        if self._waiting_for_key.get(user_id):
            # User clicked "Configurar Wallet" and is now sending the key
            # Delete the waiting state
            del self._waiting_for_key[user_id]
            
            # Check if this looks like a private key (64 hex chars, with or without 0x)
            key = text.strip()
            if key.startswith("0x"):
                key = key[2:]
            
            # Check if it's a valid hex key
            if len(key) == 64 and all(c in '0123456789abcdefABCDEF' for c in key):
                await self._process_wallet_key(bot_token, chat_id, message_id, user_id, key, config)
            else:
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Isso não parece uma private key válida.\n\nDeve ter 64 caracteres hexadecimais.\n\nClique em <b>Wallet</b> para tentar novamente."
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR CUSTOM ETH INPUT
        # =====================================================================
        if self._waiting_for_eth.get(user_id):
            del self._waiting_for_eth[user_id]
            
            try:
                eth_value = float(text.replace(",", "."))
                if eth_value <= 0:
                    raise ValueError("Must be positive")
                if eth_value > 100:
                    raise ValueError("Too high")
                
                config.eth_amount = eth_value
                await self.storage.save_user_config(config)
                
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ ETH configurado: <b>{eth_value} ETH</b>\n\nUse /config para ver suas configurações."
                )
            except (ValueError, TypeError):
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Valor inválido.\n\nDigite um número válido, ex: <code>0.15</code> ou <code>0.5</code>"
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR CUSTOM GWEI INPUT
        # =====================================================================
        if self._waiting_for_gwei.get(user_id):
            del self._waiting_for_gwei[user_id]
            
            try:
                gwei_value = float(text.replace(",", "."))
                if gwei_value <= 0:
                    raise ValueError("Must be positive")
                if gwei_value > 10000:
                    raise ValueError("Too high")
                
                config.gwei = gwei_value
                await self.storage.save_user_config(config)
                
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Gas configurado: <b>{gwei_value} gwei</b>\n\nUse /config para ver suas configurações."
                )
            except (ValueError, TypeError):
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Valor inválido.\n\nDigite um número válido, ex: <code>0.1</code> ou <code>50</code>"
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR TRIGGER HANDLE INPUT
        # =====================================================================
        if self._waiting_for_trigger.get(user_id):
            del self._waiting_for_trigger[user_id]
            
            # Clean handle input
            handle = text.lower().strip().lstrip("@")
            
            if not handle or len(handle) < 2:
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Handle inválido.\n\nDigite um @handle válido, ex: <code>@elonmusk</code>"
                )
                return
            
            # Add handle
            success = await self.storage.add_sniper_handle(user_id, handle)
            
            if success:
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Trigger adicionado: <b>@{handle}</b>\n\n"
                    f"Quando <b>@{handle}</b> criar um token no Bankr, "
                    f"sua compra será executada automaticamente!\n\n"
                    f"Use /config → Triggers para gerenciar."
                )
            else:
                await self._send_message(
                    bot_token, chat_id,
                    f"⚠️ @{handle} já está na sua lista de triggers."
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR MOLTX HANDLE INPUT
        # =====================================================================
        if hasattr(self, '_waiting_for_moltx_handle') and self._waiting_for_moltx_handle.get(user_id):
            del self._waiting_for_moltx_handle[user_id]
            
            handle = text.lower().strip().lstrip("@")
            
            if not handle or len(handle) < 2:
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Handle inválido.\n\nDigite um @handle válido, ex: <code>@elonmusk</code>"
                )
                return
            
            success = await self.storage.add_moltx_trigger_handle(user_id, handle)
            
            if success:
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Handle MoltX adicionado: <b>@{handle}</b>\n\n"
                    f"Quando <b>@{handle}</b> criar um token via MoltX, "
                    f"sua compra será executada automaticamente!\n\n"
                    f"Use /config → TRIGGER MOLTX para gerenciar."
                )
            else:
                await self._send_message(
                    bot_token, chat_id,
                    f"⚠️ @{handle} já está na sua lista MoltX."
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR MOLTBOOK HANDLE INPUT
        # =====================================================================
        if hasattr(self, '_waiting_for_moltbook_handle') and self._waiting_for_moltbook_handle.get(user_id):
            del self._waiting_for_moltbook_handle[user_id]
            
            handle = text.lower().strip().lstrip("@")
            
            if not handle or len(handle) < 2:
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Handle inválido.\n\nDigite um @handle válido, ex: <code>@elonmusk</code>"
                )
                return
            
            success = await self.storage.add_moltbook_trigger_handle(user_id, handle)
            
            if success:
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Handle MoltBook adicionado: <b>@{handle}</b>\n\n"
                    f"Quando <b>@{handle}</b> criar um token via MoltBook, "
                    f"sua compra será executada automaticamente!\n\n"
                    f"Use /config → TRIGGER MOLTBOOK para gerenciar."
                )
            else:
                await self._send_message(
                    bot_token, chat_id,
                    f"⚠️ @{handle} já está na sua lista MoltBook."
                )
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR CUSTOM MOLTX VALUES
        # =====================================================================
        if hasattr(self, '_waiting_for_custom_moltx_followers') and self._waiting_for_custom_moltx_followers.get(user_id):
            del self._waiting_for_custom_moltx_followers[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltx_trigger_min_followers = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min Followers (MoltX): <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTX para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        if hasattr(self, '_waiting_for_custom_moltx_moltfollowers') and self._waiting_for_custom_moltx_moltfollowers.get(user_id):
            del self._waiting_for_custom_moltx_moltfollowers[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltx_trigger_min_moltx_followers = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min MoltX Followers: <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTX para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        if hasattr(self, '_waiting_for_custom_moltx_smart') and self._waiting_for_custom_moltx_smart.get(user_id):
            del self._waiting_for_custom_moltx_smart[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltx_trigger_min_smart_followers = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min Smart Followers (MoltX): <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTX para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        # =====================================================================
        # CHECK IF USER IS WAITING FOR CUSTOM MOLTBOOK VALUES
        # =====================================================================
        if hasattr(self, '_waiting_for_custom_moltbook_followers') and self._waiting_for_custom_moltbook_followers.get(user_id):
            del self._waiting_for_custom_moltbook_followers[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltbook_trigger_min_followers = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min Followers (MoltBook): <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTBOOK para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        if hasattr(self, '_waiting_for_custom_moltbook_smart') and self._waiting_for_custom_moltbook_smart.get(user_id):
            del self._waiting_for_custom_moltbook_smart[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltbook_trigger_min_smart_followers = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min Smart Followers (MoltBook): <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTBOOK para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        if hasattr(self, '_waiting_for_custom_moltbook_karma') and self._waiting_for_custom_moltbook_karma.get(user_id):
            del self._waiting_for_custom_moltbook_karma[user_id]
            try:
                value = int(text.strip())
                if value < 0:
                    raise ValueError("Must be positive")
                config.moltbook_trigger_min_karma = value
                await self.storage.save_user_config(config)
                await self._send_message(
                    bot_token, chat_id,
                    f"✅ Min Karma (MoltBook): <b>{value if value > 0 else 'OFF'}</b>\n\nUse /config → TRIGGER MOLTBOOK para ver."
                )
            except (ValueError, TypeError):
                await self._send_message(bot_token, chat_id, "❌ Valor inválido. Digite um número inteiro.")
            return
        
        # =====================================================================
        # HANDLE COMMANDS
        # =====================================================================
        if not text.startswith("/"):
            return
        
        self.logger.info(f"[{bot_name}] Command @{username}: {text}")
        self.stats["commands_handled"] += 1
        
        # Route commands
        if text in ["/start", "/config", "/menu"]:
            await self._send_config_menu(bot_token, chat_id, config)
        
        elif text.startswith("/setwallet "):
            await self._handle_set_wallet(bot_token, chat_id, user_id, text, config)
        
        elif text.startswith("/setkey "):
            # Extract key from command
            key = text.split(" ", 1)[1].strip() if " " in text else ""
            if key.startswith("0x"):
                key = key[2:]
            await self._process_wallet_key(bot_token, chat_id, message_id, user_id, key, config)
        
        elif text == "/fees":
            await self._send_fees(bot_token, chat_id)
        
        elif text == "/status":
            await self._send_status(bot_token, chat_id, config)
        
        elif text == "/approve":
            text_msg, keyboard = self._build_preapprove_menu(config)
            await self._send_message(bot_token, chat_id, text_msg, keyboard)
        
        elif text == "/stop":
            # Cancel all pending snipers
            cancelled_count = await self._cancel_all_user_snipers(user_id)
            await self._send_message(
                bot_token, chat_id,
                f"🛑 <b>STOP!</b>\n\n"
                f"Cancelados: <b>{cancelled_count}</b> snipers\n\n"
                f"Todos os snipers pendentes foram cancelados."
            )
        
        self.stats["messages_processed"] += 1
    
    # =========================================================================
    # LIKE HANDLERS
    # =========================================================================
    
    async def _handle_like(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int = 0,
        chat_id: int = 0,
    ):
        """Handle like button for both BANKR (tweet_id) and CLAWNCH (post_id/thread_id)."""
        identifier = data.split(":", 1)[1]
        
        # INSTANT: Answer callback immediately (stops 'thinking' spinner)
        self.logger.info(f"[ANSWERING] callback_id={callback_id[:20]}... token={bot_token[:20]}...")
        await self._answer_callback(bot_token, callback_id)
        self.logger.info(f"[ANSWERED] callback done")
        
        # Process like/unlike in background (user doesn't wait)
        if await self.storage.has_liked(identifier, user_id):
            count = await self.storage.remove_like(identifier, user_id)
            self.logger.info(f"[UNLIKE] @{username}")
        else:
            count = await self.storage.add_like(identifier, user_id, username)
            self.stats["likes_added"] += 1
            self.logger.info(f"[LIKE] @{username}")
        
        # Update like button text with new count
        if message_id and chat_id:
            like_text = f"❤️ {count}" if count > 0 else "❤️ Like"
            
            # Detect if CLAWNCH (UUID or thread:uuid) vs BANKR (numeric tweet_id)
            is_clawnch = "-" in identifier or identifier.startswith("thread:")
            
            if is_clawnch:
                # CLAWNCH keyboard: [Agent][Post][Owner] + [Like][Buy]
                preclawnch = await self.storage.get_preclawnch_token(identifier)
                if preclawnch:
                    platform = preclawnch.get("platform", "")
                    agent_name = preclawnch.get("agent_name", "")
                    x_handle = preclawnch.get("x_handle", "")
                    
                    row1 = []
                    if platform == "moltbook":
                        if agent_name:
                            row1.append({"text": "👤 Agent", "url": f"https://www.moltbook.com/u/{agent_name}"})
                        row1.append({"text": "📄 Post", "url": f"https://www.moltbook.com/m/clawnch/post/{identifier}"})
                    elif platform == "moltx":
                        if agent_name:
                            row1.append({"text": "👤 Agent", "url": f"https://moltx.io/{agent_name}"})
                        row1.append({"text": "📄 Post", "url": f"https://moltx.io/post/{identifier}"})
                    elif platform == "4claw":
                        thread_id = identifier.replace("thread:", "")
                        row1.append({"text": "👤 Agent", "url": f"https://4claw.org/t/{thread_id}"})
                        row1.append({"text": "📄 Thread", "url": f"https://4claw.org/t/{thread_id}"})
                    
                    if x_handle:
                        row1.append({"text": "🐦 Owner", "url": f"https://x.com/{x_handle}"})
                    
                    row2 = [
                        {"text": like_text, "callback_data": f"like:{identifier}"},
                        {"text": "💰 Buy", "callback_data": f"buy:{identifier}"}
                    ]
                    
                    keyboard = {"inline_keyboard": [row1, row2] if row1 else [row2]}
                else:
                    # Fallback if no preclawnch data
                    keyboard = {"inline_keyboard": [[
                        {"text": like_text, "callback_data": f"like:{identifier}"},
                        {"text": "💰 Buy", "callback_data": f"buy:{identifier}"}
                    ]]}
            else:
                # BANKR keyboard: Tweet | From | Like | BUY (1 row)
                token_info = await self.storage.get_token_info(identifier)
                from_username = token_info.from_username if token_info else ""
                
                row = [{"text": "Tweet", "url": f"https://twitter.com/i/status/{identifier}"}]
                if from_username:
                    row.append({"text": "From", "url": f"https://twitter.com/{from_username}"})
                row.append({"text": like_text, "callback_data": f"like:{identifier}"})
                row.append({"text": "💰 BUY", "callback_data": f"buy:{identifier}"})
                
                keyboard = {"inline_keyboard": [row]}
            
            # Try to update the keyboard (may fail if message is too old)
            try:
                success = await self._update_reply_markup(bot_token, chat_id, message_id, keyboard)
                if success:
                    self.logger.info(f"[LIKE] Updated keyboard for chat={chat_id} msg={message_id}")
                else:
                    self.logger.warning(f"[LIKE] Failed to update keyboard for chat={chat_id} msg={message_id}")
            except Exception as e:
                self.logger.warning(f"[LIKE] Exception updating keyboard: {e}")
    
    # =========================================================================
    # BUY HANDLERS
    # =========================================================================
    
    async def _handle_buy_click(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        username: str,
        chat_id: int,
    ):
        """
        Handle BUY button click - create PendingBuy immediately.
        
        Supports both:
        - Pre-Bankr (tweet_id): numeric ID like "1882882771..."
        - Pre-Clawnch (post_id): UUID like "8fdd6010-..." or "thread:uuid"
        
        No pre-confirmation needed. The buy is registered immediately.
        - Auto-buy ON: Will execute automatically when on-chain
        - Auto-buy OFF: Will ask for confirmation when on-chain
        """
        identifier = data.split(":", 1)[1]
        
        # INSTANT: Answer callback immediately (stops 'thinking' spinner)
        await self._answer_callback(bot_token, callback_id)
        
        config = await self.storage.get_user_config(user_id, username)
        
        # Detect if this is Pre-Clawnch (UUID/thread:uuid) or Pre-Bankr (numeric tweet_id)
        is_preclawnch = "-" in identifier or identifier.startswith("thread:")
        
        # Use TRADING bot for private messages (same bot as /start)
        trading_bot = self.trading_token or bot_token
        
        # Check if wallet is configured
        if not config.is_configured():
            # Send to USER's private chat via TRADING bot
            await self._send_message(
                trading_bot, user_id,
                f"""<b>⚠️ CONFIG INCOMPLETA</b>

Falta configurar: {', '.join(config.get_missing_config())}

Clique no botão abaixo para configurar.""",
                {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
            )
            return
        
        # Get token info based on source
        token_symbol = "Token"
        from_username = ""
        platform_label = ""
        
        if is_preclawnch:
            # Pre-Clawnch: fetch from preclawnch storage
            preclawnch_token = await self.storage.get_preclawnch_token(identifier)
            if preclawnch_token:
                token_symbol = preclawnch_token.get("token_symbol", "Token")
                from_username = preclawnch_token.get("agent_name", "")
                platform = preclawnch_token.get("platform", "")
                platform_label = {
                    "moltx": "🦞 MoltX",
                    "moltbook": "📖 Moltbook",
                    "4claw": "🦀 4claw"
                }.get(platform, platform.title())
        else:
            # Pre-Bankr: fetch from token_info storage
            token_info = await self.storage.get_token_info(identifier)
            token_symbol = token_info.token_symbol if token_info else "Token"
            from_username = token_info.from_username if token_info else ""
        
        # ATOMIC: Try to acquire lock first (prevents race condition on double-click)
        if not await self.storage.try_acquire_buy_creation_lock(user_id, identifier):
            # Another click is already processing
            return
        
        # Check if already has a pending buy
        existing = await self.storage.get_pending_buy(user_id, identifier)
        if existing:
            await self._send_message(
                trading_bot, user_id,
                "<b>⚠️ Você já tem um BUY registrado para este token!</b>"
            )
            return
        
        # Create PendingBuy immediately
        buy = await self.storage.approve_buy(user_id, identifier, username, token_symbol)
        fee = get_fee_for_block(config.block_offset)
        
        # Build message based on auto_buy setting
        auto_status = "🟢 AUTO" if config.auto_buy else "🟡 MANUAL"
        auto_note = "Executará automaticamente quando on-chain." if config.auto_buy else "Você receberá confirmação quando on-chain."
        
        # Build from line
        if is_preclawnch:
            from_line = f"Agent: {from_username}" if from_username else ""
            platform_line = f"Platform: <b>{platform_label}</b>" if platform_label else ""
            header = "✅ PRE-CLAWNCH BUY REGISTRADO!"
        else:
            from_line = f"From: @{from_username}" if from_username else ""
            platform_line = ""
            header = "✅ BUY REGISTRADO!"
        
        # Send confirmation to USER's private chat via TRADING bot!
        await self._send_message(
            trading_bot, user_id,
            f"""<b>{header}</b>

Token: <b>{token_symbol}</b>
{from_line}
{platform_line}

<b>Config:</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Modo: <b>{auto_status}</b>

<i>{auto_note}</i>

⏳ Aguardando token ir on-chain...""",
            {"inline_keyboard": [[{"text": "❌ Cancelar Buy", "callback_data": f"buycancel:{identifier}"}]]}
        )
        
        # Publish event
        event = Events.buy_approved(
            user_id=user_id,
            username=username,
            tweet_id=identifier,  # Works for both tweet_id and post_id
            token_symbol=token_symbol,
            eth_amount=config.eth_amount,
            block_offset=config.block_offset,
            fee_expected=fee,
        )
        await self.publish(Channels.TRADING_BUY_APPROVED, event)
        
        self.stats["buys_approved"] += 1
        flow_type = "PRE-CLAWNCH" if is_preclawnch else "PRE-BANKR"
        self.logger.info(f"[{flow_type} BUY REGISTERED] @{username} -> {token_symbol} (auto_buy={config.auto_buy})")
    
    async def _handle_buy_preclawnch(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        username: str,
        chat_id: int,
    ):
        """
        Handle BUY button click from Pre-Clawnch notifications (MoltX/Moltbook/4claw).
        
        The callback data format is: buy_preclawnch:{wallet_prefix}
        We use the wallet_prefix to look up the stored post_id from the Pre-Clawnch token.
        The post_id will later match with on-chain context_parsed.messageId.
        """
        wallet_prefix = data.split(":", 1)[1] if ":" in data else ""
        
        # INSTANT: Answer callback immediately (stops 'thinking' spinner)
        await self._answer_callback(bot_token, callback_id)
        
        config = await self.storage.get_user_config(user_id, username)
        
        # Use TRADING bot for private messages
        trading_bot = self.trading_token or bot_token
        
        # Check if wallet is configured
        if not config.is_configured():
            await self._send_message(
                trading_bot, user_id,
                f"""<b>⚠️ CONFIG INCOMPLETA</b>

Falta configurar: {', '.join(config.get_missing_config())}

Clique no botão abaixo para configurar.""",
                {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
            )
            return
        
        # Look up the Pre-Clawnch token by wallet prefix
        preclawnch_token = await self.storage.get_preclawnch_token_by_wallet(wallet_prefix)
        if not preclawnch_token:
            self.logger.warning(f"[PRE-CLAWNCH BUY] Token not found for wallet prefix: {wallet_prefix}")
            await self._send_message(trading_bot, user_id, "❌ Token not found")
            return
        
        post_id = preclawnch_token.get("post_id", "")
        token_symbol = preclawnch_token.get("token_symbol", "Token")
        token_name = preclawnch_token.get("token_name", "")
        platform = preclawnch_token.get("platform", "")
        agent_name = preclawnch_token.get("agent_name", "")
        
        # ATOMIC: Try to acquire lock first (prevents race condition on double-click)
        if not await self.storage.try_acquire_buy_creation_lock(user_id, post_id):
            # Another click is already processing
            return
        
        # Check if already has a pending buy for this post_id
        existing = await self.storage.get_pending_buy(user_id, post_id)
        if existing:
            await self._send_message(
                trading_bot, user_id,
                "<b>⚠️ Você já tem um BUY registrado para este token!</b>"
            )
            return
        
        # Create PendingBuy using post_id as the identifier
        # Note: We use post_id instead of tweet_id for Pre-Clawnch tokens
        buy = await self.storage.approve_buy(user_id, post_id, username, token_symbol)
        fee = get_fee_for_block(config.block_offset)
        
        # Build message
        auto_status = "🟢 AUTO" if config.auto_buy else "🟡 MANUAL"
        auto_note = "Executará automaticamente quando on-chain." if config.auto_buy else "Você receberá confirmação quando on-chain."
        
        platform_label = {
            "moltx": "🦞 MoltX",
            "moltbook": "📖 Moltbook",
            "4claw": "🦀 4claw"
        }.get(platform, platform.title())
        
        await self._send_message(
            trading_bot, user_id,
            f"""<b>✅ PRE-CLAWNCH BUY REGISTRADO!</b>

Token: <b>${token_symbol}</b>
{f'Nome: {token_name}' if token_name else ''}
{f'Agent: {agent_name}' if agent_name else ''}
Platform: <b>{platform_label}</b>

<b>Config:</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Modo: <b>{auto_status}</b>

<i>{auto_note}</i>

⏳ Aguardando token ir on-chain (Clawnch)...""",
            {"inline_keyboard": [[{"text": "❌ Cancelar Buy", "callback_data": f"buycancel:{post_id}"}]]}
        )
        
        # Publish event (using post_id as tweet_id for compatibility)
        event = Events.buy_approved(
            user_id=user_id,
            username=username,
            tweet_id=post_id,  # Using post_id as the identifier
            token_symbol=token_symbol,
            eth_amount=config.eth_amount,
            block_offset=config.block_offset,
            fee_expected=fee,
        )
        await self.publish(Channels.TRADING_BUY_APPROVED, event)
        
        self.stats["buys_approved"] += 1
        self.logger.info(f"[PRE-CLAWNCH BUY REGISTERED] @{username} -> {token_symbol} ({platform}/{post_id})")

    
    async def _handle_buy_confirm(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int,
        chat_id: int,
    ):
        """Handle BUY confirmation (when on-chain and auto_buy=OFF)."""
        tweet_id = data.split(":", 1)[1]
        
        # INSTANT: Answer callback immediately (stops 'thinking' spinner)
        await self._answer_callback(bot_token, callback_id)
        
        config = await self.storage.get_user_config(user_id, username)
        
        if not config.is_configured():
            await self._send_message(bot_token, chat_id, "⚠️ Configure wallet primeiro!")
            return
        
        # Get pending buy
        buy = await self.storage.get_pending_buy(user_id, tweet_id)
        if not buy:
            await self._send_message(bot_token, chat_id, "❌ Buy não encontrado")
            return
        
        # Check if buy is pending confirmation
        if buy.status not in ["PENDING_CONFIRM", "APPROVED"]:
            await self._send_message(bot_token, chat_id, f"❌ Status inválido: {buy.status}")
            return
        
        # Update status to WAITING so trading_executor will pick it up
        await self.storage.update_buy_status(buy, "WAITING")
        
        # Edit message
        await self._edit_message(
            bot_token,
            chat_id,
            message_id,
            f"""<b>✅ CONFIRMADO!</b>

Token: <b>{buy.token_symbol}</b>
Contract: <code>{buy.token_address}</code>
ETH: <b>{buy.eth_amount}</b>
Bloco alvo: <b>{buy.target_block}</b>

⏳ Aguardando bloco para executar...""",
        )
        
        self.logger.info(f"[BUY CONFIRMED] @{username} -> {buy.token_symbol} (now WAITING)")
    
    async def _handle_buy_cancel(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        message_id: int,
        chat_id: int,
    ):
        """Handle BUY cancellation."""
        tweet_id = data.split(":", 1)[1] if ":" in data else ""
        
        # INSTANT: Answer callback immediately (stops 'thinking' spinner)
        await self._answer_callback(bot_token, callback_id)
        
        # Cancel the pending buy if exists - use storage.cancel_buy for proper index updates
        if tweet_id:
            success = await self.storage.cancel_buy(user_id, tweet_id)
            if success:
                self.logger.info(f"[BUY CANCELLED] User {user_id} -> tweet {tweet_id}")
        
        await self._edit_message(bot_token, chat_id, message_id, "❌ <b>Compra cancelada.</b>")
    
    async def _cancel_all_user_snipers(self, user_id: int) -> int:
        """Cancel all pending snipers for a user. Returns count of cancelled."""
        # Get all active buys for user (APPROVED, WAITING, PENDING_CONFIRM)
        cancelled = 0
        
        # Get buys from each status
        for status in ["APPROVED", "WAITING", "PENDING_CONFIRM"]:
            buys = await self.storage.get_pending_buys_by_status(status)
            for buy in buys:
                if buy.user_id == user_id:
                    success = await self.storage.cancel_buy(user_id, buy.tweet_id)
                    if success:
                        cancelled += 1
        
        self.logger.info(f"[STOP] User {user_id} cancelled {cancelled} snipers")
        return cancelled
    
    # =========================================================================
    # CONFIG HANDLERS
    # =========================================================================
    
    async def _handle_config(
        self,
        bot_token: str,
        callback_id: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int,
        chat_id: int,
    ):
        """Handle config menu callbacks."""
        config = await self.storage.get_user_config(user_id, username)
        
        if data in ["cfg:main", "cfg:back"]:
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_main_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:block":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_block_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:eth":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_eth_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:gwei":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_gwei_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:fees":
            await self._answer_callback(bot_token, callback_id)
            text = self._build_fees_text()
            keyboard = {"inline_keyboard": [[{"text": "< Voltar", "callback_data": "cfg:main"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:autobuy":
            config.auto_buy = not config.auto_buy
            await self.storage.save_user_config(config)
            status = "ON" if config.auto_buy else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Auto-buy: {status}", show_alert=True)
            text, keyboard = self._build_main_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:status":
            await self._answer_callback(bot_token, callback_id)
            stats = await self.storage.get_stats()
            wallet_status = f"<code>{config.wallet_address}</code>" if config.wallet_address else "NAO CONFIGURADA"
            text = f"""<b>STATUS</b>

Wallet: {wallet_status}
ETH: {config.eth_amount}
Bloco: +{config.block_offset} ({get_fee_for_block(config.block_offset):.1f}%)
Auto-buy: {'ON' if config.auto_buy else 'OFF'}

Pending buys: {stats.get('pending_buys_waiting', 0)}"""
            keyboard = {"inline_keyboard": [[{"text": "< Voltar", "callback_data": "cfg:main"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:snipers":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = await self._build_snipers_menu(user_id, config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("cancel_sniper:"):
            tweet_id = data.split(":", 1)[1]
            success = await self.storage.cancel_buy(user_id, tweet_id)
            if success:
                await self._answer_callback(bot_token, callback_id, "Sniper cancelado!", show_alert=True)
            else:
                await self._answer_callback(bot_token, callback_id, "Sniper nao encontrado", show_alert=True)
            # Refresh menu
            text, keyboard = await self._build_snipers_menu(user_id, config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cancel_all_snipers":
            # Cancel all pending snipers for this user
            cancelled_count = await self._cancel_all_user_snipers(user_id)
            await self._answer_callback(bot_token, callback_id, f"🗑️ {cancelled_count} snipers cancelados!", show_alert=True)
            # Refresh menu
            text, keyboard = await self._build_snipers_menu(user_id, config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:preapprove":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_preapprove_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setpreapprove:"):
            value = float(data.split(":")[1])
            config.max_eth_approved = value
            await self.storage.save_user_config(config)
            if value > 0:
                await self._answer_callback(bot_token, callback_id, f"Pre-approve: {value} ETH", show_alert=True)
            else:
                await self._answer_callback(bot_token, callback_id, "Pre-approve: OFF", show_alert=True)
            text, keyboard = self._build_preapprove_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:triggers":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_triggers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "add_trigger":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_trigger[user_id] = True
            text = """<b>➕ ADICIONAR TRIGGER</b>

Digite o @handle do Twitter que deseja monitorar.

<i>Quando este usuário criar um token no Bankr, 
sua compra será executada automaticamente!</i>

Exemplo: <code>@elonmusk</code> ou <code>cz_binance</code>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_trigger"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:cancel_trigger":
            self._waiting_for_trigger.pop(user_id, None)
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_triggers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("remove_trigger:"):
            handle = data.split(":", 1)[1]
            await self.storage.remove_sniper_handle(user_id, handle)
            await self._answer_callback(bot_token, callback_id, f"❌ @{handle} removido!", show_alert=True)
            # Refresh config
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_triggers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "clear_triggers":
            count = await self.storage.clear_sniper_handles(user_id)
            await self._answer_callback(bot_token, callback_id, f"🗑️ {count} triggers removidos!", show_alert=True)
            # Refresh config
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_triggers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        # =====================================================================
        # TRIGGER MOLTX
        # =====================================================================
        elif data == "cfg:trigger_moltx":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_toggle":
            await self.storage.set_moltx_trigger_enabled(user_id, not config.moltx_trigger_enabled)
            config = await self.storage.get_user_config(user_id, username)
            status = "ON" if config.moltx_trigger_enabled else "OFF"
            await self._answer_callback(bot_token, callback_id, f"MoltX Trigger: {status}", show_alert=True)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_followers":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltx_followers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltx_followers:"):
            value = int(data.split(":")[1])
            config.moltx_trigger_min_followers = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min Followers: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_moltfollowers":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltx_moltfollowers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltx_moltfollowers:"):
            value = int(data.split(":")[1])
            config.moltx_trigger_min_moltx_followers = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min MoltX Followers: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_smart":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltx_smart_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltx_smart:"):
            value = int(data.split(":")[1])
            config.moltx_trigger_min_smart_followers = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min Smart Followers: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_verified":
            config.moltx_trigger_x_verified = not config.moltx_trigger_x_verified
            await self.storage.save_user_config(config)
            status = "ON" if config.moltx_trigger_x_verified else "OFF"
            await self._answer_callback(bot_token, callback_id, f"X Verified: {status}", show_alert=True)
            text, keyboard = self._build_trigger_moltx_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltx_followers":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltx_followers = self._waiting_for_custom_moltx_followers if hasattr(self, '_waiting_for_custom_moltx_followers') else {}
            self._waiting_for_custom_moltx_followers[user_id] = True
            text = """<b>✏️ CUSTOM MIN FOLLOWERS (MOLTX)</b>

Digite o número mínimo de followers Twitter:

<i>Exemplo: 2500 ou 15000</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltx_followers"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltx_moltfollowers":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltx_moltfollowers = self._waiting_for_custom_moltx_moltfollowers if hasattr(self, '_waiting_for_custom_moltx_moltfollowers') else {}
            self._waiting_for_custom_moltx_moltfollowers[user_id] = True
            text = """<b>✏️ CUSTOM MIN MOLTX FOLLOWERS</b>

Digite o número mínimo de followers MoltX:

<i>Exemplo: 3 ou 25</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltx_moltfollowers"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltx_smart":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltx_smart = self._waiting_for_custom_moltx_smart if hasattr(self, '_waiting_for_custom_moltx_smart') else {}
            self._waiting_for_custom_moltx_smart[user_id] = True
            text = """<b>✏️ CUSTOM MIN SMART FOLLOWERS (MOLTX)</b>

Digite o número mínimo de smart followers:

<i>Exemplo: 3 ou 25</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltx_smart"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltx_handles":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltx_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "add_moltx_handle":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_moltx_handle = self._waiting_for_moltx_handle if hasattr(self, '_waiting_for_moltx_handle') else {}
            self._waiting_for_moltx_handle[user_id] = True
            text = """<b>➕ ADICIONAR HANDLE MOLTX</b>

Digite o @handle do Twitter que deseja monitorar no MoltX.

<i>Quando este usuário criar um token via MoltX, 
sua compra será executada automaticamente!</i>

Exemplo: <code>@elonmusk</code> ou <code>vitalik</code>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_moltx_handle"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:cancel_moltx_handle":
            if hasattr(self, '_waiting_for_moltx_handle'):
                self._waiting_for_moltx_handle.pop(user_id, None)
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltx_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("remove_moltx_handle:"):
            handle = data.split(":", 1)[1]
            await self.storage.remove_moltx_trigger_handle(user_id, handle)
            await self._answer_callback(bot_token, callback_id, f"❌ @{handle} removido!", show_alert=True)
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_moltx_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "clear_moltx_handles":
            count = await self.storage.clear_moltx_trigger_handles(user_id)
            await self._answer_callback(bot_token, callback_id, f"🗑️ {count} handles removidos!", show_alert=True)
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_moltx_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        # =====================================================================
        # TRIGGER MOLTBOOK
        # =====================================================================
        elif data == "cfg:trigger_moltbook":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_toggle":
            await self.storage.set_moltbook_trigger_enabled(user_id, not config.moltbook_trigger_enabled)
            config = await self.storage.get_user_config(user_id, username)
            status = "ON" if config.moltbook_trigger_enabled else "OFF"
            await self._answer_callback(bot_token, callback_id, f"MoltBook Trigger: {status}", show_alert=True)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_followers":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltbook_followers_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltbook_followers:"):
            value = int(data.split(":")[1])
            config.moltbook_trigger_min_followers = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min Followers: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_smart":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltbook_smart_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltbook_smart:"):
            value = int(data.split(":")[1])
            config.moltbook_trigger_min_smart_followers = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min Smart Followers: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_karma":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltbook_karma_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setmoltbook_karma:"):
            value = int(data.split(":")[1])
            config.moltbook_trigger_min_karma = value
            await self.storage.save_user_config(config)
            label = f"{value}" if value > 0 else "OFF"
            await self._answer_callback(bot_token, callback_id, f"Min Karma: {label}", show_alert=True)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_verified":
            config.moltbook_trigger_x_verified = not config.moltbook_trigger_x_verified
            await self.storage.save_user_config(config)
            status = "ON" if config.moltbook_trigger_x_verified else "OFF"
            await self._answer_callback(bot_token, callback_id, f"X Verified: {status}", show_alert=True)
            text, keyboard = self._build_trigger_moltbook_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltbook_followers":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltbook_followers = self._waiting_for_custom_moltbook_followers if hasattr(self, '_waiting_for_custom_moltbook_followers') else {}
            self._waiting_for_custom_moltbook_followers[user_id] = True
            text = """<b>✏️ CUSTOM MIN FOLLOWERS (MOLTBOOK)</b>

Digite o número mínimo de followers Twitter:

<i>Exemplo: 2500 ou 15000</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltbook_followers"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltbook_smart":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltbook_smart = self._waiting_for_custom_moltbook_smart if hasattr(self, '_waiting_for_custom_moltbook_smart') else {}
            self._waiting_for_custom_moltbook_smart[user_id] = True
            text = """<b>✏️ CUSTOM MIN SMART FOLLOWERS (MOLTBOOK)</b>

Digite o número mínimo de smart followers:

<i>Exemplo: 3 ou 25</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltbook_smart"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custom_moltbook_karma":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_custom_moltbook_karma = self._waiting_for_custom_moltbook_karma if hasattr(self, '_waiting_for_custom_moltbook_karma') else {}
            self._waiting_for_custom_moltbook_karma[user_id] = True
            text = """<b>✏️ CUSTOM MIN KARMA (MOLTBOOK)</b>

Digite o número mínimo de karma:

<i>Exemplo: 15 ou 200</i>

Digite <b>0</b> para desativar."""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:moltbook_karma"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:moltbook_handles":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltbook_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "add_moltbook_handle":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_moltbook_handle = self._waiting_for_moltbook_handle if hasattr(self, '_waiting_for_moltbook_handle') else {}
            self._waiting_for_moltbook_handle[user_id] = True
            text = """<b>➕ ADICIONAR HANDLE MOLTBOOK</b>

Digite o @handle do Twitter que deseja monitorar no MoltBook.

<i>Quando este usuário criar um token via MoltBook, 
sua compra será executada automaticamente!</i>

Exemplo: <code>@elonmusk</code> ou <code>vitalik</code>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_moltbook_handle"}]]}
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:cancel_moltbook_handle":
            if hasattr(self, '_waiting_for_moltbook_handle'):
                self._waiting_for_moltbook_handle.pop(user_id, None)
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_moltbook_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("remove_moltbook_handle:"):
            handle = data.split(":", 1)[1]
            await self.storage.remove_moltbook_trigger_handle(user_id, handle)
            await self._answer_callback(bot_token, callback_id, f"❌ @{handle} removido!", show_alert=True)
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_moltbook_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "clear_moltbook_handles":
            count = await self.storage.clear_moltbook_trigger_handles(user_id)
            await self._answer_callback(bot_token, callback_id, f"🗑️ {count} handles removidos!", show_alert=True)
            config = await self.storage.get_user_config(user_id, username)
            text, keyboard = self._build_moltbook_handles_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:wallet":
            await self._answer_callback(bot_token, callback_id)
            
            # Set waiting state for this user
            self._waiting_for_key[user_id] = True
            
            # Send new message (don't edit) asking for the key
            text = """<b>🔐 ENVIE SUA PRIVATE KEY</b>

Cole sua private key na próxima mensagem.
<i>(com ou sem 0x)</i>

<b>O que vai acontecer:</b>
1️⃣ Sua msg será <b>deletada</b> imediatamente
2️⃣ Wallet será derivada automaticamente
3️⃣ Tudo salvo de forma segura

⏳ <b>Aguardando sua key...</b>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_wallet"}]]}
            await self._send_message(bot_token, chat_id, text, keyboard)
        
        elif data == "cfg:cancel_wallet":
            # Cancel waiting for key
            if user_id in self._waiting_for_key:
                del self._waiting_for_key[user_id]
            await self._answer_callback(bot_token, callback_id, "Cancelado")
            await self._edit_message(bot_token, chat_id, message_id, "Cancelado.")
        
        elif data.startswith("setblk:"):
            blk = int(data.split(":")[1])
            config.block_offset = blk
            await self.storage.save_user_config(config)
            fee = get_fee_for_block(blk)
            await self._answer_callback(bot_token, callback_id, f"+{blk} = {fee:.1f}%")
            text, keyboard = self._build_block_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("seteth:"):
            eth = float(data.split(":")[1])
            config.eth_amount = eth
            await self.storage.save_user_config(config)
            await self._answer_callback(bot_token, callback_id, f"{eth} ETH")
            text, keyboard = self._build_eth_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setgwei:"):
            gwei = float(data.split(":")[1])
            config.gwei = gwei
            await self.storage.save_user_config(config)
            await self._answer_callback(bot_token, callback_id, f"{gwei} gwei")
            text, keyboard = self._build_gwei_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "custometh":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_eth[user_id] = True
            text = """<b>💰 CUSTOM ETH</b>

Digite a quantidade de ETH que deseja usar:

<i>Exemplo: 0.15 ou 0.25 ou 2.5</i>

⏳ <b>Aguardando valor...</b>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_eth"}]]}
            await self._send_message(bot_token, chat_id, text, keyboard)
        
        elif data == "cfg:cancel_eth":
            if user_id in self._waiting_for_eth:
                del self._waiting_for_eth[user_id]
            await self._answer_callback(bot_token, callback_id, "Cancelado")
            await self._edit_message(bot_token, chat_id, message_id, "Cancelado.")
        
        elif data == "customgwei":
            await self._answer_callback(bot_token, callback_id)
            self._waiting_for_gwei[user_id] = True
            text = """<b>⛽ CUSTOM GWEI</b>

Digite o gas price em gwei:

<i>Exemplo: 0.05 ou 15 ou 250</i>

⏳ <b>Aguardando valor...</b>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_gwei"}]]}
            await self._send_message(bot_token, chat_id, text, keyboard)
        
        elif data == "cfg:cancel_gwei":
            if user_id in self._waiting_for_gwei:
                del self._waiting_for_gwei[user_id]
            await self._answer_callback(bot_token, callback_id, "Cancelado")
            await self._edit_message(bot_token, chat_id, message_id, "Cancelado.")
        
        elif data == "cfg:tolerance":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_tolerance_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("settol:"):
            tol = int(data.split(":")[1])
            config.block_tolerance = tol
            await self.storage.save_user_config(config)
            label = "Exato" if tol == 0 else f"±{tol}"
            await self._answer_callback(bot_token, callback_id, f"Tolerância: {label}", show_alert=True)
            text, keyboard = self._build_tolerance_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:buyttl":
            await self._answer_callback(bot_token, callback_id)
            text, keyboard = self._build_buyttl_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data.startswith("setttl:"):
            ttl = int(data.split(":")[1])
            config.buy_ttl_minutes = ttl
            await self.storage.save_user_config(config)
            label = f"{ttl} min" if ttl > 0 else "∞ OFF"
            await self._answer_callback(bot_token, callback_id, f"TTL Compra: {label}", show_alert=True)
            text, keyboard = self._build_buyttl_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        else:
            await self._answer_callback(bot_token, callback_id)
    
    async def _handle_set_wallet(
        self,
        bot_token: str,
        chat_id: int,
        user_id: int,
        text: str,
        config: UserConfig,
    ):
        """Handle /setwallet command."""
        wallet = text.split(" ", 1)[1].strip()
        
        if wallet.startswith("0x") and len(wallet) == 42:
            config.wallet_address = wallet
            await self.storage.save_user_config(config)
            await self._send_message(
                bot_token, chat_id,
                f"Wallet configurada: <code>{wallet[:10]}...{wallet[-6:]}</code>"
            )
        else:
            await self._send_message(bot_token, chat_id, "Wallet invalida. Use formato 0x...")
    
    async def _process_wallet_key(
        self,
        bot_token: str,
        chat_id: int,
        message_id: int,
        user_id: int,
        key: str,
        config: UserConfig,
    ):
        """Process wallet private key - derive address, delete message, save config."""
        # SECURITY: Delete the message containing the private key IMMEDIATELY
        if message_id:
            await self._delete_message(bot_token, chat_id, message_id)
        
        # Clean key
        key = key.strip()
        if key.startswith("0x"):
            key = key[2:]
        
        if len(key) == 64 and all(c in '0123456789abcdefABCDEF' for c in key):
            try:
                # Derive wallet address from private key
                account = Account.from_key(bytes.fromhex(key))
                wallet_address = account.address
                
                # Save both private key and derived address
                config.wallet_private_key = key
                config.wallet_address = wallet_address
                await self.storage.save_user_config(config)
                
                await self._send_message(
                    bot_token, chat_id,
                    f"""✅ <b>Wallet configurada!</b>

<code>{wallet_address}</code>

<i>🔒 Sua mensagem foi deletada por segurança.</i>"""
                )
                self.logger.info(f"[CONFIG] User {config.user_id} set wallet {wallet_address}")
                
            except Exception as e:
                self.logger.error(f"[CONFIG] Invalid private key: {e}")
                await self._send_message(
                    bot_token, chat_id,
                    "❌ Private key inválida. Verifique e tente novamente."
                )
        else:
            await self._send_message(
                bot_token, chat_id,
                "❌ Private key inválida (deve ter 64 caracteres hex, com ou sem 0x)"
            )
    
    async def _delete_message(self, bot_token: str, chat_id: int, message_id: int) -> bool:
        """Delete a message (for security - removing private keys from chat)."""
        if not self._session:
            return False
        
        try:
            url = f"{self.API_BASE}{bot_token}/deleteMessage"
            async with self._session.post(url, json={
                "chat_id": chat_id,
                "message_id": message_id,
            }) as resp:
                result = await resp.json()
                if result.get("ok"):
                    self.logger.info(f"[SECURITY] Deleted message {message_id} (private key)")
                    return True
                else:
                    self.logger.warning(f"[SECURITY] Failed to delete message: {result}")
                    return False
        except Exception as e:
            self.logger.error(f"[SECURITY] Delete message error: {e}")
            return False
    
    async def _send_config_menu(self, bot_token: str, chat_id: int, config: UserConfig):
        """Send config menu."""
        text, keyboard = self._build_main_menu(config)
        await self._send_message(bot_token, chat_id, text, keyboard)
    
    async def _send_fees(self, bot_token: str, chat_id: int):
        """Send fee schedule."""
        text = self._build_fees_text()
        await self._send_message(bot_token, chat_id, text)
    
    async def _send_status(self, bot_token: str, chat_id: int, config: UserConfig):
        """Send status."""
        stats = await self.storage.get_stats()
        wallet_status = f"<code>{config.wallet_address}</code>" if config.wallet_address else "NAO CONFIGURADA"
        
        text = f"""<b>STATUS</b>

Wallet: {wallet_status}
Config OK: {'SIM' if config.is_configured() else 'NAO'}
ETH: {config.eth_amount}
Bloco: +{config.block_offset}
Gwei: {config.gwei}
Auto-buy: {'ON' if config.auto_buy else 'OFF'}

Pending: {stats.get('pending_buys_waiting', 0)}"""
        
        await self._send_message(bot_token, chat_id, text)
    
    # =========================================================================
    # MENU BUILDERS
    # =========================================================================
    
    def _build_main_menu(self, config: UserConfig) -> tuple:
        """Build main menu text and keyboard."""
        fee = get_fee_for_block(config.block_offset)
        
        # Build wallet display
        if config.wallet_address:
            wallet_display = f"<code>{config.wallet_address}</code>"
            wallet_btn = "✅ Wallet"
        else:
            wallet_display = "⚠️ <b>NAO CONFIGURADA</b>"
            wallet_btn = "⚠️ Configurar Wallet"
        
        auto_status = "🟢 ON" if config.auto_buy else "🔴 OFF"
        preapprove_status = f"✅ {config.max_eth_approved} ETH" if config.max_eth_approved > 0 else "❌ OFF"
        tolerance_display = f"±{config.block_tolerance}" if config.block_tolerance > 0 else "Exato"
        ttl_display = f"{config.buy_ttl_minutes} min" if config.buy_ttl_minutes > 0 else "∞ OFF"
        
        text = f"""<b>TRADING BOT</b>

<b>Wallet:</b>
{wallet_display}

<b>Config:</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Gwei: <b>{config.gwei}</b>
Auto-buy: <b>{auto_status}</b>
Pre-Approve: <b>{preapprove_status}</b>
Tolerância: <b>{tolerance_display}</b>
TTL Compra: <b>{ttl_display}</b>"""
        
        keyboard = {
            "inline_keyboard": [
                [{"text": wallet_btn, "callback_data": "cfg:wallet"}],
                [
                    {"text": f"ETH: {config.eth_amount}", "callback_data": "cfg:eth"},
                    {"text": f"Bloco: +{config.block_offset}", "callback_data": "cfg:block"},
                ],
                [
                    {"text": f"Gwei: {config.gwei}", "callback_data": "cfg:gwei"},
                    {"text": f"Auto-buy: {auto_status}", "callback_data": "cfg:autobuy"},
                ],
                [{"text": f"💰 Pre-Approve: {preapprove_status}", "callback_data": "cfg:preapprove"}],
                [{"text": f"⏱️ Tolerância: {tolerance_display}", "callback_data": "cfg:tolerance"}],
                [{"text": f"⏰ TTL Compra: {ttl_display}", "callback_data": "cfg:buyttl"}],
                [{"text": "🎯 TRIGGER PRE BANKR", "callback_data": "cfg:triggers"}],
                [{"text": "🦞 TRIGGER MOLTX", "callback_data": "cfg:trigger_moltx"}],
                [{"text": "📖 TRIGGER MOLTBOOK", "callback_data": "cfg:trigger_moltbook"}],
                [
                    {"text": "🎯 Snipers Ativos", "callback_data": "cfg:snipers"},
                    {"text": "📊 Status", "callback_data": "cfg:status"},
                ],
            ]
        }
        
        return text, keyboard
    
    def _build_block_menu(self, config: UserConfig) -> tuple:
        """Build block selection menu."""
        current_fee = get_fee_for_block(config.block_offset)
        
        text = f"""<b>SELECIONAR BLOCO</b>

Quantos blocos esperar após auction expirar.
Quanto mais blocos, menor a fee.

<code>BLOCO     FEE</code>
<code>  +0    66.7%</code>
<code>  +1    58.3%</code>
<code>  +2    50.0%</code>
<code>  +3    41.7%</code>
<code>  +4    33.3%</code>
<code>  +5    25.0%</code>
<code>  +6    16.7%</code>
<code>  +7     8.3%</code>
<code>  +8     4.2%</code>

Atual: <b>+{config.block_offset}</b> ({current_fee:.1f}% fee)"""
        
        # Build buttons - show current selection with checkmark
        buttons = []
        for row_blocks in [[0, 1, 2], [3, 4, 5], [6, 7, 8]]:
            row = []
            for blk in row_blocks:
                fee = FEE_SCHEDULE[blk]
                if blk == config.block_offset:
                    row.append({"text": f"✅ +{blk}", "callback_data": f"setblk:{blk}"})
                else:
                    row.append({"text": f"+{blk} ({fee:.0f}%)", "callback_data": f"setblk:{blk}"})
            buttons.append(row)
        
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_tolerance_menu(self, config: UserConfig) -> tuple:
        """Build block tolerance selection menu."""
        
        text = f"""<b>⏱️ TOLERÂNCIA DE BLOCO</b>

Se o bloco alvo for perdido, quantos blocos
depois ainda pode executar a compra?

<code>±0</code> = Somente bloco exato
<code>±1</code> = 1 bloco de tolerância
<code>±2</code> = 2 blocos de tolerância
<code>±3</code> = 3 blocos

Atual: <b>±{config.block_tolerance}</b>"""
        
        buttons = []
        row = []
        for tol in [0, 1, 2, 3]:
            if tol == config.block_tolerance:
                row.append({"text": f"✅ ±{tol}", "callback_data": f"settol:{tol}"})
            else:
                row.append({"text": f"±{tol}", "callback_data": f"settol:{tol}"})
        buttons.append(row)
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_buyttl_menu(self, config: UserConfig) -> tuple:
        """Build buy TTL selection menu."""
        ttl_display = f"{config.buy_ttl_minutes} min" if config.buy_ttl_minutes > 0 else "∞ OFF"
        
        text = f"""<b>⏰ TTL COMPRA (Tempo Máximo de Espera)</b>

Quanto tempo aguardar o token ir on-chain
após clicar BUY.

Se o token não for deployado nesse tempo,
o buy é automaticamente cancelado.

<code>  5 min</code> = Espera até 5 minutos
<code> 15 min</code> = Espera até 15 minutos
<code> 30 min</code> = Espera até 30 minutos
<code> 60 min</code> = Espera até 1 hora
<code>180 min</code> = Espera até 3 horas
<code>∞ OFF</code>  = Sem limite (espera para sempre)

Atual: <b>{ttl_display}</b>"""
        
        # TTL options in minutes
        ttl_options = [5, 15, 30, 60, 180, 0]
        buttons = []
        
        # First row: 5, 15, 30
        row1 = []
        for ttl in [5, 15, 30]:
            label = f"{ttl} min"
            if ttl == config.buy_ttl_minutes:
                row1.append({"text": f"✅ {label}", "callback_data": f"setttl:{ttl}"})
            else:
                row1.append({"text": label, "callback_data": f"setttl:{ttl}"})
        buttons.append(row1)
        
        # Second row: 60, 180, OFF
        row2 = []
        for ttl in [60, 180]:
            label = f"{ttl} min"
            if ttl == config.buy_ttl_minutes:
                row2.append({"text": f"✅ {label}", "callback_data": f"setttl:{ttl}"})
            else:
                row2.append({"text": label, "callback_data": f"setttl:{ttl}"})
        # OFF option (0)
        if config.buy_ttl_minutes == 0:
            row2.append({"text": "✅ ∞ OFF", "callback_data": "setttl:0"})
        else:
            row2.append({"text": "∞ OFF", "callback_data": "setttl:0"})
        buttons.append(row2)
        
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_eth_menu(self, config: UserConfig) -> tuple:
        """Build ETH amount menu."""
        text = f"""<b>💰 QUANTIDADE ETH</b>

Quanto ETH usar em cada compra.

Atual: <b>{config.eth_amount} ETH</b>

Selecione:"""
        
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
        
        # Custom button
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custometh"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_gwei_menu(self, config: UserConfig) -> tuple:
        """Build gwei menu."""
        text = f"""<b>⛽ GAS PRICE (GWEI)</b>

Gas price para transacoes.

Atual: <b>{config.gwei} gwei</b>

Selecione:"""
        
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
        
        # Custom button
        buttons.append([{"text": "✏️ Custom...", "callback_data": "customgwei"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_preapprove_menu(self, config: UserConfig) -> tuple:
        """Build pre-approve menu."""
        current = config.max_eth_approved
        status = f"✅ {current} ETH" if current > 0 else "❌ OFF"
        
        text = f"""<b>💰 PRE-APPROVE</b>

Aprovar automaticamente compras até um limite de ETH.

Quando ativo, qualquer sniper com valor até o limite será aprovado automaticamente, sem precisar confirmar.

<b>Status:</b> {status}

<b>Como funciona:</b>
1. Defina um limite máximo (ex: 1 ETH)
2. Qualquer buy ≤ 1 ETH = auto-aprovado
3. Buys > 1 ETH = pedem confirmação

Selecione o limite:"""
        
        amounts = [0, 0.1, 0.5, 1, 2, 5, 10]
        buttons = []
        row = []
        
        for eth in amounts:
            if eth == 0:
                label = "❌ OFF"
            else:
                label = f"{eth} ETH"
            
            if eth == current:
                label = f"✅ {label}"
            
            row.append({"text": label, "callback_data": f"setpreapprove:{eth}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_triggers_menu(self, config: UserConfig) -> tuple:
        """Build triggers (@handle) menu."""
        handles = config.sniper_handles
        
        if not handles:
            text = """<b>🎯 TRIGGERS (@handle)</b>

Nenhum trigger ativo.

<b>Como funciona:</b>
Adicione um @handle do Twitter para monitorar.
Quando este usuário criar um token no Bankr,
sua compra será executada automaticamente
usando sua config padrão!

<i>Exemplo: @elonmusk, @cz_binance</i>"""
            
            buttons = [
                [{"text": "➕ Adicionar @handle", "callback_data": "add_trigger"}],
                [{"text": "< Voltar", "callback_data": "cfg:main"}]
            ]
        else:
            lines = [f"<b>🎯 TRIGGERS ATIVOS ({len(handles)})</b>\n"]
            lines.append(f"ETH: <b>{config.eth_amount}</b> | Bloco: <b>+{config.block_offset}</b>\n")
            
            buttons = []
            
            for handle in handles[:15]:  # Max 15 handles shown
                lines.append(f"🔔 <b>@{handle}</b>")
                buttons.append([
                    {"text": f"❌ Remover @{handle}", "callback_data": f"remove_trigger:{handle}"}
                ])
            
            if len(handles) > 15:
                lines.append(f"\n<i>... e mais {len(handles) - 15}</i>")
            
            text = "\n".join(lines)
            
            buttons.append([{"text": "➕ Adicionar @handle", "callback_data": "add_trigger"}])
            if len(handles) > 1:
                buttons.append([{"text": "🗑️ Remover Todos", "callback_data": "clear_triggers"}])
            buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    # =========================================================================
    # TRIGGER MOLTX MENUS
    # =========================================================================
    
    def _build_trigger_moltx_menu(self, config: UserConfig) -> tuple:
        """Build MoltX trigger menu."""
        enabled_status = "🟢 ON" if config.moltx_trigger_enabled else "🔴 OFF"
        followers_display = f"{config.moltx_trigger_min_followers}" if config.moltx_trigger_min_followers > 0 else "OFF"
        moltfollowers_display = f"{config.moltx_trigger_min_moltx_followers}" if config.moltx_trigger_min_moltx_followers > 0 else "OFF"
        smart_display = f"{config.moltx_trigger_min_smart_followers}" if config.moltx_trigger_min_smart_followers > 0 else "OFF"
        verified_status = "🟢 ON" if config.moltx_trigger_x_verified else "🔴 OFF"
        handles_count = len(config.moltx_trigger_handles)
        
        text = f"""<b>🦞 TRIGGER MOLTX</b>

Compra automática quando tokens são criados no MoltX.

<b>Status:</b> {enabled_status}

<b>Filtros (AND):</b>
👤 Min Followers Twitter: <b>{followers_display}</b>
🦞 Min MoltX Followers: <b>{moltfollowers_display}</b>
⭐️ Min Smart Followers: <b>{smart_display}</b>
✅ X Verified Only: <b>{verified_status}</b>
🎯 Handles: <b>{handles_count}</b>

<i>Todos os filtros ativos devem ser satisfeitos (AND).
Se não definir filtros, comprará todos os tokens!</i>"""
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"Status: {enabled_status}", "callback_data": "cfg:moltx_toggle"}],
                [{"text": f"👤 Min Followers: {followers_display}", "callback_data": "cfg:moltx_followers"}],
                [{"text": f"🦞 Min MoltX Followers: {moltfollowers_display}", "callback_data": "cfg:moltx_moltfollowers"}],
                [{"text": f"⭐️ Min Smart Followers: {smart_display}", "callback_data": "cfg:moltx_smart"}],
                [{"text": f"✅ X Verified: {verified_status}", "callback_data": "cfg:moltx_verified"}],
                [{"text": f"🎯 Handles ({handles_count})", "callback_data": "cfg:moltx_handles"}],
                [{"text": "< Voltar", "callback_data": "cfg:main"}],
            ]
        }
        
        return text, keyboard
    
    def _build_moltx_followers_menu(self, config: UserConfig) -> tuple:
        """Build MoltX min followers selection menu."""
        current = config.moltx_trigger_min_followers
        
        text = f"""<b>👤 MIN FOLLOWERS TWITTER (MOLTX)</b>

Mínimo de followers no Twitter para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 100, 500, 1000, 5000, 10000, 50000, 100000]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltx_followers:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltx_followers"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltx"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltx_moltfollowers_menu(self, config: UserConfig) -> tuple:
        """Build MoltX min MoltX followers selection menu."""
        current = config.moltx_trigger_min_moltx_followers
        
        text = f"""<b>🦞 MIN MOLTX FOLLOWERS</b>

Mínimo de followers no MoltX para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 1, 2, 3, 5, 10, 20, 50]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltx_moltfollowers:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltx_moltfollowers"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltx"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltx_smart_menu(self, config: UserConfig) -> tuple:
        """Build MoltX min smart followers selection menu."""
        current = config.moltx_trigger_min_smart_followers
        
        text = f"""<b>⭐️ MIN SMART FOLLOWERS (MOLTX)</b>

Mínimo de smart followers para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 1, 2, 5, 10, 15, 20, 50]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltx_smart:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltx_smart"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltx"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltx_handles_menu(self, config: UserConfig) -> tuple:
        """Build MoltX handles menu."""
        handles = config.moltx_trigger_handles
        
        if not handles:
            text = """<b>🎯 HANDLES MOLTX</b>

Nenhum handle configurado.

<b>Como funciona:</b>
Adicione @handles específicos para snipar.
Se nenhum handle for definido, o trigger
usará apenas os outros filtros.

<i>Exemplo: @elonmusk, @vitalik</i>"""
            
            buttons = [
                [{"text": "➕ Adicionar @handle", "callback_data": "add_moltx_handle"}],
                [{"text": "< Voltar", "callback_data": "cfg:trigger_moltx"}]
            ]
        else:
            lines = [f"<b>🎯 HANDLES MOLTX ({len(handles)})</b>\n"]
            
            buttons = []
            
            for handle in handles[:15]:
                lines.append(f"🔔 <b>@{handle}</b>")
                buttons.append([
                    {"text": f"❌ Remover @{handle}", "callback_data": f"remove_moltx_handle:{handle}"}
                ])
            
            if len(handles) > 15:
                lines.append(f"\n<i>... e mais {len(handles) - 15}</i>")
            
            text = "\n".join(lines)
            
            buttons.append([{"text": "➕ Adicionar @handle", "callback_data": "add_moltx_handle"}])
            if len(handles) > 1:
                buttons.append([{"text": "🗑️ Remover Todos", "callback_data": "clear_moltx_handles"}])
            buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltx"}])
        
        return text, {"inline_keyboard": buttons}
    
    # =========================================================================
    # TRIGGER MOLTBOOK MENUS
    # =========================================================================
    
    def _build_trigger_moltbook_menu(self, config: UserConfig) -> tuple:
        """Build MoltBook trigger menu."""
        enabled_status = "🟢 ON" if config.moltbook_trigger_enabled else "🔴 OFF"
        followers_display = f"{config.moltbook_trigger_min_followers}" if config.moltbook_trigger_min_followers > 0 else "OFF"
        smart_display = f"{config.moltbook_trigger_min_smart_followers}" if config.moltbook_trigger_min_smart_followers > 0 else "OFF"
        karma_display = f"{config.moltbook_trigger_min_karma}" if config.moltbook_trigger_min_karma > 0 else "OFF"
        verified_status = "🟢 ON" if config.moltbook_trigger_x_verified else "🔴 OFF"
        handles_count = len(config.moltbook_trigger_handles)
        
        text = f"""<b>📖 TRIGGER MOLTBOOK</b>

Compra automática quando tokens são criados no MoltBook.

<b>Status:</b> {enabled_status}

<b>Filtros (AND):</b>
👤 Min Followers Twitter: <b>{followers_display}</b>
⭐️ Min Smart Followers: <b>{smart_display}</b>
🏆 Min Karma: <b>{karma_display}</b>
✅ X Verified Only: <b>{verified_status}</b>
🎯 Handles: <b>{handles_count}</b>

<i>Todos os filtros ativos devem ser satisfeitos (AND).
Se não definir filtros, comprará todos os tokens!</i>"""
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"Status: {enabled_status}", "callback_data": "cfg:moltbook_toggle"}],
                [{"text": f"👤 Min Followers: {followers_display}", "callback_data": "cfg:moltbook_followers"}],
                [{"text": f"⭐️ Min Smart Followers: {smart_display}", "callback_data": "cfg:moltbook_smart"}],
                [{"text": f"🏆 Min Karma: {karma_display}", "callback_data": "cfg:moltbook_karma"}],
                [{"text": f"✅ X Verified: {verified_status}", "callback_data": "cfg:moltbook_verified"}],
                [{"text": f"🎯 Handles ({handles_count})", "callback_data": "cfg:moltbook_handles"}],
                [{"text": "< Voltar", "callback_data": "cfg:main"}],
            ]
        }
        
        return text, keyboard
    
    def _build_moltbook_followers_menu(self, config: UserConfig) -> tuple:
        """Build MoltBook min followers selection menu."""
        current = config.moltbook_trigger_min_followers
        
        text = f"""<b>👤 MIN FOLLOWERS TWITTER (MOLTBOOK)</b>

Mínimo de followers no Twitter para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 100, 500, 1000, 5000, 10000, 50000, 100000]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltbook_followers:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltbook_followers"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltbook"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltbook_smart_menu(self, config: UserConfig) -> tuple:
        """Build MoltBook min smart followers selection menu."""
        current = config.moltbook_trigger_min_smart_followers
        
        text = f"""<b>⭐️ MIN SMART FOLLOWERS (MOLTBOOK)</b>

Mínimo de smart followers para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 1, 2, 5, 10, 15, 20, 50]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltbook_smart:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltbook_smart"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltbook"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltbook_karma_menu(self, config: UserConfig) -> tuple:
        """Build MoltBook min karma selection menu."""
        current = config.moltbook_trigger_min_karma
        
        text = f"""<b>🏆 MIN KARMA (MOLTBOOK)</b>

Mínimo de karma no MoltBook para ativar o trigger.

Atual: <b>{current if current > 0 else 'OFF'}</b>"""
        
        options = [0, 1, 5, 10, 25, 50, 100, 500]
        buttons = []
        row = []
        
        for val in options:
            label = str(val) if val > 0 else "OFF"
            if val == current:
                label = f"✅ {label}"
            row.append({"text": label, "callback_data": f"setmoltbook_karma:{val}"})
            if len(row) == 4:
                buttons.append(row)
                row = []
        
        if row:
            buttons.append(row)
        buttons.append([{"text": "✏️ Custom...", "callback_data": "custom_moltbook_karma"}])
        buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltbook"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_moltbook_handles_menu(self, config: UserConfig) -> tuple:
        """Build MoltBook handles menu."""
        handles = config.moltbook_trigger_handles
        
        if not handles:
            text = """<b>🎯 HANDLES MOLTBOOK</b>

Nenhum handle configurado.

<b>Como funciona:</b>
Adicione @handles específicos para snipar.
Se nenhum handle for definido, o trigger
usará apenas os outros filtros.

<i>Exemplo: @elonmusk, @vitalik</i>"""
            
            buttons = [
                [{"text": "➕ Adicionar @handle", "callback_data": "add_moltbook_handle"}],
                [{"text": "< Voltar", "callback_data": "cfg:trigger_moltbook"}]
            ]
        else:
            lines = [f"<b>🎯 HANDLES MOLTBOOK ({len(handles)})</b>\n"]
            
            buttons = []
            
            for handle in handles[:15]:
                lines.append(f"🔔 <b>@{handle}</b>")
                buttons.append([
                    {"text": f"❌ Remover @{handle}", "callback_data": f"remove_moltbook_handle:{handle}"}
                ])
            
            if len(handles) > 15:
                lines.append(f"\n<i>... e mais {len(handles) - 15}</i>")
            
            text = "\n".join(lines)
            
            buttons.append([{"text": "➕ Adicionar @handle", "callback_data": "add_moltbook_handle"}])
            if len(handles) > 1:
                buttons.append([{"text": "🗑️ Remover Todos", "callback_data": "clear_moltbook_handles"}])
            buttons.append([{"text": "< Voltar", "callback_data": "cfg:trigger_moltbook"}])
        
        return text, {"inline_keyboard": buttons}
    
    def _build_fees_text(self) -> str:
        """Build fee schedule text."""
        lines = ["<b>FEE SCHEDULE COMPLETO</b>\n"]
        lines.append("Quando token e criado:\n")
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
    
    async def _build_snipers_menu(self, user_id: int, config: UserConfig) -> tuple:
        """Build active snipers menu."""
        # Get all pending buys for this user
        buy_keys = await self.storage.redis.smembers(f"buys_by_user:{user_id}")
        
        buys = []
        for buy_key in buy_keys:
            # buy_key is "user_id:tweet_id"
            parts = buy_key.split(":", 1)
            if len(parts) == 2:
                tweet_id = parts[1]
                buy = await self.storage.get_pending_buy(user_id, tweet_id)
                if buy and buy.status in ["APPROVED", "WAITING", "PENDING_CONFIRM"]:
                    buys.append(buy)
        
        auto_status = "🟢 ON" if config.auto_buy else "🔴 OFF"
        
        if not buys:
            text = f"""<b>🎯 SNIPERS ATIVOS</b>

Nenhum sniper ativo no momento.

Auto-buy: <b>{auto_status}</b>

<i>Seus snipers aparecerao aqui quando voce curtir um tweet com token.</i>"""
            
            buttons = [
                [{"text": f"Auto-buy: {auto_status}", "callback_data": "cfg:autobuy"}],
                [{"text": "< Voltar", "callback_data": "cfg:main"}]
            ]
        else:
            lines = [f"<b>🎯 SNIPERS ATIVOS ({len(buys)})</b>\n"]
            lines.append(f"Auto-buy: <b>{auto_status}</b>\n")
            
            buttons = []
            
            for buy in buys[:10]:  # Max 10 snipers shown
                status_emoji = {
                    "APPROVED": "🟡",
                    "WAITING": "⏳",
                    "PENDING_CONFIRM": "❓",
                }.get(buy.status, "❔")
                
                symbol = buy.token_symbol or "???"
                lines.append(f"{status_emoji} <b>{symbol}</b> - {buy.eth_amount} ETH")
                lines.append(f"   Status: {buy.status}")
                
                # Cancel button for each sniper
                buttons.append([
                    {"text": f"❌ Cancelar {symbol}", "callback_data": f"cancel_sniper:{buy.tweet_id}"}
                ])
            
            text = "\n".join(lines)
            
            # Cancel all button (only if there are multiple snipers)
            if len(buys) > 1:
                buttons.append([{"text": "🗑️ CANCELAR TODOS", "callback_data": "cancel_all_snipers"}])
            
            buttons.append([{"text": f"Auto-buy: {auto_status}", "callback_data": "cfg:autobuy"}])
            buttons.append([{"text": "< Voltar", "callback_data": "cfg:main"}])
        
        return text, {"inline_keyboard": buttons}
    
    # =========================================================================
    # TELEGRAM API HELPERS
    # =========================================================================
    
    async def _answer_callback(
        self,
        bot_token: str,
        callback_id: str,
        text: str = None,
        show_alert: bool = False,
    ):
        """Answer callback query."""
        url = f"{self.API_BASE}{bot_token}/answerCallbackQuery"
        
        payload = {
            "callback_query_id": callback_id,
            "show_alert": show_alert,
        }
        
        if text:
            payload["text"] = text
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    self.logger.error(f"[ANSWER CALLBACK] Failed: {data}")
                else:
                    self.logger.debug(f"[ANSWER CALLBACK] OK for {callback_id[:20]}...")
        except Exception as e:
            self.logger.error(f"[ANSWER CALLBACK] Exception: {e}")
    
    async def _send_message(
        self,
        bot_token: str,
        chat_id: int,
        text: str,
        reply_markup: Dict = None,
    ) -> Optional[int]:
        """Send message."""
        url = f"{self.API_BASE}{bot_token}/sendMessage"
        
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data["result"]["message_id"]
        except:
            pass
        
        return None
    
    async def _edit_message(
        self,
        bot_token: str,
        chat_id: int,
        message_id: int,
        text: str,
        reply_markup: Dict = None,
    ) -> bool:
        """Edit message."""
        url = f"{self.API_BASE}{bot_token}/editMessageText"
        
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                return data.get("ok", False)
        except:
            return False
    
    async def _update_reply_markup(
        self,
        bot_token: str,
        chat_id: int,
        message_id: int,
        reply_markup: Dict,
    ) -> bool:
        """Update message reply markup (keyboard only)."""
        url = f"{self.API_BASE}{bot_token}/editMessageReplyMarkup"
        
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": reply_markup,
        }
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok", False):
                    self.logger.warning(f"[MARKUP] API error: {data.get('description', 'unknown')}")
                return data.get("ok", False)
        except Exception as e:
            self.logger.warning(f"[MARKUP] Exception: {e}")
            return False


# Entry point
if __name__ == "__main__":
    TelegramCallbackWorker.main()
