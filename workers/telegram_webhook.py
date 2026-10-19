"""
Telegram Webhook Worker

Receives callback updates from Telegram via HTTP webhooks.
Much faster and more reliable than polling.

How it works:
1. Starts an HTTP server on port 8443
2. Telegram sends POST requests when users click buttons
3. We process the callback and respond immediately

Setup:
1. Run ngrok: ngrok http 8443
2. Register webhook: curl "https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://xxx.ngrok.io/webhook/{TOKEN}"
"""

import os
import json
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from aiohttp import web

from workers.base import BaseWorker
from shared.pubsub import Channels
from config.fees import get_fee_for_block


class TelegramWebhookWorker(BaseWorker):
    """Handle Telegram callbacks via webhooks (not polling)."""
    
    def __init__(self):
        super().__init__()
        
        # Bot tokens - for SENDING messages
        self.prebankr_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.trading_token = os.getenv("TELEGRAM_TRADING_BOT_TOKEN", "")
        self.clawnch_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")
        self.callback_token = os.getenv("TELEGRAM_CALLBACK_BOT_TOKEN", "")
        
        # Map tokens to names for logging
        self.token_names = {}
        if self.prebankr_token:
            self.token_names[self.prebankr_token] = "prebankr"
        if self.trading_token:
            self.token_names[self.trading_token] = "trading"
        if self.clawnch_token:
            self.token_names[self.clawnch_token] = "clawnch"
        if self.callback_token:
            self.token_names[self.callback_token] = "callback"
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._runner: Optional[web.AppRunner] = None
        
        # Track users waiting for input
        self._waiting_for_key: Dict[int, bool] = {}
        self._waiting_for_eth: Dict[int, bool] = {}
        self._waiting_for_gwei: Dict[int, bool] = {}
        self._waiting_for_trigger: Dict[int, bool] = {}
        
        # Stats
        self.stats.update({
            "webhooks_received": 0,
            "callbacks_handled": 0,
            "commands_handled": 0,
            "likes_added": 0,
            "buys_approved": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
    
    async def teardown(self):
        """Cleanup resources."""
        if self._runner:
            await self._runner.cleanup()
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Start the webhook HTTP server."""
        app = web.Application()
        
        # Route: POST /webhook/{token} - receives updates from Telegram
        app.router.add_post('/webhook/{token}', self.handle_webhook)
        
        # Health check route
        app.router.add_get('/health', self.handle_health)
        
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        
        port = int(os.getenv("WEBHOOK_PORT", "8443"))
        site = web.TCPSite(self._runner, '0.0.0.0', port)
        await site.start()
        
        self.logger.info(f"🚀 Webhook server started on port {port}")
        self.logger.info(f"📡 Waiting for Telegram updates...")
        
        # Keep running
        while True:
            await asyncio.sleep(60)
            self.logger.debug(f"Webhook stats: {self.stats}")
    
    async def handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({"status": "ok", "stats": self.stats})
    
    async def handle_webhook(self, request: web.Request) -> web.Response:
        """Handle incoming webhook from Telegram."""
        try:
            token = request.match_info.get('token', '')
            bot_name = self.token_names.get(token, 'unknown')
            
            update = await request.json()
            self.stats["webhooks_received"] += 1
            
            # Handle callback query (button click)
            if 'callback_query' in update:
                callback = update['callback_query']
                await self._handle_callback(bot_name, token, callback)
            
            # Handle message (commands, text input)
            elif 'message' in update:
                message = update['message']
                await self._handle_message(bot_name, token, message)
            
            # Always return 200 OK quickly
            return web.Response(text='OK')
            
        except Exception as e:
            self.logger.error(f"Webhook error: {e}")
            # Still return 200 to prevent Telegram from retrying
            return web.Response(text='OK')
    
    # =========================================================================
    # CALLBACK HANDLERS (copied from telegram_callback.py)
    # =========================================================================
    
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
        
        # INSTANT: Answer callback immediately to stop "thinking" spinner
        await self._answer_callback(bot_token, callback_id)
        
        # Route to appropriate handler
        if data.startswith("like:"):
            await self._handle_like(bot_token, data, user_id, username, message_id, chat_id)
        
        elif data.startswith("buy:"):
            await self._handle_buy_click(bot_token, data, user_id, username, chat_id)
        
        elif data.startswith("buyconfirm:"):
            await self._handle_buy_confirm(bot_token, data, user_id, username, message_id, chat_id)
        
        elif data.startswith("buy_preclawnch:"):
            await self._handle_buy_preclawnch(bot_token, data, user_id, username, chat_id)
        
        elif data.startswith("buycancel:"):
            await self._handle_buy_cancel(bot_token, data, user_id, message_id, chat_id)
        
        elif data.startswith("cfg:") or data.startswith("set") or data.startswith("cancel_sniper:") or data == "cancel_all_snipers":
            await self._handle_config(bot_token, data, user_id, username, message_id, chat_id)
    
    async def _handle_message(
        self,
        bot_name: str,
        bot_token: str,
        message: Dict[str, Any],
    ):
        """Handle text messages (commands, wallet keys, etc.)."""
        text = message.get("text", "")
        user = message.get("from", {})
        user_id = user.get("id", 0)
        username = user.get("username", "")
        chat_id = message.get("chat", {}).get("id", user_id)
        message_id = message.get("message_id", 0)
        
        if not text:
            return
        
        self.logger.info(f"[{bot_name}] Message @{username}: {text[:50]}")
        self.stats["commands_handled"] += 1
        
        # Handle commands
        if text.startswith("/start"):
            await self._send_start_message(bot_token, chat_id, username)
        
        elif text.startswith("/config"):
            await self._send_config_menu(bot_token, chat_id, user_id, username)
        
        elif text.startswith("/help"):
            await self._send_help_message(bot_token, chat_id)
        
        # Handle wallet key input
        elif user_id in self._waiting_for_key:
            await self._process_wallet_key(bot_token, chat_id, message_id, user_id, username, text)
        
        # Handle custom ETH input
        elif user_id in self._waiting_for_eth:
            await self._process_custom_eth(bot_token, chat_id, user_id, username, text)
        
        # Handle custom GWEI input
        elif user_id in self._waiting_for_gwei:
            await self._process_custom_gwei(bot_token, chat_id, user_id, username, text)
        
        # Handle trigger handle input
        elif user_id in self._waiting_for_trigger:
            await self._process_trigger_handle(bot_token, chat_id, user_id, username, text)
    
    # =========================================================================
    # LIKE HANDLER
    # =========================================================================
    
    async def _handle_like(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int,
        chat_id: int,
    ):
        """Handle like button click."""
        identifier = data.split(":", 1)[1]
        
        if await self.storage.has_liked(identifier, user_id):
            count = await self.storage.remove_like(identifier, user_id)
            self.logger.info(f"[UNLIKE] @{username}")
        else:
            count = await self.storage.add_like(identifier, user_id, username)
            self.stats["likes_added"] += 1
            self.logger.info(f"[LIKE] @{username}")
        
        # Update the like button count in the message
        try:
            # Get current message to preserve other buttons
            # For now, just update the like count in place
            pass  # TODO: Update keyboard with new count
        except Exception as e:
            self.logger.error(f"Error updating like button: {e}")
    
    # =========================================================================
    # BUY HANDLERS
    # =========================================================================
    
    async def _handle_buy_click(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        username: str,
        chat_id: int,
    ):
        """Handle BUY button click."""
        identifier = data.split(":", 1)[1]
        trading_bot = self.trading_token or bot_token
        
        config = await self.storage.get_user_config(user_id, username)
        
        if not config.is_configured():
            await self._send_message(
                trading_bot, user_id,
                f"<b>⚠️ CONFIG INCOMPLETA</b>\n\nFalta configurar: {', '.join(config.get_missing_config())}",
                {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
            )
            return
        
        # Check if already has pending buy
        existing = await self.storage.get_pending_buy(user_id, identifier)
        if existing:
            await self._send_message(
                trading_bot, user_id,
                "<b>⚠️ Você já tem um BUY registrado para este token!</b>"
            )
            return
        
        # Detect source type
        is_preclawnch = "-" in identifier or identifier.startswith("thread:")
        
        # Get token info
        if is_preclawnch:
            token_info = await self.storage.get_preclawnch_token(identifier)
            token_symbol = token_info.get("token_symbol", "Token") if token_info else "Token"
            from_username = token_info.get("agent_name", "") if token_info else ""
            platform = token_info.get("platform", "") if token_info else ""
        else:
            token_info = await self.storage.get_token_info(identifier)
            token_symbol = token_info.token_symbol if token_info else "Token"
            from_username = token_info.from_username if token_info else ""
            platform = ""
        
        # Create pending buy
        buy = await self.storage.approve_buy(user_id, identifier, username, token_symbol)
        fee = get_fee_for_block(config.block_offset)
        
        # Send confirmation
        auto_status = "🟢 AUTO" if config.auto_buy else "🟡 MANUAL"
        auto_note = "Executará automaticamente quando on-chain." if config.auto_buy else "Você receberá confirmação quando on-chain."
        
        header = "✅ PRE-CLAWNCH BUY REGISTRADO!" if is_preclawnch else "✅ BUY REGISTRADO!"
        
        msg = f"""<b>{header}</b>

Token: <b>{token_symbol}</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Modo: <b>{auto_status}</b>

{auto_note}"""
        
        await self._send_message(
            trading_bot, user_id, msg,
            {"inline_keyboard": [[{"text": "❌ Cancelar Buy", "callback_data": f"buycancel:{identifier}"}]]}
        )
        
        self.stats["buys_approved"] += 1
        flow_type = "PRE-CLAWNCH" if is_preclawnch else "PRE-BANKR"
        self.logger.info(f"[{flow_type} BUY REGISTERED] @{username} -> {token_symbol}")
    
    async def _handle_buy_preclawnch(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        username: str,
        chat_id: int,
    ):
        """Handle Pre-Clawnch buy button click."""
        wallet_prefix = data.split(":", 1)[1] if ":" in data else ""
        trading_bot = self.trading_token or bot_token
        
        config = await self.storage.get_user_config(user_id, username)
        
        if not config.is_configured():
            await self._send_message(
                trading_bot, user_id,
                f"<b>⚠️ CONFIG INCOMPLETA</b>\n\nFalta configurar: {', '.join(config.get_missing_config())}",
                {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
            )
            return
        
        # Look up token by wallet prefix
        preclawnch_token = await self.storage.get_preclawnch_token_by_wallet(wallet_prefix)
        if not preclawnch_token:
            await self._send_message(trading_bot, user_id, "❌ Token not found")
            return
        
        post_id = preclawnch_token.get("post_id", "")
        token_symbol = preclawnch_token.get("token_symbol", "Token")
        platform = preclawnch_token.get("platform", "")
        
        # Check existing
        existing = await self.storage.get_pending_buy(user_id, post_id)
        if existing:
            await self._send_message(
                trading_bot, user_id,
                "<b>⚠️ Você já tem um BUY registrado para este token!</b>"
            )
            return
        
        # Create pending buy
        buy = await self.storage.approve_buy(user_id, post_id, username, token_symbol)
        fee = get_fee_for_block(config.block_offset)
        
        auto_status = "🟢 AUTO" if config.auto_buy else "🟡 MANUAL"
        
        msg = f"""<b>✅ PRE-CLAWNCH BUY REGISTRADO!</b>

Token: <b>{token_symbol}</b>
Platform: <b>{platform}</b>
ETH: <b>{config.eth_amount}</b>
Bloco: <b>+{config.block_offset}</b> ({fee:.1f}% fee)
Modo: <b>{auto_status}</b>"""
        
        await self._send_message(
            trading_bot, user_id, msg,
            {"inline_keyboard": [[{"text": "❌ Cancelar Buy", "callback_data": f"buycancel:{post_id}"}]]}
        )
        
        self.stats["buys_approved"] += 1
        self.logger.info(f"[PRE-CLAWNCH BUY REGISTERED] @{username} -> {token_symbol}")
    
    async def _handle_buy_confirm(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int,
        chat_id: int,
    ):
        """Handle buy confirmation."""
        tweet_id = data.split(":", 1)[1]
        
        config = await self.storage.get_user_config(user_id, username)
        if not config.is_configured():
            await self._send_message(bot_token, chat_id, "⚠️ Configure wallet primeiro!")
            return
        
        buy = await self.storage.get_pending_buy(user_id, tweet_id)
        if not buy:
            await self._send_message(bot_token, chat_id, "❌ Buy não encontrado")
            return
        
        if buy.status not in ["PENDING_CONFIRM", "APPROVED"]:
            await self._send_message(bot_token, chat_id, f"❌ Status inválido: {buy.status}")
            return
        
        await self.storage.update_buy_status(buy, "WAITING")
        
        await self._edit_message(
            bot_token, chat_id, message_id,
            f"""<b>✅ CONFIRMADO!</b>

Token: <b>{buy.token_symbol}</b>
Contract: <code>{buy.token_address}</code>
ETH: <b>{buy.eth_amount}</b>
Bloco alvo: <b>{buy.target_block}</b>

⏳ Aguardando bloco para executar..."""
        )
        
        self.logger.info(f"[BUY CONFIRMED] @{username} -> {buy.token_symbol}")
    
    async def _handle_buy_cancel(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        message_id: int,
        chat_id: int,
    ):
        """Handle buy cancellation."""
        tweet_id = data.split(":", 1)[1] if ":" in data else ""
        
        if tweet_id:
            success = await self.storage.cancel_buy(user_id, tweet_id)
            if success:
                self.logger.info(f"[BUY CANCELLED] User {user_id} -> {tweet_id}")
        
        await self._edit_message(bot_token, chat_id, message_id, "❌ <b>Compra cancelada.</b>")
    
    # =========================================================================
    # CONFIG HANDLER (simplified)
    # =========================================================================
    
    async def _handle_config(
        self,
        bot_token: str,
        data: str,
        user_id: int,
        username: str,
        message_id: int,
        chat_id: int,
    ):
        """Handle config menu callbacks."""
        config = await self.storage.get_user_config(user_id, username)
        
        # Route to specific config handler based on data
        # This is simplified - full implementation should match telegram_callback.py
        
        if data in ["cfg:main", "cfg:back"]:
            text, keyboard = self._build_main_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:autobuy":
            config.auto_buy = not config.auto_buy
            await self.storage.save_user_config(config)
            text, keyboard = self._build_main_menu(config)
            await self._edit_message(bot_token, chat_id, message_id, text, keyboard)
        
        elif data == "cfg:wallet":
            self._waiting_for_key[user_id] = True
            text = """<b>🔐 ENVIE SUA PRIVATE KEY</b>

Cole sua private key na próxima mensagem.
<i>(com ou sem 0x)</i>

⏳ <b>Aguardando sua key...</b>"""
            keyboard = {"inline_keyboard": [[{"text": "❌ Cancelar", "callback_data": "cfg:cancel_wallet"}]]}
            await self._send_message(bot_token, chat_id, text, keyboard)
        
        elif data == "cfg:cancel_wallet":
            self._waiting_for_key.pop(user_id, None)
            await self._edit_message(bot_token, chat_id, message_id, "Cancelado.")
        
        # Add more config handlers as needed...
    
    # =========================================================================
    # MENU BUILDERS
    # =========================================================================
    
    def _build_main_menu(self, config):
        """Build main config menu."""
        wallet_status = f"✅ {config.wallet_address[:8]}..." if config.wallet_address else "❌ Não configurada"
        auto_status = "🟢 ON" if config.auto_buy else "🔴 OFF"
        
        text = f"""<b>⚙️ CONFIGURAÇÕES</b>

<b>Wallet:</b> {wallet_status}
<b>ETH:</b> {config.eth_amount}
<b>Bloco:</b> +{config.block_offset}
<b>Auto-buy:</b> {auto_status}"""
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔐 Wallet", "callback_data": "cfg:wallet"}],
                [{"text": f"💰 ETH: {config.eth_amount}", "callback_data": "cfg:eth"}],
                [{"text": f"📦 Bloco: +{config.block_offset}", "callback_data": "cfg:block"}],
                [{"text": f"🤖 Auto-buy: {auto_status}", "callback_data": "cfg:autobuy"}],
            ]
        }
        
        return text, keyboard
    
    # =========================================================================
    # TELEGRAM API HELPERS
    # =========================================================================
    
    async def _answer_callback(self, bot_token: str, callback_id: str, text: str = "", show_alert: bool = False):
        """Answer callback query to stop 'thinking' indicator."""
        url = f"https://api.telegram.org/bot{bot_token}/answerCallbackQuery"
        payload = {
            "callback_query_id": callback_id,
            "cache_time": 2,
        }
        if text:
            payload["text"] = text
            payload["show_alert"] = show_alert
        
        try:
            async with self._session.post(url, json=payload) as resp:
                pass
        except Exception as e:
            self.logger.error(f"Answer callback error: {e}")
    
    async def _send_message(self, bot_token: str, chat_id: int, text: str, keyboard: dict = None):
        """Send a message."""
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        
        try:
            async with self._session.post(url, json=payload) as resp:
                return await resp.json()
        except Exception as e:
            self.logger.error(f"Send message error: {e}")
    
    async def _edit_message(self, bot_token: str, chat_id: int, message_id: int, text: str, keyboard: dict = None):
        """Edit a message."""
        url = f"https://api.telegram.org/bot{bot_token}/editMessageText"
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        
        try:
            async with self._session.post(url, json=payload) as resp:
                return await resp.json()
        except Exception as e:
            self.logger.error(f"Edit message error: {e}")
    
    async def _delete_message(self, bot_token: str, chat_id: int, message_id: int):
        """Delete a message."""
        url = f"https://api.telegram.org/bot{bot_token}/deleteMessage"
        payload = {"chat_id": chat_id, "message_id": message_id}
        
        try:
            async with self._session.post(url, json=payload) as resp:
                pass
        except Exception as e:
            self.logger.error(f"Delete message error: {e}")
    
    # =========================================================================
    # HELPER HANDLERS (simplified)
    # =========================================================================
    
    async def _send_start_message(self, bot_token: str, chat_id: int, username: str):
        """Send /start welcome message."""
        text = f"""<b>🚀 Bem-vindo ao Bankr Monitor!</b>

Olá @{username}!

Use /config para configurar sua wallet e preferências."""
        
        keyboard = {"inline_keyboard": [[{"text": "⚙️ Configurar", "callback_data": "cfg:main"}]]}
        await self._send_message(bot_token, chat_id, text, keyboard)
    
    async def _send_config_menu(self, bot_token: str, chat_id: int, user_id: int, username: str):
        """Send config menu."""
        config = await self.storage.get_user_config(user_id, username)
        text, keyboard = self._build_main_menu(config)
        await self._send_message(bot_token, chat_id, text, keyboard)
    
    async def _send_help_message(self, bot_token: str, chat_id: int):
        """Send /help message."""
        text = """<b>📚 AJUDA</b>

<b>Comandos:</b>
/start - Iniciar bot
/config - Configurar wallet e preferências
/help - Esta mensagem

<b>Como usar:</b>
1. Configure sua wallet em /config
2. Clique em <b>Buy</b> nas notificações de token
3. O bot executará automaticamente quando o token for deployado"""
        
        await self._send_message(bot_token, chat_id, text)
    
    async def _process_wallet_key(self, bot_token: str, chat_id: int, message_id: int, user_id: int, username: str, text: str):
        """Process wallet private key input."""
        from eth_account import Account
        
        # Delete the message with the key immediately
        await self._delete_message(bot_token, chat_id, message_id)
        
        self._waiting_for_key.pop(user_id, None)
        
        try:
            key = text.strip()
            if not key.startswith("0x"):
                key = "0x" + key
            
            account = Account.from_key(key)
            address = account.address
            
            config = await self.storage.get_user_config(user_id, username)
            config.wallet_address = address
            config.wallet_private_key = key
            await self.storage.save_user_config(config)
            
            await self._send_message(
                bot_token, chat_id,
                f"<b>✅ Wallet configurada!</b>\n\nEndereço: <code>{address}</code>",
                {"inline_keyboard": [[{"text": "⚙️ Voltar ao Menu", "callback_data": "cfg:main"}]]}
            )
            
            self.logger.info(f"[WALLET SET] @{username} -> {address[:10]}...")
            
        except Exception as e:
            await self._send_message(
                bot_token, chat_id,
                f"<b>❌ Erro ao processar key:</b> {str(e)}",
                {"inline_keyboard": [[{"text": "🔄 Tentar novamente", "callback_data": "cfg:wallet"}]]}
            )
    
    async def _process_custom_eth(self, bot_token: str, chat_id: int, user_id: int, username: str, text: str):
        """Process custom ETH amount input."""
        self._waiting_for_eth.pop(user_id, None)
        
        try:
            eth = float(text.strip())
            if eth <= 0 or eth > 100:
                raise ValueError("ETH deve ser entre 0 e 100")
            
            config = await self.storage.get_user_config(user_id, username)
            config.eth_amount = eth
            await self.storage.save_user_config(config)
            
            await self._send_message(
                bot_token, chat_id,
                f"<b>✅ ETH configurado: {eth}</b>",
                {"inline_keyboard": [[{"text": "⚙️ Voltar ao Menu", "callback_data": "cfg:main"}]]}
            )
            
        except Exception as e:
            await self._send_message(bot_token, chat_id, f"<b>❌ Valor inválido:</b> {str(e)}")
    
    async def _process_custom_gwei(self, bot_token: str, chat_id: int, user_id: int, username: str, text: str):
        """Process custom GWEI input."""
        self._waiting_for_gwei.pop(user_id, None)
        
        try:
            gwei = float(text.strip())
            if gwei < 0:
                raise ValueError("GWEI deve ser positivo")
            
            config = await self.storage.get_user_config(user_id, username)
            config.gwei = gwei
            await self.storage.save_user_config(config)
            
            await self._send_message(
                bot_token, chat_id,
                f"<b>✅ GWEI configurado: {gwei}</b>",
                {"inline_keyboard": [[{"text": "⚙️ Voltar ao Menu", "callback_data": "cfg:main"}]]}
            )
            
        except Exception as e:
            await self._send_message(bot_token, chat_id, f"<b>❌ Valor inválido:</b> {str(e)}")
    
    async def _process_trigger_handle(self, bot_token: str, chat_id: int, user_id: int, username: str, text: str):
        """Process trigger handle input."""
        self._waiting_for_trigger.pop(user_id, None)
        
        handle = text.strip().lstrip("@").lower()
        
        if not handle:
            await self._send_message(bot_token, chat_id, "<b>❌ Handle inválido</b>")
            return
        
        await self.storage.add_sniper_handle(user_id, handle)
        
        await self._send_message(
            bot_token, chat_id,
            f"<b>✅ Trigger adicionado: @{handle}</b>",
            {"inline_keyboard": [[{"text": "⚙️ Voltar ao Menu", "callback_data": "cfg:triggers"}]]}
        )


# Entry point
if __name__ == "__main__":
    import asyncio
    from workers.base import run_worker
    asyncio.run(run_worker(TelegramWebhookWorker))
