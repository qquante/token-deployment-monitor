"""
Moltbook Tracker Worker

Monitors Moltbook PRE-CLAWNCHs and forwards matching tokens to tracking group.

Responsibilities:
- Poll Telegram commands in tracking group
- Manage track list (twitter, ticker, name)
- Subscribe to preclawnch:moltbook channel
- Forward matching tokens to tracking group

Commands:
- /trackmoltbook twitter @handle
- /trackmoltbook ticker $SYMBOL
- /trackmoltbook name agent_name
- /listmolt - List all tracks with IDs
- /removemolt{id} - Remove track by ID
- /infomolt - Status info
"""

import os
import re
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List

from workers.base import BaseWorker
from shared.pubsub import Channels


class MoltbookTrackerWorker(BaseWorker):
    """Monitors Moltbook PRE-CLAWNCHs and forwards matching tokens."""
    
    WORKER_NAME = "moltbook-tracker"
    API_BASE = "https://api.telegram.org/bot"
    
    # Target group for tracker
    TRACKER_GROUP_ID = -1002207322576
    
    def __init__(self):
        super().__init__()
        
        # Bot token (dedicated tracker bot - separate from preclawnch to avoid 409 conflict)
        self.bot_token = os.getenv("TELEGRAM_TRACKER_BOT_TOKEN", "")
        if not self.bot_token:
            raise ValueError("TELEGRAM_TRACKER_BOT_TOKEN not set!")
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Polling state
        self._last_update_id: Optional[int] = None
        
        # Stats
        self.stats.update({
            "tracks_matched": 0,
            "messages_forwarded": 0,
            "commands_handled": 0,
        })
    
    async def setup(self):
        """Initialize HTTP session."""
        await super().setup()
        self._session = aiohttp.ClientSession()
        self.logger.info(f"[{self.worker_id}] Tracker group: {self.TRACKER_GROUP_ID}")
    
    async def teardown(self):
        """Close HTTP session."""
        if self._session:
            await self._session.close()
        await super().teardown()
    
    async def run(self):
        """Main worker loop."""
        # Send startup message
        await self._send_startup_message()
        
        # Run parallel tasks
        await asyncio.gather(
            self._poll_commands(),
            self._listen_preclawnch(),
            return_exceptions=True,
        )
    
    # =========================================================================
    # STARTUP
    # =========================================================================
    
    async def _send_startup_message(self):
        """Send startup notification to tracker group."""
        tracks = await self.storage.list_molttracks()
        
        msg = (
            "🔵 <b>Monitorando Moltbook</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📍 <b>Comandos:</b>\n"
            "• <code>/trackmoltbook twitter @handle</code>\n"
            "• <code>/trackmoltbook ticker $SYMBOL</code>\n"
            "• <code>/trackmoltbook name agent_name</code>\n"
            "• <code>/listmolt</code> - Ver tracks ativos\n"
            "• <code>/removemolt{id}</code> - Remover track\n"
            "• <code>/infomolt</code> - Status\n\n"
            f"📊 <b>Tracks ativos:</b> {len(tracks)}"
        )
        
        await self._send_message(self.TRACKER_GROUP_ID, msg)
        self.logger.info(f"[TRACKER] Startup message sent to {self.TRACKER_GROUP_ID}")
    
    # =========================================================================
    # TELEGRAM POLLING
    # =========================================================================
    
    async def _poll_commands(self):
        """Poll Telegram updates for commands."""
        self.logger.info(f"[TRACKER] Starting poll loop for group {self.TRACKER_GROUP_ID}")
        
        while self._running:
            try:
                updates = await self._get_updates(
                    offset=self._last_update_id + 1 if self._last_update_id else None,
                )
                
                for update in updates:
                    self._last_update_id = update.get("update_id", self._last_update_id)
                    
                    # Handle message
                    message = update.get("message")
                    if message:
                        await self._handle_message(message)
                
                await asyncio.sleep(1)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"[TRACKER] Poll error: {e}")
                self.stats["errors"] += 1
                await asyncio.sleep(5)
    
    async def _get_updates(
        self,
        offset: Optional[int] = None,
        timeout: int = 30,
    ) -> List[Dict[str, Any]]:
        """Get updates from Telegram."""
        url = f"{self.API_BASE}{self.bot_token}/getUpdates"
        
        payload = {
            "timeout": timeout,
            "allowed_updates": ["message"],
        }
        
        if offset:
            payload["offset"] = offset
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if data.get("ok"):
                    return data.get("result", [])
                return []
        except Exception as e:
            self.logger.error(f"[TRACKER] Get updates error: {e}")
            return []
    
    async def _handle_message(self, message: Dict[str, Any]):
        """Handle incoming message."""
        chat_id = message.get("chat", {}).get("id", 0)
        text = message.get("text", "").strip()
        
        # Only process from tracker group
        if chat_id != self.TRACKER_GROUP_ID:
            return
        
        if not text or not text.startswith("/"):
            return
        
        self.logger.info(f"[TRACKER] Command: {text}")
        self.stats["commands_handled"] += 1
        
        # Route commands
        if text.startswith("/trackmoltbook "):
            await self._handle_trackmoltbook(text)
        
        elif text == "/listmolt":
            await self._handle_listmolt()
        
        elif text.startswith("/removemolt"):
            await self._handle_removemolt(text)
        
        elif text == "/infomolt":
            await self._handle_infomolt()
    
    # =========================================================================
    # COMMAND HANDLERS
    # =========================================================================
    
    async def _handle_trackmoltbook(self, text: str):
        """Handle /trackmoltbook command."""
        # Parse: /trackmoltbook twitter @handle
        # Parse: /trackmoltbook ticker $SYMBOL
        # Parse: /trackmoltbook name agent_name
        
        parts = text.split(maxsplit=2)
        if len(parts) < 3:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                "❌ Uso: <code>/trackmoltbook twitter @handle</code>\n"
                "ou <code>/trackmoltbook ticker $SYMBOL</code>\n"
                "ou <code>/trackmoltbook name agent_name</code>"
            )
            return
        
        track_type = parts[1].lower()
        value = parts[2]
        
        if track_type not in ["twitter", "ticker", "name"]:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                "❌ Tipo inválido. Use: <b>twitter</b>, <b>ticker</b> ou <b>name</b>"
            )
            return
        
        try:
            track_id = await self.storage.add_molttrack(track_type, value)
            
            # Format display value
            if track_type == "twitter":
                display = f"@{value.lower().lstrip('@')}"
            elif track_type == "ticker":
                display = f"${value.upper().lstrip('$')}"
            else:
                display = value
            
            await self._send_message(
                self.TRACKER_GROUP_ID,
                f"✅ Track <b>#{track_id}</b> adicionado!\n\n"
                f"📍 Tipo: <b>{track_type.upper()}</b>\n"
                f"🎯 Valor: <b>{display}</b>\n\n"
                f"Quando um PRE-CLAWNCH bater, você será notificado!"
            )
            
            self.logger.info(f"[TRACKER] Added track #{track_id}: {track_type}={value}")
        
        except Exception as e:
            self.logger.error(f"[TRACKER] Add track error: {e}")
            await self._send_message(
                self.TRACKER_GROUP_ID,
                f"❌ Erro ao adicionar track: {e}"
            )
    
    async def _handle_listmolt(self):
        """Handle /listmolt command."""
        tracks = await self.storage.list_molttracks()
        
        if not tracks:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                "📋 <b>Nenhum track ativo</b>\n\n"
                "Use <code>/trackmoltbook twitter @handle</code> para adicionar."
            )
            return
        
        # Group by type
        by_type = {"twitter": [], "ticker": [], "name": []}
        for t in tracks:
            by_type[t["type"]].append(t)
        
        lines = ["📋 <b>Tracks Ativos</b>\n"]
        
        if by_type["twitter"]:
            lines.append("\n🐦 <b>Twitter:</b>")
            for t in by_type["twitter"]:
                lines.append(f"  #{t['id']} → @{t['value']}")
        
        if by_type["ticker"]:
            lines.append("\n💰 <b>Ticker:</b>")
            for t in by_type["ticker"]:
                lines.append(f"  #{t['id']} → ${t['value']}")
        
        if by_type["name"]:
            lines.append("\n🤖 <b>Name:</b>")
            for t in by_type["name"]:
                lines.append(f"  #{t['id']} → {t['value']}")
        
        lines.append(f"\n\n📊 <b>Total:</b> {len(tracks)} tracks")
        lines.append("\nUse <code>/removemolt{id}</code> para remover.")
        
        await self._send_message(self.TRACKER_GROUP_ID, "\n".join(lines))
    
    async def _handle_removemolt(self, text: str):
        """Handle /removemolt{id} command."""
        # Extract ID from command like /removemolt1 or /removemolt 1
        match = re.match(r"/removemolt\s*(\d+)", text)
        if not match:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                "❌ Uso: <code>/removemolt1</code> (onde 1 é o ID do track)"
            )
            return
        
        track_id = int(match.group(1))
        
        # Get track info before removing
        tracks = await self.storage.list_molttracks()
        track_info = next((t for t in tracks if t["id"] == track_id), None)
        
        if not track_info:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                f"❌ Track #{track_id} não encontrado.\n\n"
                "Use <code>/listmolt</code> para ver os tracks ativos."
            )
            return
        
        success = await self.storage.remove_molttrack(track_id)
        
        if success:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                f"✅ Track <b>#{track_id}</b> removido!\n\n"
                f"📍 Tipo: <b>{track_info['type'].upper()}</b>\n"
                f"🎯 Valor: <b>{track_info['value']}</b>"
            )
            self.logger.info(f"[TRACKER] Removed track #{track_id}")
        else:
            await self._send_message(
                self.TRACKER_GROUP_ID,
                f"❌ Erro ao remover track #{track_id}"
            )
    
    async def _handle_infomolt(self):
        """Handle /infomolt command."""
        tracks = await self.storage.list_molttracks()
        
        # Count by type
        twitter_count = len([t for t in tracks if t["type"] == "twitter"])
        ticker_count = len([t for t in tracks if t["type"] == "ticker"])
        name_count = len([t for t in tracks if t["type"] == "name"])
        
        msg = (
            "📊 <b>Moltbook Tracker Status</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🐦 Twitter tracks: <b>{twitter_count}</b>\n"
            f"💰 Ticker tracks: <b>{ticker_count}</b>\n"
            f"🤖 Name tracks: <b>{name_count}</b>\n\n"
            f"📈 <b>Total:</b> {len(tracks)} tracks ativos\n\n"
            f"🔔 <b>Matches:</b> {self.stats['tracks_matched']}\n"
            f"📨 <b>Forwarded:</b> {self.stats['messages_forwarded']}\n"
            f"⌨️ <b>Commands:</b> {self.stats['commands_handled']}"
        )
        
        await self._send_message(self.TRACKER_GROUP_ID, msg)
    
    # =========================================================================
    # PRECLAWNCH LISTENER
    # =========================================================================
    
    async def _listen_preclawnch(self):
        """Listen for PRE-CLAWNCH tokens and check matches."""
        self.logger.info(f"[TRACKER] Subscribing to {Channels.PRECLAWNCH_MOLTBOOK}")
        
        # Use async for to consume the subscribe generator
        async for channel, event in self.redis.subscribe(Channels.PRECLAWNCH_MOLTBOOK):
            try:
                await self._handle_preclawnch(event)
            except Exception as e:
                self.logger.error(f"[TRACKER] Handle preclawnch error: {e}")
                self.stats["errors"] += 1
    
    async def _handle_preclawnch(self, event: Dict[str, Any]):
        """Check if event matches any track and forward."""
        x_handle = event.get("x_handle", "")
        symbol = event.get("token_symbol", "")
        agent_name = event.get("agent_name", "")
        message = event.get("message", "")
        buttons = event.get("buttons", [])
        
        self.logger.info(f"[TRACKER] Received: x_handle={x_handle}, symbol={symbol}, name={agent_name}")
        
        matched = False
        match_type = ""
        match_value = ""
        
        # Check twitter match
        if x_handle and await self.storage.has_molttrack("twitter", x_handle):
            matched = True
            match_type = "Twitter"
            match_value = f"@{x_handle}"
        
        # Check ticker match
        if symbol and await self.storage.has_molttrack("ticker", symbol):
            matched = True
            match_type = "Ticker"
            match_value = f"${symbol}"
        
        # Check name match
        if agent_name and await self.storage.has_molttrack("name", agent_name):
            matched = True
            match_type = "Name"
            match_value = agent_name
        
        if matched:
            self.stats["tracks_matched"] += 1
            self.logger.info(f"[TRACKER] MATCH! {match_type}={match_value}")
            
            # Add match header
            header = f"🎯 <b>TRACK MATCH: {match_type} = {match_value}</b>\n\n"
            full_message = header + message
            
            # Forward message with buttons
            await self._send_message(
                self.TRACKER_GROUP_ID,
                full_message,
                buttons,
            )
            
            self.stats["messages_forwarded"] += 1
            self.logger.info(f"[TRACKER] Forwarded to {self.TRACKER_GROUP_ID}")
    
    # =========================================================================
    # TELEGRAM HELPERS
    # =========================================================================
    
    async def _send_message(
        self,
        chat_id: int,
        text: str,
        buttons: List[List[Dict[str, str]]] = None,
    ):
        """Send message to Telegram."""
        url = f"{self.API_BASE}{self.bot_token}/sendMessage"
        
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        
        if buttons:
            payload["reply_markup"] = {"inline_keyboard": buttons}
        
        try:
            async with self._session.post(url, json=payload) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    self.logger.error(f"[TRACKER] Send error: {data}")
        except Exception as e:
            self.logger.error(f"[TRACKER] Send message error: {e}")


# Entry point
if __name__ == "__main__":
    MoltbookTrackerWorker.main()
