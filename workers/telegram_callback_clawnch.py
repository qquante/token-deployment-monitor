"""
Telegram Callback Clawnch Worker

ISOLATED worker that polls ONLY the CLAWNCH bot token for callbacks.

This solves the 409 Conflict error by separating polling responsibilities:
- telegram_callback: polls prebankr, trading tokens
- telegram_callback_clawnch: polls ONLY clawnch token

Responsibilities:
- Poll updates from CLAWNCH bot only
- Handle callback queries (Like/Buy button clicks)
- Process same callback logic as main telegram_callback

NOTE: This worker shares storage with telegram_callback, so likes and
pending buys are visible to both workers.
"""

import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional, List

from workers.telegram_callback import TelegramCallbackWorker


class TelegramCallbackClawnchWorker(TelegramCallbackWorker):
    """
    Isolated callback handler for CLAWNCH bot token only.
    
    Inherits all callback handling logic from TelegramCallbackWorker
    but only polls the clawnch token.
    """
    
    WORKER_NAME = "telegram-callback-clawnch"
    
    def __init__(self):
        # Call parent init (sets up storage, etc)
        super().__init__()
        
        # Override: Use CLAWNCH bot token (same bot that sends the messages!)
        self.clawnch_token = os.getenv("TELEGRAM_BOT_TOKEN_CLAWNCH", "")
        
        if not self.clawnch_token:
            raise ValueError("TELEGRAM_BOT_TOKEN_CLAWNCH not set!")
        
        # Reset last_update_id to only track clawnch
        self._last_update_id = {
            "clawnch": 0,
        }
    
    async def run(self):
        """Run polling loop for CLAWNCH bot only."""
        self.logger.info("Starting ISOLATED clawnch callback polling")
        
        # Only poll clawnch token
        await self._poll_loop("clawnch", self.clawnch_token)


# Entry point
if __name__ == "__main__":
    TelegramCallbackClawnchWorker.main()
