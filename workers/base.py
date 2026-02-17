"""
Base worker class with common functionality.

All workers inherit from this class.
"""

import os
import sys
import asyncio
import logging
import signal
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime
import uuid

# Add project root to path
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

# Export project_root for other modules
__all__ = ["BaseWorker", "project_root"]

from shared.redis_client import RedisClient, get_redis, close_redis
from shared.pubsub import Channels, Events
from shared.storage import Storage, get_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)


class BaseWorker(ABC):
    """
    Base class for all workers.
    
    Provides:
    - Redis connection management
    - Pub/Sub helpers
    - Graceful shutdown handling
    - Heartbeat reporting
    """
    
    # Override in subclass
    WORKER_NAME = "base"
    
    def __init__(self):
        self.worker_id = f"{self.WORKER_NAME}-{uuid.uuid4().hex[:8]}"
        self.logger = logging.getLogger(self.WORKER_NAME)
        
        self.redis: Optional[RedisClient] = None
        self.storage: Optional[Storage] = None
        
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # Stats
        self.stats = {
            "started_at": None,
            "messages_processed": 0,
            "errors": 0,
        }
    
    async def setup(self):
        """Initialize connections and resources."""
        self.logger.info(f"[{self.worker_id}] Starting...")
        
        # Connect to Redis
        self.redis = await get_redis()
        self.storage = await get_storage()
        
        self.stats["started_at"] = datetime.now().isoformat()
        self._running = True
        
        self.logger.info(f"[{self.worker_id}] Ready")
    
    async def teardown(self):
        """Cleanup connections and resources."""
        self.logger.info(f"[{self.worker_id}] Shutting down...")
        self._running = False
        
        await close_redis()
        
        self.logger.info(f"[{self.worker_id}] Stopped")
    
    async def publish(self, channel: str, event: dict):
        """Publish event to Redis channel."""
        try:
            await self.redis.publish(channel, event)
            self.logger.debug(f"Published to {channel}: {event.get('event', 'unknown')}")
        except Exception as e:
            self.logger.error(f"Publish error: {e}")
            self.stats["errors"] += 1
    
    async def heartbeat_loop(self, interval: int = 30):
        """Send periodic heartbeats."""
        while self._running:
            try:
                event = Events.heartbeat(
                    worker_name=self.WORKER_NAME,
                    worker_id=self.worker_id,
                    status="healthy",
                    stats=self.stats.copy(),
                )
                await self.publish(Channels.SYSTEM_HEARTBEAT, event)
            except Exception as e:
                self.logger.error(f"Heartbeat error: {e}")
            
            await asyncio.sleep(interval)
    
    def handle_signal(self, sig):
        """Handle shutdown signals."""
        self.logger.info(f"[{self.worker_id}] Received signal {sig}")
        self._shutdown_event.set()
    
    @abstractmethod
    async def run(self):
        """Main worker loop. Override in subclass."""
        pass
    
    async def start(self):
        """Start the worker."""
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda s=sig: self.handle_signal(s))
            except NotImplementedError:
                # Windows doesn't support add_signal_handler
                pass
        
        try:
            await self.setup()
            
            # Run main loop and heartbeat in parallel
            await asyncio.gather(
                self.run(),
                self.heartbeat_loop(),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.teardown()
    
    @classmethod
    def main(cls):
        """Entry point for running the worker."""
        worker = cls()
        
        if sys.platform == "win32":
            # Windows needs this for Ctrl+C to work
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        try:
            asyncio.run(worker.start())
        except KeyboardInterrupt:
            pass
