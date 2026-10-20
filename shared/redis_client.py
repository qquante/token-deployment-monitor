"""
Redis client for shared state between workers.

Provides:
- Connection management
- Pub/Sub helpers
- User config storage
- Pending buys storage
"""

import os
import json
import logging
import asyncio
from typing import Optional, Dict, Any, List, Callable, Awaitable
from contextlib import asynccontextmanager

import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Async Redis client with connection pooling.
    
    Usage:
        client = RedisClient()
        await client.connect()
        
        # Key-value
        await client.set("key", "value")
        value = await client.get("key")
        
        # Hash (for objects)
        await client.hset("user:123", {"name": "John", "age": 30})
        data = await client.hgetall("user:123")
        
        # Pub/Sub
        await client.publish("channel", {"event": "data"})
        async for message in client.subscribe("channel"):
            print(message)
    """
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        db: int = 0,
        password: str = None,
    ):
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port or int(os.getenv("REDIS_PORT", 6379))
        self.db = db or int(os.getenv("REDIS_DB", 0))
        self.password = password or os.getenv("REDIS_PASSWORD", None)
        
        self._pool: Optional[redis.ConnectionPool] = None
        self._client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
    
    async def connect(self):
        """Connect to Redis."""
        if self._client:
            return
        
        self._pool = redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=True,
            max_connections=20,
        )
        
        self._client = redis.Redis(connection_pool=self._pool)
        
        # Test connection
        try:
            await self._client.ping()
            logger.info(f"[REDIS] Connected to {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"[REDIS] Connection failed: {e}")
            raise
    
    async def close(self):
        """Close connection."""
        if self._pubsub:
            await self._pubsub.close()
        if self._client:
            await self._client.close()
        if self._pool:
            await self._pool.disconnect()
        logger.info("[REDIS] Disconnected")
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    # BASIC OPERATIONS
    
    async def get(self, key: str) -> Optional[str]:
        """Get string value."""
        return await self._client.get(key)
    
    async def set(self, key: str, value: str, ex: int = None) -> bool:
        """Set string value with optional expiry (seconds)."""
        return await self._client.set(key, value, ex=ex)
    
    async def delete(self, *keys: str) -> int:
        """Delete keys."""
        return await self._client.delete(*keys)
    
    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        return await self._client.exists(key) > 0
    
    async def incr(self, key: str) -> int:
        """Increment counter."""
        return await self._client.incr(key)
    
    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiry on key."""
        return await self._client.expire(key, seconds)
    
    async def setnx(self, key: str, value: str) -> bool:
        """Set if not exists (atomic). Returns True if key was set, False if existed."""
        return await self._client.setnx(key, value)
    
    async def setex(self, key: str, seconds: int, value: str) -> bool:
        """Set with expiry in one atomic operation."""
        return await self._client.setex(key, seconds, value)
    
    # HASH OPERATIONS (for objects)
    
    async def hset(self, key: str, mapping: Dict[str, Any]) -> int:
        """Set hash fields. Converts non-strings to JSON."""
        clean_mapping = {}
        for k, v in mapping.items():
            if v is None:
                clean_mapping[k] = ""
            elif isinstance(v, (dict, list)):
                clean_mapping[k] = json.dumps(v)
            elif isinstance(v, bool):
                clean_mapping[k] = "1" if v else "0"
            else:
                clean_mapping[k] = str(v)
        return await self._client.hset(key, mapping=clean_mapping)
    
    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get single hash field."""
        return await self._client.hget(key, field)
    
    async def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields."""
        return await self._client.hgetall(key)
    
    async def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields."""
        return await self._client.hdel(key, *fields)
    
    async def hexists(self, key: str, field: str) -> bool:
        """Check if hash field exists."""
        return await self._client.hexists(key, field)
    
    # SET OPERATIONS (for indexes)
    
    async def sadd(self, key: str, *members: str) -> int:
        """Add to set."""
        return await self._client.sadd(key, *members)
    
    async def srem(self, key: str, *members: str) -> int:
        """Remove from set."""
        return await self._client.srem(key, *members)
    
    async def smembers(self, key: str) -> set:
        """Get all set members."""
        return await self._client.smembers(key)
    
    async def sismember(self, key: str, member: str) -> bool:
        """Check if member in set."""
        return await self._client.sismember(key, member)
    
    async def scard(self, key: str) -> int:
        """Get set size."""
        return await self._client.scard(key)
    
    # LIST OPERATIONS (for queues)
    
    async def lpush(self, key: str, *values: str) -> int:
        """Push to left of list."""
        return await self._client.lpush(key, *values)
    
    async def rpush(self, key: str, *values: str) -> int:
        """Push to right of list."""
        return await self._client.rpush(key, *values)
    
    async def lpop(self, key: str) -> Optional[str]:
        """Pop from left of list."""
        return await self._client.lpop(key)
    
    async def rpop(self, key: str) -> Optional[str]:
        """Pop from right of list."""
        return await self._client.rpop(key)
    
    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Get range from list."""
        return await self._client.lrange(key, start, stop)
    
    async def llen(self, key: str) -> int:
        """Get list length."""
        return await self._client.llen(key)
    
    async def brpop(self, key: str, timeout: int = 0) -> Optional[tuple]:
        """Blocking pop from right."""
        return await self._client.brpop(key, timeout=timeout)
    
    # PUB/SUB
    
    async def publish(self, channel: str, message: Any) -> int:
        """
        Publish message to channel.
        
        Args:
            channel: Channel name
            message: Message (will be JSON serialized if dict/list)
            
        Returns:
            Number of subscribers that received the message
        """
        if isinstance(message, (dict, list)):
            message = json.dumps(message)
        return await self._client.publish(channel, message)
    
    async def subscribe(self, *channels: str):
        """
        Subscribe to channels and yield messages.
        
        Usage:
            async for channel, message in client.subscribe("events"):
                print(f"{channel}: {message}")
        """
        if not self._pubsub:
            self._pubsub = self._client.pubsub()
        
        await self._pubsub.subscribe(*channels)
        logger.info(f"[REDIS] Subscribed to: {channels}")
        
        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    channel = message["channel"]
                    data = message["data"]
                    
                    # Try to parse as JSON
                    try:
                        data = json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        pass
                    
                    yield channel, data
        finally:
            await self._pubsub.unsubscribe(*channels)
    
    async def subscribe_handler(
        self,
        channels: List[str],
        handler: Callable[[str, Any], Awaitable[None]],
    ):
        """
        Subscribe to channels with a handler function.
        
        Args:
            channels: List of channel names
            handler: Async function(channel, message) to call for each message
        """
        async for channel, message in self.subscribe(*channels):
            try:
                await handler(channel, message)
            except Exception as e:
                logger.error(f"[REDIS] Handler error for {channel}: {e}")


# GLOBAL INSTANCE

_redis_client: Optional[RedisClient] = None


async def get_redis() -> RedisClient:
    """Get or create global Redis client."""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
        await _redis_client.connect()
    return _redis_client


async def close_redis():
    """Close global Redis client."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
