"""
High-level storage operations using Redis.

Provides convenient methods for managing:
- User configs
- Pending buys
- Token info
- Enriched users
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from .redis_client import RedisClient, get_redis
from .models import UserConfig, PendingBuy, TokenInfo, EnrichedUser, get_fee_for_block

logger = logging.getLogger(__name__)

# TTL for cached data (in seconds)
ENRICHED_USER_TTL = 86400  # 24 hours - Combined enriched user (Twitter + AlphaGate + ENS)
TOKEN_INFO_TTL = 86400    # 24 hours
ALPHAGATE_CACHE_TTL = 86400  # 24 hours - AlphaGate data (R, C, SF) changes rarely
TWITTER_CACHE_TTL = 86400  # 24 hours - Twitter user data


class Storage:
    """
    High-level storage operations.
    
    All methods are async and work with Redis.
    """
    
    def __init__(self, redis: RedisClient):
        self.redis = redis
    
    # =========================================================================
    # USER CONFIG
    # =========================================================================
    
    async def get_user_config(self, user_id: int, username: str = "") -> UserConfig:
        """Get user config, creating default if not exists."""
        key = UserConfig.redis_key(user_id)
        data = await self.redis.hgetall(key)
        
        if data:
            config = UserConfig.from_redis(data)
            # Update username if provided
            if username and not config.username:
                config.username = username
                await self.save_user_config(config)
            return config
        
        # Create new config
        config = UserConfig(user_id=user_id, username=username)
        await self.save_user_config(config)
        return config
    
    async def save_user_config(self, config: UserConfig) -> bool:
        """Save user config to Redis."""
        key = UserConfig.redis_key(config.user_id)
        await self.redis.hset(key, config.to_redis())
        
        # Update auto_buy index
        if config.auto_buy:
            await self.redis.sadd("auto_buy_users", str(config.user_id))
        else:
            await self.redis.srem("auto_buy_users", str(config.user_id))
        
        logger.debug(f"[STORAGE] Saved user config: {config.user_id}")
        return True
    
    async def user_exists(self, user_id: int) -> bool:
        """Check if user config exists."""
        key = UserConfig.redis_key(user_id)
        return await self.redis.exists(key)
    
    # =========================================================================
    # SNIPER HANDLES (Trigger by @handle)
    # =========================================================================
    
    async def add_sniper_handle(self, user_id: int, handle: str) -> bool:
        """
        Add a Twitter handle to user's sniper list.
        
        Creates index: sniper_handle:{handle} -> set of user_ids
        """
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        # Get current config
        config = await self.get_user_config(user_id)
        if handle in config.sniper_handles:
            return False  # Already exists
        
        # Add to user config
        config.sniper_handles.append(handle)
        await self.save_user_config(config)
        
        # Add to handle index (for fast lookup)
        await self.redis.sadd(f"sniper_handle:{handle}", str(user_id))
        
        logger.info(f"[SNIPER] User {user_id} added handle: @{handle}")
        return True
    
    async def remove_sniper_handle(self, user_id: int, handle: str) -> bool:
        """Remove a Twitter handle from user's sniper list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        config = await self.get_user_config(user_id)
        if handle not in config.sniper_handles:
            return False
        
        # Remove from user config
        config.sniper_handles.remove(handle)
        await self.save_user_config(config)
        
        # Remove from handle index
        await self.redis.srem(f"sniper_handle:{handle}", str(user_id))
        
        logger.info(f"[SNIPER] User {user_id} removed handle: @{handle}")
        return True
    
    async def clear_sniper_handles(self, user_id: int) -> int:
        """Remove all handles from user's sniper list. Returns count removed."""
        config = await self.get_user_config(user_id)
        handles = config.sniper_handles.copy()
        count = len(handles)
        
        for handle in handles:
            await self.redis.srem(f"sniper_handle:{handle}", str(user_id))
        
        config.sniper_handles = []
        await self.save_user_config(config)
        
        logger.info(f"[SNIPER] User {user_id} cleared {count} handles")
        return count
    
    async def get_users_by_handle(self, handle: str) -> List[int]:
        """Get all user IDs that have this handle in their sniper list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return []
        
        user_ids = await self.redis.smembers(f"sniper_handle:{handle}")
        return [int(uid) for uid in user_ids]
    
    # =========================================================================
    # MOLTX TRIGGERS (auto-buy on MoltX posts)
    # =========================================================================
    
    async def set_moltx_trigger_enabled(self, user_id: int, enabled: bool) -> bool:
        """Enable/disable MoltX trigger for user."""
        config = await self.get_user_config(user_id)
        config.moltx_trigger_enabled = enabled
        await self.save_user_config(config)
        
        # Update index for fast lookups
        if enabled:
            await self.redis.sadd("moltx_trigger_users", str(user_id))
        else:
            await self.redis.srem("moltx_trigger_users", str(user_id))
        
        logger.info(f"[TRIGGER] MoltX {'enabled' if enabled else 'disabled'} for user {user_id}")
        return True
    
    async def get_users_with_moltx_trigger(self) -> List[int]:
        """Get all user IDs with MoltX trigger enabled."""
        user_ids = await self.redis.smembers("moltx_trigger_users")
        return [int(uid) for uid in user_ids]
    
    async def add_moltx_trigger_handle(self, user_id: int, handle: str) -> bool:
        """Add a handle to MoltX trigger list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        config = await self.get_user_config(user_id)
        if handle in config.moltx_trigger_handles:
            return False  # Already exists
        
        config.moltx_trigger_handles.append(handle)
        await self.save_user_config(config)
        
        # Index for fast lookup
        await self.redis.sadd(f"moltx_trigger_handle:{handle}", str(user_id))
        
        logger.info(f"[TRIGGER] MoltX user {user_id} added handle: @{handle}")
        return True
    
    async def remove_moltx_trigger_handle(self, user_id: int, handle: str) -> bool:
        """Remove a handle from MoltX trigger list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        config = await self.get_user_config(user_id)
        if handle not in config.moltx_trigger_handles:
            return False
        
        config.moltx_trigger_handles.remove(handle)
        await self.save_user_config(config)
        
        await self.redis.srem(f"moltx_trigger_handle:{handle}", str(user_id))
        
        logger.info(f"[TRIGGER] MoltX user {user_id} removed handle: @{handle}")
        return True
    
    async def clear_moltx_trigger_handles(self, user_id: int) -> int:
        """Clear all handles from MoltX trigger. Returns count removed."""
        config = await self.get_user_config(user_id)
        handles = config.moltx_trigger_handles.copy()
        count = len(handles)
        
        for handle in handles:
            await self.redis.srem(f"moltx_trigger_handle:{handle}", str(user_id))
        
        config.moltx_trigger_handles = []
        await self.save_user_config(config)
        
        logger.info(f"[TRIGGER] MoltX user {user_id} cleared {count} handles")
        return count
    
    async def get_moltx_trigger_users_by_handle(self, handle: str) -> List[int]:
        """Get all user IDs that have this handle in their MoltX trigger."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return []
        
        user_ids = await self.redis.smembers(f"moltx_trigger_handle:{handle}")
        return [int(uid) for uid in user_ids]
    
    async def check_moltx_trigger(
        self,
        user_id: int,
        x_handle: str = "",
        followers: int = 0,
        moltx_followers: int = 0,
        smart_followers: int = 0,
        is_verified: bool = False,
    ) -> bool:
        """
        Check if MoltX trigger conditions are met for a user.
        
        All non-zero/non-empty conditions must be met (AND logic).
        Returns True if user should auto-buy.
        """
        config = await self.get_user_config(user_id)
        
        if not config.moltx_trigger_enabled:
            return False
        
        # Collect all conditions that are set
        conditions_met = True
        
        # Handle check: if user has handles set, x_handle must match one
        if config.moltx_trigger_handles:
            handle_clean = x_handle.lower().strip().lstrip("@")
            if handle_clean not in config.moltx_trigger_handles:
                conditions_met = False
        
        # Min followers check
        if config.moltx_trigger_min_followers > 0:
            if followers < config.moltx_trigger_min_followers:
                conditions_met = False
        
        # Min MoltX followers check
        if config.moltx_trigger_min_moltx_followers > 0:
            if moltx_followers < config.moltx_trigger_min_moltx_followers:
                conditions_met = False
        
        # Min smart followers check
        if config.moltx_trigger_min_smart_followers > 0:
            if smart_followers < config.moltx_trigger_min_smart_followers:
                conditions_met = False
        
        # X verified check
        if config.moltx_trigger_x_verified:
            if not is_verified:
                conditions_met = False
        
        return conditions_met
    
    # =========================================================================
    # MOLTBOOK TRIGGERS (auto-buy on MoltBook posts)
    # =========================================================================
    
    async def set_moltbook_trigger_enabled(self, user_id: int, enabled: bool) -> bool:
        """Enable/disable MoltBook trigger for user."""
        config = await self.get_user_config(user_id)
        config.moltbook_trigger_enabled = enabled
        await self.save_user_config(config)
        
        # Update index for fast lookups
        if enabled:
            await self.redis.sadd("moltbook_trigger_users", str(user_id))
        else:
            await self.redis.srem("moltbook_trigger_users", str(user_id))
        
        logger.info(f"[TRIGGER] MoltBook {'enabled' if enabled else 'disabled'} for user {user_id}")
        return True
    
    async def get_users_with_moltbook_trigger(self) -> List[int]:
        """Get all user IDs with MoltBook trigger enabled."""
        user_ids = await self.redis.smembers("moltbook_trigger_users")
        return [int(uid) for uid in user_ids]
    
    async def add_moltbook_trigger_handle(self, user_id: int, handle: str) -> bool:
        """Add a handle to MoltBook trigger list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        config = await self.get_user_config(user_id)
        if handle in config.moltbook_trigger_handles:
            return False  # Already exists
        
        config.moltbook_trigger_handles.append(handle)
        await self.save_user_config(config)
        
        # Index for fast lookup
        await self.redis.sadd(f"moltbook_trigger_handle:{handle}", str(user_id))
        
        logger.info(f"[TRIGGER] MoltBook user {user_id} added handle: @{handle}")
        return True
    
    async def remove_moltbook_trigger_handle(self, user_id: int, handle: str) -> bool:
        """Remove a handle from MoltBook trigger list."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return False
        
        config = await self.get_user_config(user_id)
        if handle not in config.moltbook_trigger_handles:
            return False
        
        config.moltbook_trigger_handles.remove(handle)
        await self.save_user_config(config)
        
        await self.redis.srem(f"moltbook_trigger_handle:{handle}", str(user_id))
        
        logger.info(f"[TRIGGER] MoltBook user {user_id} removed handle: @{handle}")
        return True
    
    async def clear_moltbook_trigger_handles(self, user_id: int) -> int:
        """Clear all handles from MoltBook trigger. Returns count removed."""
        config = await self.get_user_config(user_id)
        handles = config.moltbook_trigger_handles.copy()
        count = len(handles)
        
        for handle in handles:
            await self.redis.srem(f"moltbook_trigger_handle:{handle}", str(user_id))
        
        config.moltbook_trigger_handles = []
        await self.save_user_config(config)
        
        logger.info(f"[TRIGGER] MoltBook user {user_id} cleared {count} handles")
        return count
    
    async def get_moltbook_trigger_users_by_handle(self, handle: str) -> List[int]:
        """Get all user IDs that have this handle in their MoltBook trigger."""
        handle = handle.lower().strip().lstrip("@")
        if not handle:
            return []
        
        user_ids = await self.redis.smembers(f"moltbook_trigger_handle:{handle}")
        return [int(uid) for uid in user_ids]
    
    async def check_moltbook_trigger(
        self,
        user_id: int,
        x_handle: str = "",
        followers: int = 0,
        smart_followers: int = 0,
        karma: int = 0,
        is_verified: bool = False,
    ) -> bool:
        """
        Check if MoltBook trigger conditions are met for a user.
        
        All non-zero/non-empty conditions must be met (AND logic).
        Returns True if user should auto-buy.
        """
        config = await self.get_user_config(user_id)
        
        if not config.moltbook_trigger_enabled:
            return False
        
        # Collect all conditions that are set
        conditions_met = True
        
        # Handle check: if user has handles set, x_handle must match one
        if config.moltbook_trigger_handles:
            handle_clean = x_handle.lower().strip().lstrip("@")
            if handle_clean not in config.moltbook_trigger_handles:
                conditions_met = False
        
        # Min followers check
        if config.moltbook_trigger_min_followers > 0:
            if followers < config.moltbook_trigger_min_followers:
                conditions_met = False
        
        # Min smart followers check
        if config.moltbook_trigger_min_smart_followers > 0:
            if smart_followers < config.moltbook_trigger_min_smart_followers:
                conditions_met = False
        
        # Min karma check
        if config.moltbook_trigger_min_karma > 0:
            if karma < config.moltbook_trigger_min_karma:
                conditions_met = False
        
        # X verified check
        if config.moltbook_trigger_x_verified:
            if not is_verified:
                conditions_met = False
        
        return conditions_met
    
    # =========================================================================
    # PENDING BUYS
    # =========================================================================
    
    async def approve_buy(
        self,
        user_id: int,
        tweet_id: str,
        username: str = "",
        token_symbol: str = "",
    ) -> PendingBuy:
        """
        Approve a buy for a user.
        
        Creates PendingBuy from user's current config.
        """
        config = await self.get_user_config(user_id, username)
        
        buy = PendingBuy(
            user_id=user_id,
            tweet_id=tweet_id,
            username=config.username,
            token_symbol=token_symbol,
            eth_amount=config.eth_amount,
            block_offset=config.block_offset,
            gwei=config.gwei,
            slippage=config.slippage,
            fee_expected=get_fee_for_block(config.block_offset),
            status="APPROVED",
            approved_at=datetime.now().isoformat(),
        )
        
        await self.save_pending_buy(buy)
        return buy
    
    async def save_pending_buy(self, buy: PendingBuy) -> bool:
        """Save pending buy to Redis with indexes."""
        key = PendingBuy.redis_key(buy.user_id, buy.tweet_id)
        
        # Save buy data
        await self.redis.hset(key, buy.to_redis())
        
        # Update indexes
        await self.redis.sadd(f"buys_by_user:{buy.user_id}", buy.buy_key)
        await self.redis.sadd(f"buys_by_status:{buy.status}", buy.buy_key)
        await self.redis.sadd(f"buys_by_tweet:{buy.tweet_id}", buy.buy_key)  # Tweet index
        
        if buy.target_block > 0:
            await self.redis.sadd(f"buys_by_target_block:{buy.target_block}", buy.buy_key)
        
        if buy.token_address:
            await self.redis.sadd(f"buys_by_token:{buy.token_address.lower()}", buy.buy_key)
        
        logger.info(f"[STORAGE] Saved pending buy: {buy.buy_key}")
        return True
    
    async def get_pending_buy(self, user_id: int, tweet_id: str) -> Optional[PendingBuy]:
        """Get a specific pending buy."""
        key = PendingBuy.redis_key(user_id, tweet_id)
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return PendingBuy.from_redis(data)
    
    async def get_pending_buys_by_tweet(self, tweet_id: str) -> List[PendingBuy]:
        """
        Get all pending buys for a tweet (from all users).
        
        Note: When buys are linked across threads (PRE-BANKR -> reply),
        the buy_key contains the ORIGINAL tweet_id where user clicked BUY.
        We must use that original tweet_id to fetch the buy from Redis.
        """
        buys = []
        
        # Use tweet_id index - O(1) lookup
        buy_keys = await self.redis.smembers(f"buys_by_tweet:{tweet_id}")
        
        for buy_key in buy_keys:
            # buy_key format: user_id:original_tweet_id
            parts = buy_key.split(":", 1)
            if len(parts) == 2:
                user_id = int(parts[0])
                original_tweet_id = parts[1]  # Use tweet_id from buy_key, not parameter!
                buy = await self.get_pending_buy(user_id, original_tweet_id)
                if buy:
                    buys.append(buy)
        
        return buys
    
    async def link_pending_buys_to_thread(self, parent_tweet_id: str, child_tweet_id: str) -> int:
        """
        Link PendingBuys from parent tweet to child tweet's index.
        
        When user clicks BUY on PRE-BANKR (parent), and token deploys
        on a reply (child), this ensures trading_executor finds the buy
        when searching by child_tweet_id.
        
        Args:
            parent_tweet_id: Tweet ID where user clicked BUY (PRE-BANKR original)
            child_tweet_id: Reply tweet ID where token was deployed
        
        Returns:
            Number of buys linked
        """
        if not parent_tweet_id or not child_tweet_id:
            return 0
        
        if parent_tweet_id == child_tweet_id:
            return 0  # Nothing to link
        
        # Get all buy_keys for parent tweet
        buy_keys = await self.redis.smembers(f"buys_by_tweet:{parent_tweet_id}")
        
        if not buy_keys:
            return 0
        
        # Add all buy_keys to child's index (allows lookup by either tweet)
        for buy_key in buy_keys:
            await self.redis.sadd(f"buys_by_tweet:{child_tweet_id}", buy_key)
        
        logger.info(
            f"[STORAGE] Linked {len(buy_keys)} buys: "
            f"{parent_tweet_id[:12]}... -> {child_tweet_id[:12]}..."
        )
        return len(buy_keys)
    
    async def get_pending_buys_by_status(self, status: str) -> List[PendingBuy]:
        """Get all pending buys with a specific status."""
        buy_keys = await self.redis.smembers(f"buys_by_status:{status}")
        buys = []
        
        for buy_key in buy_keys:
            parts = buy_key.split(":", 1)
            if len(parts) == 2:
                user_id = int(parts[0])
                tweet_id = parts[1]
                buy = await self.get_pending_buy(user_id, tweet_id)
                if buy:
                    buys.append(buy)
        
        return buys
    
    async def get_buys_for_block(self, block_number: int) -> List[PendingBuy]:
        """Get all buys with target_block <= block_number and status WAITING."""
        waiting_buys = await self.get_pending_buys_by_status("WAITING")
        
        return [
            buy for buy in waiting_buys
            if buy.target_block > 0 and buy.target_block <= block_number
        ]
    
    # =========================================================================
    # ATOMIC TRADE LOCK
    # =========================================================================
    
    async def try_acquire_trade_lock(self, buy_key: str, ttl_seconds: int = 120) -> bool:
        """
        Atomically acquire execution lock for a trade.
        
        Uses Redis SETNX for true atomicity - only ONE caller can acquire.
        Returns True if lock acquired, False if already held by another execution.
        """
        lock_key = f"trade_lock:{buy_key}"
        acquired = await self.redis.setnx(lock_key, "1")
        if acquired:
            await self.redis.expire(lock_key, ttl_seconds)
            logger.info(f"[LOCK] ACQUIRED: {buy_key}")
        else:
            logger.warning(f"[LOCK] BLOCKED: {buy_key} (already held)")
        return acquired
    
    async def release_trade_lock(self, buy_key: str) -> bool:
        """Release trade execution lock."""
        lock_key = f"trade_lock:{buy_key}"
        await self.redis.delete(lock_key)
        logger.debug(f"[LOCK] RELEASED: {buy_key}")
    
    async def try_acquire_buy_creation_lock(self, user_id: int, tweet_id: str, ttl_seconds: int = 10) -> bool:
        """
        Atomically acquire lock for creating a pending buy.
        
        Prevents race condition when user clicks BUY multiple times quickly.
        Uses Redis SETNX for true atomicity.
        Returns True if lock acquired, False if already held.
        """
        lock_key = f"buy_creation_lock:{user_id}:{tweet_id}"
        acquired = await self.redis.setnx(lock_key, "1")
        if acquired:
            await self.redis.expire(lock_key, ttl_seconds)
            logger.debug(f"[BUY LOCK] ACQUIRED: {user_id}:{tweet_id}")
        return acquired
        return True
    
    # Valid status transitions to prevent invalid state changes
    VALID_TRANSITIONS = {
        "APPROVED": ["WAITING", "CANCELLED"],
        "WAITING": ["EXECUTING", "CANCELLED", "PENDING_CONFIRM"],
        "PENDING_CONFIRM": ["WAITING", "CANCELLED"],
        "EXECUTING": ["DONE", "FAILED", "CANCELLED"],  # Allow cancel during execution
        "DONE": [],  # Terminal state
        "FAILED": [],  # Terminal state
        "CANCELLED": [],  # Terminal state
    }
    
    async def update_buy_status(
        self,
        buy: PendingBuy,
        new_status: str,
        **extra_fields,
    ) -> bool:
        """
        Update buy status with fresh read and transition validation.
        
        IMPORTANT: Does a fresh read from Redis to avoid stale object issues.
        Validates that the transition is allowed before proceeding.
        """
        # FRESH READ: Get current status from Redis, not from potentially stale object
        current = await self.get_pending_buy(buy.user_id, buy.tweet_id)
        if not current:
            logger.error(f"[STORAGE] Buy not found for status update: {buy.buy_key}")
            return False
        
        old_status = current.status
        
        # Validate transition
        allowed = self.VALID_TRANSITIONS.get(old_status, [])
        if new_status not in allowed:
            logger.error(
                f"[STORAGE] INVALID TRANSITION: {buy.buy_key} "
                f"{old_status} -> {new_status} (allowed: {allowed})"
            )
            return False
        
        # Update the buy object
        buy.status = new_status
        for key, value in extra_fields.items():
            if hasattr(buy, key):
                setattr(buy, key, value)
        
        # Update status index
        await self.redis.srem(f"buys_by_status:{old_status}", buy.buy_key)
        await self.redis.sadd(f"buys_by_status:{new_status}", buy.buy_key)
        
        # Remove from target block index when reaching terminal state
        # This prevents the executor from repeatedly trying to process completed trades
        if new_status in ["DONE", "FAILED", "CANCELLED"] and buy.target_block > 0:
            await self.redis.srem(f"buys_by_target_block:{buy.target_block}", buy.buy_key)
        
        # Save updated buy
        await self.save_pending_buy(buy)
        
        logger.info(f"[STORAGE] Updated buy {buy.buy_key}: {old_status} -> {new_status}")
        return True
    
    async def link_token_to_buy(
        self,
        buy: PendingBuy,
        token_address: str,
        token_symbol: str,
        pool_id: str,
        auction_expire_block: int,
    ) -> bool:
        """Link on-chain token data to a pending buy."""
        buy.token_address = token_address
        buy.token_symbol = token_symbol
        buy.pool_id = pool_id
        buy.auction_expire_block = auction_expire_block
        buy.target_block = auction_expire_block + buy.block_offset
        buy.linked_at = datetime.now().isoformat()
        
        # Update status from APPROVED to WAITING
        return await self.update_buy_status(buy, "WAITING")
    
    async def cancel_buy(self, user_id: int, tweet_id: str) -> bool:
        """Cancel a pending buy."""
        buy = await self.get_pending_buy(user_id, tweet_id)
        if not buy:
            return False
        
        # Update to CANCELLED (for logging/history purposes)
        await self.update_buy_status(buy, "CANCELLED")
        
        # Remove from active indexes
        await self.redis.srem(f"buys_by_user:{user_id}", buy.buy_key)
        await self.redis.srem(f"buys_by_tweet:{tweet_id}", buy.buy_key)
        if buy.target_block > 0:
            await self.redis.srem(f"buys_by_target_block:{buy.target_block}", buy.buy_key)
        
        # Delete the main key to allow user to create a new buy for same tweet
        key = PendingBuy.redis_key(user_id, tweet_id)
        await self.redis.delete(key)
        
        logger.info(f"[STORAGE] Cancelled and deleted buy: {buy.buy_key}")
        return True
    
    async def complete_buy(
        self,
        buy: PendingBuy,
        tx_hash: str = "",
        error: str = "",
        gas_used: int = 0,
    ) -> bool:
        """Mark buy as complete (DONE or FAILED)."""
        status = "DONE" if tx_hash and not error else "FAILED"
        
        return await self.update_buy_status(
            buy,
            status,
            tx_hash=tx_hash,
            error=error,
            gas_used=gas_used,
            executed_at=datetime.now().isoformat(),
        )
    
    # =========================================================================
    # TOKEN INFO
    # =========================================================================
    
    async def save_token_info(self, token: TokenInfo) -> bool:
        """Save token info to Redis."""
        key = TokenInfo.redis_key(token.tweet_id)
        await self.redis.hset(key, token.to_redis())
        await self.redis.expire(key, TOKEN_INFO_TTL)
        
        # Index by address if available
        if token.token_address:
            await self.redis.set(
                f"token_by_address:{token.token_address.lower()}",
                token.tweet_id,
                ex=TOKEN_INFO_TTL,
            )
        
        logger.debug(f"[STORAGE] Saved token info: {token.tweet_id}")
        return True
    
    async def get_token_info(self, tweet_id: str) -> Optional[TokenInfo]:
        """Get token info by tweet ID."""
        key = TokenInfo.redis_key(tweet_id)
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return TokenInfo.from_redis(data)
    
    async def get_token_by_address(self, token_address: str) -> Optional[TokenInfo]:
        """Get token info by contract address."""
        tweet_id = await self.redis.get(f"token_by_address:{token_address.lower()}")
        if not tweet_id:
            return None
        return await self.get_token_info(tweet_id)
    
    # =========================================================================
    # PRE-CLAWNCH TOKENS (MoltX, Moltbook, 4claw)
    # =========================================================================
    
    # TTL for pre-clawnch tokens (30 days - longer to ensure linking works)
    PRECLAWNCH_TTL = 86400 * 30
    
    async def save_preclawnch_token(
        self,
        post_id: str,
        platform: str,  # "moltx", "moltbook", "4claw"
        token_name: str,
        token_symbol: str,
        token_wallet: str = "",
        agent_name: str = "",
        x_handle: str = "",
    ) -> bool:
        """
        Save Pre-Clawnch token for later linking.
        
        The post_id corresponds to context_parsed.messageId on-chain:
        - MoltX: UUID like "8fdd6010-16d1-41c0-88ee-132cfa6b5938"
        - 4claw: "thread:uuid" like "thread:4a6b2b62-30a9-49b9-b064-58ca8e174c87"
        - Moltbook: UUID similar to MoltX
        """
        key = f"preclawnch:{post_id}"
        data = {
            "post_id": post_id,
            "platform": platform,
            "token_name": token_name,
            "token_symbol": token_symbol,
            "token_wallet": token_wallet,
            "agent_name": agent_name,
            "x_handle": x_handle,
            "created_at": datetime.now().isoformat(),
        }
        await self.redis.hset(key, data)
        await self.redis.expire(key, self.PRECLAWNCH_TTL)
        
        # Index by wallet prefix (for button callback)
        if token_wallet:
            wallet_prefix = token_wallet[:16].lower()
            await self.redis.set(
                f"preclawnch_by_wallet:{wallet_prefix}",
                post_id,
                ex=self.PRECLAWNCH_TTL,
            )
        
        # Index by symbol (for lookup)
        if token_symbol:
            await self.redis.set(
                f"preclawnch_by_symbol:{token_symbol.upper()}",
                post_id,
                ex=self.PRECLAWNCH_TTL,
            )
        
        logger.info(f"[STORAGE] Saved Pre-Clawnch: {platform}/{post_id} - ${token_symbol}")
        return True
    
    async def get_preclawnch_token(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Get Pre-Clawnch token by post ID (messageId)."""
        key = f"preclawnch:{post_id}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return {
            "post_id": data.get("post_id", ""),
            "platform": data.get("platform", ""),
            "token_name": data.get("token_name", ""),
            "token_symbol": data.get("token_symbol", ""),
            "token_wallet": data.get("token_wallet", ""),
            "agent_name": data.get("agent_name", ""),
            "x_handle": data.get("x_handle", ""),
            "created_at": data.get("created_at", ""),
        }
    
    async def get_preclawnch_token_by_wallet(self, wallet_prefix: str) -> Optional[Dict[str, Any]]:
        """Get Pre-Clawnch token by wallet prefix (from button callback)."""
        wallet_prefix = wallet_prefix.lower()
        post_id = await self.redis.get(f"preclawnch_by_wallet:{wallet_prefix}")
        if not post_id:
            return None
        return await self.get_preclawnch_token(post_id)
    
    async def get_pending_buys_by_post_id(self, post_id: str) -> List[PendingBuy]:
        """
        Get all pending buys for a Pre-Clawnch post.
        
        Uses same index as tweet_id since we use post_id as the unique identifier.
        """
        return await self.get_pending_buys_by_tweet(post_id)
    
    # =========================================================================
    # ENRICHED USERS (cache)
    # =========================================================================
    
    async def save_enriched_user(self, user: EnrichedUser) -> bool:
        """Save enriched user to cache."""
        key = EnrichedUser.redis_key(user.username)
        user.cached_at = datetime.now().isoformat()
        await self.redis.hset(key, user.to_redis())
        await self.redis.expire(key, ENRICHED_USER_TTL)
        logger.debug(f"[STORAGE] Cached enriched user: {user.username}")
        return True
    
    async def get_enriched_user(self, username: str) -> Optional[EnrichedUser]:
        """Get enriched user from cache."""
        key = EnrichedUser.redis_key(username)
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return EnrichedUser.from_redis(data)
    
    # =========================================================================
    # ALPHAGATE CACHE (24 hours - R, C, Smart Followers)
    # =========================================================================
    
    async def save_alphagate_data(
        self,
        username: str,
        prev_usernames_count: int,
        contracts_count: int,
        smart_followers: int,
    ) -> bool:
        """Cache AlphaGate data for 24 hours."""
        key = f"alphagate:{username.lower()}"
        data = {
            "prev_usernames_count": str(prev_usernames_count),
            "contracts_count": str(contracts_count),
            "smart_followers": str(smart_followers),
            "cached_at": datetime.now().isoformat(),
        }
        await self.redis.hset(key, data)
        await self.redis.expire(key, ALPHAGATE_CACHE_TTL)
        logger.debug(f"[STORAGE] AlphaGate cached: @{username} R={prev_usernames_count} C={contracts_count} SF={smart_followers}")
        return True
    
    async def get_alphagate_data(self, username: str) -> Optional[Dict[str, int]]:
        """Get cached AlphaGate data."""
        key = f"alphagate:{username.lower()}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return {
            "prev_usernames_count": int(data.get("prev_usernames_count", 0)),
            "contracts_count": int(data.get("contracts_count", 0)),
            "smart_followers": int(data.get("smart_followers", 0)),
        }
    
    # =========================================================================
    # TWITTER CACHE (24 hours - user info + about)
    # =========================================================================
    
    async def save_twitter_user(
        self,
        username: str,
        user_id: str = "",
        name: str = "",
        followers: int = 0,
        following: int = 0,
        is_blue_verified: bool = False,
        verified_type: str = "",
        created_at: str = "",
        bio: str = "",
        affiliate_label: str = "",
    ) -> bool:
        """Cache Twitter user data for 24 hours."""
        key = f"twitter:{username.lower()}"
        data = {
            "user_id": str(user_id),
            "name": name,
            "followers": str(followers),
            "following": str(following),
            "is_blue_verified": "1" if is_blue_verified else "0",
            "verified_type": verified_type or "",
            "created_at": created_at or "",
            "bio": bio or "",
            "affiliate_label": affiliate_label or "",
            "cached_at": datetime.now().isoformat(),
        }
        await self.redis.hset(key, data)
        await self.redis.expire(key, TWITTER_CACHE_TTL)
        logger.debug(f"[STORAGE] Twitter cached: @{username} ({followers} followers)")
        return True
    
    async def get_twitter_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Get cached Twitter user data."""
        key = f"twitter:{username.lower()}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return {
            "user_id": data.get("user_id", ""),
            "name": data.get("name", ""),
            "followers": int(data.get("followers", 0)),
            "following": int(data.get("following", 0)),
            "is_blue_verified": data.get("is_blue_verified") == "1",
            "verified_type": data.get("verified_type", ""),
            "created_at": data.get("created_at", ""),
            "bio": data.get("bio", ""),
            "affiliate_label": data.get("affiliate_label", ""),
        }
    
    # =========================================================================
    # LIKES (for PRE-BANKR)
    # =========================================================================
    
    async def add_like(
        self,
        tweet_id: str,
        user_id: int,
        username: str,
    ) -> int:
        """Add like to tweet."""
        key = f"likes:{tweet_id}"
        await self.redis.sadd(key, f"{user_id}:{username}")
        return await self.redis.scard(key)
    
    async def remove_like(self, tweet_id: str, user_id: int) -> int:
        """Remove like from tweet."""
        key = f"likes:{tweet_id}"
        # Need to find and remove the user's like
        members = await self.redis.smembers(key)
        for member in members:
            if member.startswith(f"{user_id}:"):
                await self.redis.srem(key, member)
                break
        return await self.redis.scard(key)
    
    async def has_liked(self, tweet_id: str, user_id: int) -> bool:
        """Check if user has liked tweet."""
        key = f"likes:{tweet_id}"
        members = await self.redis.smembers(key)
        return any(m.startswith(f"{user_id}:") for m in members)
    
    async def get_like_count(self, tweet_id: str) -> int:
        """Get total likes for tweet."""
        key = f"likes:{tweet_id}"
        return await self.redis.scard(key)
    
    async def get_like_usernames(self, tweet_id: str) -> List[str]:
        """Get usernames who liked tweet."""
        key = f"likes:{tweet_id}"
        members = await self.redis.smembers(key)
        return [m.split(":", 1)[1] for m in members if ":" in m]
    
    # =========================================================================
    # NOTIFIED CONTRACTS (dedupe)
    # =========================================================================
    
    async def has_notified_contract(self, contract: str) -> bool:
        """Check if contract was already notified."""
        return await self.redis.sismember("notified_contracts", contract.lower())
    
    async def mark_contract_notified(self, contract: str) -> bool:
        """Mark contract as notified."""
        await self.redis.sadd("notified_contracts", contract.lower())
        return True
    
    # =========================================================================
    # PRE-BANKR DATA CACHE (for instant DEPLOYED display)
    # =========================================================================
    
    PREBANKR_TTL = 10800  # 3 hours - PRE-BANKR cache for linking to DEPLOYED
    
    async def save_prebankr_data(
        self,
        tweet_id: str,
        from_username: str,
        to_usernames: List[str],
        token_name: str = "",
        token_symbol: str = "",
        wallet_map: Dict[str, str] = None,
        enriched_from: Dict[str, Any] = None,
        enriched_to: List[Dict[str, Any]] = None,
    ) -> bool:
        """
        Cache PRE-BANKR data for instant use when DEPLOYED arrives.
        
        Same as websocket_async.py prebankr_data dict but in Redis.
        ALWAYS caches OK - critical for trading flow.
        """
        import json
        key = f"prebankr_data:{tweet_id}"
        
        try:
            data = {
                "tweet_id": tweet_id,
                "from_username": from_username,
                "to_usernames": json.dumps(to_usernames),
                "token_name": token_name or "",
                "token_symbol": token_symbol or "",
                "wallet_map": json.dumps(wallet_map or {}),
                "enriched_from": json.dumps(enriched_from) if enriched_from else "",
                "enriched_to": json.dumps(enriched_to or []),
                "cached_at": datetime.now().isoformat(),
            }
            
            await self.redis.hset(key, data)
            await self.redis.expire(key, self.PREBANKR_TTL)
            
            logger.info(f"[STORAGE] PRE-BANKR cached OK: {tweet_id[:8]}... ${token_symbol}")
            return True
        except Exception as e:
            logger.error(f"[STORAGE] PRE-BANKR cache FAILED for {tweet_id[:8]}...: {e}")
            return False
    
    async def get_prebankr_data(self, tweet_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cached PRE-BANKR data for a tweet.
        
        Returns None if not found or expired.
        """
        import json
        key = f"prebankr_data:{tweet_id}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        # Parse JSON fields
        result = {
            "tweet_id": data.get("tweet_id", ""),
            "from_username": data.get("from_username", ""),
            "to_usernames": json.loads(data.get("to_usernames", "[]")),
            "token_name": data.get("token_name", ""),
            "token_symbol": data.get("token_symbol", ""),
            "wallet_map": json.loads(data.get("wallet_map", "{}")),
            "enriched_from": None,
            "enriched_to": [],
            "cached_at": data.get("cached_at", ""),
        }
        
        # Parse enriched data if present
        if data.get("enriched_from"):
            result["enriched_from"] = json.loads(data["enriched_from"])
        
        if data.get("enriched_to"):
            result["enriched_to"] = json.loads(data["enriched_to"])
        
        return result
    
    async def update_prebankr_enriched(
        self,
        tweet_id: str,
        enriched_from: Dict[str, Any] = None,
        enriched_to: List[Dict[str, Any]] = None,
    ) -> bool:
        """
        Update cached PRE-BANKR data with enriched user data.
        
        Called after enrichment completes.
        """
        import json
        key = f"prebankr_data:{tweet_id}"
        
        if not await self.redis.exists(key):
            return False
        
        updates = {}
        if enriched_from is not None:
            updates["enriched_from"] = json.dumps(enriched_from)
        if enriched_to is not None:
            updates["enriched_to"] = json.dumps(enriched_to)
        
        if updates:
            await self.redis.hset(key, updates)
            logger.debug(f"[STORAGE] Updated PRE-BANKR enriched data for {tweet_id[:8]}...")
        
        return True
    
    # =========================================================================
    # MOLT AGENTS
    # =========================================================================
    
    MOLT_AGENT_TTL = 86400  # 24 hours cache
    
    async def save_molt_agent(
        self,
        x_handle: str,
        agent_name: str,
        is_verified: bool = False,
        karma: int = 0,
        description: str = "",
        agent_id: str = "",
        # New fields for KOL filtering
        followers: int = 0,
        smart_followers_count: int = 0,
        vip_followers: List[str] = None,
        passes_kol_filter: bool = False,
    ) -> bool:
        """
        Save a MOLT agent to Redis.
        
        Args:
            x_handle: Twitter username of the agent owner
            agent_name: Name of the agent
            is_verified: Whether agent has verification tweet
            karma: Agent karma score
            description: Agent description
            agent_id: Moltbook agent ID
            followers: Twitter follower count
            smart_followers_count: Number of smart followers from AlphaGate
            vip_followers: List of VIP smart follower usernames
            passes_kol_filter: Whether agent passes KOL filter (500+ followers OR 2+ VIPs)
        """
        import json
        x_handle = x_handle.lower().lstrip("@")
        key = f"molt_agent:{x_handle}"
        
        data = {
            "x_handle": x_handle,
            "agent_name": agent_name,
            "is_verified": "1" if is_verified else "0",
            "karma": str(karma),
            "description": description[:500] if description else "",
            "agent_id": agent_id,
            "followers": str(followers),
            "smart_followers_count": str(smart_followers_count),
            "vip_followers": json.dumps(vip_followers or []),
            "passes_kol_filter": "1" if passes_kol_filter else "0",
            "cached_at": datetime.now().isoformat(),
        }
        
        await self.redis.hset(key, data)
        await self.redis.expire(key, self.MOLT_AGENT_TTL)
        
        # Add to index set
        await self.redis.sadd("molt_agents_index", x_handle)
        
        # Create reverse indexes for lookup by name and id
        if agent_name:
            await self.redis.set(
                f"molt_name_to_handle:{agent_name.lower()}",
                x_handle,
                ex=self.MOLT_AGENT_TTL
            )
        
        if agent_id:
            await self.redis.set(
                f"molt_id_to_handle:{agent_id}",
                x_handle,
                ex=self.MOLT_AGENT_TTL
            )
        
        logger.debug(f"[STORAGE] Saved MOLT agent: @{x_handle} ({agent_name}) KOL={passes_kol_filter}")
        return True
    
    async def get_molt_agent(self, x_handle: str) -> Optional[Dict[str, Any]]:
        """
        Get MOLT agent info by Twitter handle.
        
        Returns None if not a MOLT agent.
        """
        import json
        x_handle = x_handle.lower().lstrip("@")
        key = f"molt_agent:{x_handle}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        # Parse vip_followers JSON
        vip_followers = []
        try:
            vip_followers = json.loads(data.get("vip_followers", "[]"))
        except:
            pass
        
        return {
            "x_handle": data.get("x_handle", ""),
            "agent_name": data.get("agent_name", ""),
            "is_verified": data.get("is_verified", "0") == "1",
            "karma": int(data.get("karma", 0)),
            "description": data.get("description", ""),
            "agent_id": data.get("agent_id", ""),
            "followers": int(data.get("followers", 0)),
            "smart_followers_count": int(data.get("smart_followers_count", 0)),
            "vip_followers": vip_followers,
            "passes_kol_filter": data.get("passes_kol_filter", "0") == "1",
            "cached_at": data.get("cached_at", ""),
        }
    
    async def is_molt_agent(self, x_handle: str) -> bool:
        """Check if a Twitter handle is a MOLT agent."""
        x_handle = x_handle.lower().lstrip("@")
        return await self.redis.sismember("molt_agents_index", x_handle)
    
    async def set_molt_agent_verified(self, x_handle: str, verified: bool = True) -> bool:
        """Update verification status of a MOLT agent."""
        x_handle = x_handle.lower().lstrip("@")
        key = f"molt_agent:{x_handle}"
        
        if not await self.redis.exists(key):
            return False
        
        await self.redis.hset(key, {"is_verified": "1" if verified else "0"})
        return True
    
    async def get_all_molt_agents(self) -> List[str]:
        """Get all MOLT agent handles."""
        return list(await self.redis.smembers("molt_agents_index"))
    
    async def get_molt_agents_count(self) -> int:
        """Get count of cached MOLT agents."""
        return await self.redis.scard("molt_agents_index")
    
    async def get_molt_agent_by_name(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """
        Get MOLT agent by Moltbook agent name.
        
        Uses reverse index for fast O(1) lookup.
        Returns None if agent not found in cache.
        """
        agent_name_clean = agent_name.lower().strip()
        
        # Try reverse index first (O(1) lookup)
        x_handle = await self.redis.get(f"molt_name_to_handle:{agent_name_clean}")
        if x_handle:
            logger.debug(f"[STORAGE] Found agent by name (cache): {agent_name} -> @{x_handle}")
            return await self.get_molt_agent(x_handle)
        
        logger.debug(f"[STORAGE] Agent not in reverse index: {agent_name}")
        return None
    
    async def get_molt_agent_by_id(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """
        Get MOLT agent by Moltbook agent ID (UUID).
        
        Uses reverse index for fast O(1) lookup.
        Returns None if agent not found in cache.
        """
        x_handle = await self.redis.get(f"molt_id_to_handle:{agent_id}")
        if x_handle:
            logger.debug(f"[STORAGE] Found agent by ID (cache): {agent_id[:8]}... -> @{x_handle}")
            return await self.get_molt_agent(x_handle)
        
        logger.debug(f"[STORAGE] Agent ID not in reverse index: {agent_id[:8]}...")
        return None
    
    # =========================================================================
    # CLANKER POOL DATA (cached by clanker_enricher worker)
    # =========================================================================
    
    POOL_CACHE_TTL = 86400 * 7  # 7 days - pool params don't change
    
    async def save_pool_data(
        self,
        token_address: str,
        pool_id: str,
        currency0: str,
        currency1: str,
        fee: int,
        tick_spacing: int,
        hooks: str,
        sqrt_price_x96: str = "",
        tick: int = 0,
        zero_for_one: bool = True,
        block_number: int = 0,
        tx_hash: str = "",
    ) -> bool:
        """
        Cache Clanker pool data for a token.
        
        Used by clanker_enricher worker and can be retrieved by trading workers.
        """
        key = f"pool:{token_address.lower()}"
        
        data = {
            "token_address": token_address.lower(),
            "pool_id": pool_id,
            "currency0": currency0,
            "currency1": currency1,
            "fee": str(fee),
            "tick_spacing": str(tick_spacing),
            "hooks": hooks,
            "sqrt_price_x96": sqrt_price_x96,
            "tick": str(tick),
            "zero_for_one": "1" if zero_for_one else "0",
            "block_number": str(block_number),
            "tx_hash": tx_hash,
            "cached_at": datetime.now().isoformat(),
        }
        
        await self.redis.hset(key, data)
        await self.redis.expire(key, self.POOL_CACHE_TTL)
        
        logger.debug(f"[STORAGE] Pool cached: {token_address[:10]}... fee={fee}")
        return True
    
    async def get_pool_data(self, token_address: str) -> Optional[Dict[str, Any]]:
        """
        Get cached pool data for a token.
        
        Returns None if not cached.
        """
        key = f"pool:{token_address.lower()}"
        data = await self.redis.hgetall(key)
        
        if not data:
            return None
        
        return {
            "token_address": data.get("token_address", ""),
            "pool_id": data.get("pool_id", ""),
            "currency0": data.get("currency0", ""),
            "currency1": data.get("currency1", ""),
            "fee": int(data.get("fee", 0)),
            "tick_spacing": int(data.get("tick_spacing", 0)),
            "hooks": data.get("hooks", ""),
            "sqrt_price_x96": data.get("sqrt_price_x96", ""),
            "tick": int(data.get("tick", 0)),
            "zero_for_one": data.get("zero_for_one", "1") == "1",
            "block_number": int(data.get("block_number", 0)),
            "tx_hash": data.get("tx_hash", ""),
            "cached_at": data.get("cached_at", ""),
        }
    
    async def has_pool_data(self, token_address: str) -> bool:
        """Check if pool data is cached for a token."""
        key = f"pool:{token_address.lower()}"
        return await self.redis.exists(key)
    
    # =========================================================================
    # STATS
    # =========================================================================
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get overall storage stats."""
        return {
            "pending_buys_approved": await self.redis.scard("buys_by_status:APPROVED"),
            "pending_buys_waiting": await self.redis.scard("buys_by_status:WAITING"),
            "pending_buys_done": await self.redis.scard("buys_by_status:DONE"),
            "pending_buys_failed": await self.redis.scard("buys_by_status:FAILED"),
            "notified_contracts": await self.redis.scard("notified_contracts"),
            "molt_agents": await self.redis.scard("molt_agents_index"),
        }
    
    # =========================================================================
    # SEEN INTERFACES (for new interface detection)
    # =========================================================================
    
    # Default known interfaces - will be populated on first run
    DEFAULT_INTERFACES = {
        "Bankr",
        "Clawnch",
        "clanker.world",
        "Clank.fun",
        "SDK",
        "Clanker Telegram Mini App",
        "Molty Bot",
        "",  # empty interface is also "known"
    }
    
    async def get_seen_interfaces(self) -> set:
        """Get all seen interfaces from Redis."""
        members = await self.redis.smembers("seen_interfaces")
        return set(members) if members else set()
    
    async def add_seen_interface(self, interface: str) -> bool:
        """Add a new interface to the seen set."""
        await self.redis.sadd("seen_interfaces", interface)
        logger.info(f"[STORAGE] Added seen interface: {interface}")
        return True
    
    async def init_default_interfaces(self) -> int:
        """
        Initialize seen_interfaces with default known interfaces.
        Only adds if not already present.
        Returns number of new interfaces added.
        """
        existing = await self.get_seen_interfaces()
        added = 0
        
        for interface in self.DEFAULT_INTERFACES:
            if interface not in existing:
                await self.redis.sadd("seen_interfaces", interface)
                added += 1
        
        if added > 0:
            logger.info(f"[STORAGE] Initialized {added} default interfaces")
        
        return added
    
    async def has_seen_interface(self, interface: str) -> bool:
        """Check if interface has been seen before."""
        return await self.redis.sismember("seen_interfaces", interface)
    
    # =========================================================================
    # MOLTBOOK TRACKING
    # =========================================================================
    
    async def add_molttrack(self, track_type: str, value: str) -> int:
        """
        Add a Moltbook track.
        
        Args:
            track_type: 'twitter', 'ticker', or 'name'
            value: The value to track (handle, symbol, or name)
        
        Returns:
            Track ID (int)
        """
        # Normalize value based on type
        if track_type == "twitter":
            value = value.lower().strip().lstrip("@")
        elif track_type == "ticker":
            value = value.upper().strip().lstrip("$")
        elif track_type == "name":
            value = value.lower().strip()
        else:
            raise ValueError(f"Invalid track_type: {track_type}")
        
        # Check if already exists
        existing_id = await self.redis.get(f"molttrack:{track_type}:{value}")
        if existing_id:
            return int(existing_id)  # Already tracked
        
        # Get next ID
        track_id = await self.redis.incr("molttrack:next_id")
        
        # Save track data
        track_data = {"type": track_type, "value": value}
        await self.redis.hset("molttrack:list", {str(track_id): json.dumps(track_data)})
        
        # Create index for quick lookup
        await self.redis.set(f"molttrack:{track_type}:{value}", str(track_id))
        
        logger.info(f"[MOLTTRACK] Added track #{track_id}: {track_type}={value}")
        return track_id
    
    async def remove_molttrack(self, track_id: int) -> bool:
        """
        Remove a Moltbook track by ID.
        
        Returns:
            True if removed, False if not found
        """
        # Get track data
        track_json = await self.redis.hget("molttrack:list", str(track_id))
        if not track_json:
            return False
        
        track = json.loads(track_json)
        track_type = track["type"]
        value = track["value"]
        
        # Remove from list
        await self.redis.hdel("molttrack:list", str(track_id))
        
        # Remove index
        await self.redis.delete(f"molttrack:{track_type}:{value}")
        
        logger.info(f"[MOLTTRACK] Removed track #{track_id}: {track_type}={value}")
        return True
    
    async def list_molttracks(self) -> List[Dict[str, Any]]:
        """
        Get all Moltbook tracks with IDs.
        
        Returns:
            List of dicts: [{"id": 1, "type": "twitter", "value": "clawpilot00"}, ...]
        """
        tracks = []
        track_data = await self.redis.hgetall("molttrack:list")
        
        if track_data:
            for track_id, track_json in track_data.items():
                track = json.loads(track_json)
                tracks.append({
                    "id": int(track_id),
                    "type": track["type"],
                    "value": track["value"],
                })
        
        # Sort by ID
        tracks.sort(key=lambda x: x["id"])
        return tracks
    
    async def has_molttrack(self, track_type: str, value: str) -> bool:
        """
        Check if a value matches any track of given type.
        
        Args:
            track_type: 'twitter', 'ticker', or 'name'
            value: The value to check
        
        Returns:
            True if matched, False otherwise
        """
        # Normalize value
        if track_type == "twitter":
            value = value.lower().strip().lstrip("@")
        elif track_type == "ticker":
            value = value.upper().strip().lstrip("$")
        elif track_type == "name":
            value = value.lower().strip()
        else:
            return False
        
        if not value:
            return False
        
        exists = await self.redis.exists(f"molttrack:{track_type}:{value}")
        return bool(exists)
    
    # =========================================================================
    # PRE-BANKR DEV BLACKLIST (with IDs)
    # =========================================================================
    
    async def add_prebankr_blacklist(self, handle: str) -> int:
        """
        Add a handle to the PRE-BANKR blacklist.
        Handles are stored case-insensitively (lowercase).
        
        Args:
            handle: Twitter handle (with or without @)
        
        Returns:
            Blacklist ID (int), or existing ID if already blacklisted
        """
        normalized = handle.lower().strip().lstrip("@")
        if not normalized:
            return 0
        
        # Check if already blacklisted
        existing_id = await self.redis.get(f"prebankr:blacklist:handle:{normalized}")
        if existing_id:
            return int(existing_id)  # Already blacklisted
        
        # Get next ID
        blacklist_id = await self.redis.incr("prebankr:blacklist:next_id")
        
        # Save blacklist data
        await self.redis.hset("prebankr:blacklist:list", {str(blacklist_id): normalized})
        
        # Create index for quick lookup by handle
        await self.redis.set(f"prebankr:blacklist:handle:{normalized}", str(blacklist_id))
        
        logger.info(f"[BLACKLIST] Added #{blacklist_id} @{normalized} to PRE-BANKR blacklist")
        return blacklist_id
    
    async def remove_prebankr_blacklist_by_id(self, blacklist_id: int) -> Optional[str]:
        """
        Remove a handle from the PRE-BANKR blacklist by ID.
        
        Args:
            blacklist_id: The ID of the blacklist entry
        
        Returns:
            Handle that was removed, or None if not found
        """
        # Get handle from list
        handle = await self.redis.hget("prebankr:blacklist:list", str(blacklist_id))
        if not handle:
            return None
        
        # Remove from list
        await self.redis.hdel("prebankr:blacklist:list", str(blacklist_id))
        
        # Remove index
        await self.redis.delete(f"prebankr:blacklist:handle:{handle}")
        
        logger.info(f"[BLACKLIST] Removed #{blacklist_id} @{handle} from PRE-BANKR blacklist")
        return handle
    
    async def remove_prebankr_blacklist(self, handle: str) -> bool:
        """
        Remove a handle from the PRE-BANKR blacklist by handle.
        
        Args:
            handle: Twitter handle (with or without @)
        
        Returns:
            True if removed, False if not found
        """
        normalized = handle.lower().strip().lstrip("@")
        if not normalized:
            return False
        
        # Get ID from index
        blacklist_id = await self.redis.get(f"prebankr:blacklist:handle:{normalized}")
        if not blacklist_id:
            return False
        
        # Remove from list
        await self.redis.hdel("prebankr:blacklist:list", blacklist_id)
        
        # Remove index
        await self.redis.delete(f"prebankr:blacklist:handle:{normalized}")
        
        logger.info(f"[BLACKLIST] Removed #{blacklist_id} @{normalized} from PRE-BANKR blacklist")
        return True
    
    async def is_prebankr_blacklisted(self, handle: str) -> bool:
        """
        Check if a handle is in the PRE-BANKR blacklist.
        Case-insensitive check.
        
        Args:
            handle: Twitter handle (with or without @)
        
        Returns:
            True if blacklisted, False otherwise
        """
        if not handle:
            return False
        normalized = handle.lower().strip().lstrip("@")
        exists = await self.redis.exists(f"prebankr:blacklist:handle:{normalized}")
        return bool(exists)
    
    async def list_prebankr_blacklist(self) -> List[Dict[str, Any]]:
        """
        Get all blacklisted handles with IDs.
        
        Returns:
            List of dicts: [{"id": 1, "handle": "0xdoha"}, ...]
        """
        entries = []
        data = await self.redis.hgetall("prebankr:blacklist:list")
        
        if data:
            for blacklist_id, handle in data.items():
                entries.append({
                    "id": int(blacklist_id),
                    "handle": handle,
                })
        
        # Sort by ID
        entries.sort(key=lambda x: x["id"])
        return entries
    
    async def clear_prebankr_blacklist(self) -> int:
        """
        Clear all blacklisted handles.
        
        Returns:
            Number of handles removed
        """
        # Get all handles to delete their indexes
        data = await self.redis.hgetall("prebankr:blacklist:list")
        count = len(data) if data else 0
        
        if data:
            # Delete all handle indexes
            for handle in data.values():
                await self.redis.delete(f"prebankr:blacklist:handle:{handle}")
        
        # Delete list and next_id
        await self.redis.delete("prebankr:blacklist:list")
        await self.redis.delete("prebankr:blacklist:next_id")
        
        logger.info(f"[BLACKLIST] Cleared PRE-BANKR blacklist ({count} handles)")
        return count
    
    # =========================================================================
    # PRE-BANKR WORD BLACKLIST
    # =========================================================================
    
    async def add_prebankr_word_blacklist(self, word: str) -> int:
        """
        Add a word to the PRE-BANKR word blacklist.
        Words are stored lowercase for case-insensitive matching.
        
        Args:
            word: Word to blacklist
        
        Returns:
            Blacklist ID (int), or existing ID if already blacklisted
        """
        normalized = word.lower().strip()
        if not normalized:
            return 0
        
        # Check if already blacklisted
        existing_id = await self.redis.get(f"prebankr:wordblacklist:word:{normalized}")
        if existing_id:
            return int(existing_id)  # Already blacklisted
        
        # Get next ID
        blacklist_id = await self.redis.incr("prebankr:wordblacklist:next_id")
        
        # Save blacklist data
        await self.redis.hset("prebankr:wordblacklist:list", {str(blacklist_id): normalized})
        
        # Create index for quick lookup
        await self.redis.set(f"prebankr:wordblacklist:word:{normalized}", str(blacklist_id))
        
        logger.info(f"[WORDBLACKLIST] Added #{blacklist_id} '{normalized}' to PRE-BANKR word blacklist")
        return blacklist_id
    
    async def remove_prebankr_word_blacklist_by_id(self, blacklist_id: int) -> Optional[str]:
        """
        Remove a word from the PRE-BANKR word blacklist by ID.
        
        Returns:
            Word that was removed, or None if not found
        """
        word = await self.redis.hget("prebankr:wordblacklist:list", str(blacklist_id))
        if not word:
            return None
        
        await self.redis.hdel("prebankr:wordblacklist:list", str(blacklist_id))
        await self.redis.delete(f"prebankr:wordblacklist:word:{word}")
        
        logger.info(f"[WORDBLACKLIST] Removed #{blacklist_id} '{word}' from PRE-BANKR word blacklist")
        return word
    
    async def list_prebankr_word_blacklist(self) -> List[Dict[str, Any]]:
        """
        Get all blacklisted words with IDs.
        
        Returns:
            List of dicts: [{"id": 1, "word": "solana"}, ...]
        """
        entries = []
        data = await self.redis.hgetall("prebankr:wordblacklist:list")
        
        if data:
            for blacklist_id, word in data.items():
                entries.append({
                    "id": int(blacklist_id),
                    "word": word,
                })
        
        entries.sort(key=lambda x: x["id"])
        return entries
    
    async def check_prebankr_word_blacklist(self, text: str) -> Optional[str]:
        """
        Check if text contains any blacklisted word.
        Case-insensitive check.
        
        Args:
            text: Text to check (token name, symbol, tweet text, etc.)
        
        Returns:
            First matched word if found, None otherwise
        """
        if not text:
            return None
        
        text_lower = text.lower()
        words = await self.list_prebankr_word_blacklist()
        
        for entry in words:
            if entry["word"] in text_lower:
                return entry["word"]
        
        return None


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_storage: Optional[Storage] = None


async def get_storage() -> Storage:
    """Get or create global storage instance."""
    global _storage
    if _storage is None:
        redis = await get_redis()
        _storage = Storage(redis)
    return _storage
