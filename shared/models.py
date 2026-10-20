"""
Data models for Redis storage.

All models are stored as Redis hashes with helper methods
for serialization/deserialization.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)


# FEE SCHEDULE

FEE_SCHEDULE = {
    0: 66.7,   # Auction acabou de expirar
    1: 58.3,   # Primeiro bloco decay
    2: 50.0,
    3: 41.7,
    4: 33.3,
    5: 25.0,
    6: 16.7,
    7: 8.3,
    8: 4.2,    # Fee minima
    9: 4.2,
    10: 4.2,
}


def get_fee_for_block(blocks_after_expire: int) -> float:
    """Get fee % for X blocks after auction expires."""
    if blocks_after_expire in FEE_SCHEDULE:
        return FEE_SCHEDULE[blocks_after_expire]
    elif blocks_after_expire >= 8:
        return 4.2
    else:
        return 66.7


# USER CONFIG

@dataclass
class UserConfig:
    """
    Per-user trading configuration.
    
    Redis key: user:{user_id}
    """
    
    user_id: int
    username: str = ""
    
    # Wallet
    wallet_address: str = ""
    wallet_private_key: str = ""  # NEVER log this!
    
    # Trading params
    eth_amount: float = 0.01
    block_offset: int = 6
    gwei: float = 0.1
    slippage: float = 5.0
    
    # Toggles
    auto_buy: bool = False
    alerts_enabled: bool = True
    
    # Pre-approval: max ETH to auto-approve (0 = disabled)
    max_eth_approved: float = 0.0
    
    # Block tolerance: how many blocks late can still execute (0 = exact only)
    block_tolerance: int = 1
    
    # Sniper triggers: auto-buy when these Twitter handles create tokens
    sniper_handles: List[str] = field(default_factory=list)
    
    # Buy TTL: max time (minutes) to wait for token to go on-chain (0 = forever)
    buy_ttl_minutes: int = 60
    
    # MoltX Triggers (auto-buy when conditions match)
    moltx_trigger_enabled: bool = False
    moltx_trigger_handles: List[str] = field(default_factory=list)  # Specific handles to snipe
    moltx_trigger_min_followers: int = 0  # Min Twitter followers (0 = disabled)
    moltx_trigger_min_moltx_followers: int = 0  # Min MoltX followers (0 = disabled)
    moltx_trigger_min_smart_followers: int = 0  # Min smart followers (0 = disabled)
    moltx_trigger_x_verified: bool = False  # Only buy from verified accounts
    
    # MoltBook Triggers (auto-buy when conditions match)
    moltbook_trigger_enabled: bool = False
    moltbook_trigger_handles: List[str] = field(default_factory=list)  # Specific handles to snipe
    moltbook_trigger_min_followers: int = 0  # Min Twitter followers (0 = disabled)
    moltbook_trigger_min_moltx_followers: int = 0  # Min MoltX followers (0 = disabled)
    moltbook_trigger_min_smart_followers: int = 0  # Min smart followers (0 = disabled)
    moltbook_trigger_min_karma: int = 0  # Min MoltBook karma (0 = disabled)
    moltbook_trigger_x_verified: bool = False  # Only buy from verified accounts
    
    # Stats
    total_buys: int = 0
    total_spent: float = 0.0
    
    def is_configured(self) -> bool:
        """Check if user has minimum config for trading."""
        return bool(self.wallet_address and self.wallet_private_key)
    
    def get_missing_config(self) -> List[str]:
        """Get list of missing required config."""
        missing = []
        if not self.wallet_address:
            missing.append("wallet")
        if not self.wallet_private_key:
            missing.append("private_key")
        return missing
    
    def to_redis(self) -> Dict[str, str]:
        """Convert to Redis hash mapping."""
        return {
            "user_id": str(self.user_id),
            "username": self.username,
            "wallet_address": self.wallet_address,
            "wallet_private_key": self.wallet_private_key,
            "eth_amount": str(self.eth_amount),
            "block_offset": str(self.block_offset),
            "gwei": str(self.gwei),
            "slippage": str(self.slippage),
            "auto_buy": "1" if self.auto_buy else "0",
            "alerts_enabled": "1" if self.alerts_enabled else "0",
            "max_eth_approved": str(self.max_eth_approved),
            "block_tolerance": str(self.block_tolerance),
            "sniper_handles": ",".join(self.sniper_handles) if self.sniper_handles else "",
            "buy_ttl_minutes": str(self.buy_ttl_minutes),
            # MoltX triggers
            "moltx_trigger_enabled": "1" if self.moltx_trigger_enabled else "0",
            "moltx_trigger_handles": ",".join(self.moltx_trigger_handles) if self.moltx_trigger_handles else "",
            "moltx_trigger_min_followers": str(self.moltx_trigger_min_followers),
            "moltx_trigger_min_moltx_followers": str(self.moltx_trigger_min_moltx_followers),
            "moltx_trigger_min_smart_followers": str(self.moltx_trigger_min_smart_followers),
            "moltx_trigger_x_verified": "1" if self.moltx_trigger_x_verified else "0",
            # MoltBook triggers
            "moltbook_trigger_enabled": "1" if self.moltbook_trigger_enabled else "0",
            "moltbook_trigger_handles": ",".join(self.moltbook_trigger_handles) if self.moltbook_trigger_handles else "",
            "moltbook_trigger_min_followers": str(self.moltbook_trigger_min_followers),
            "moltbook_trigger_min_moltx_followers": str(self.moltbook_trigger_min_moltx_followers),
            "moltbook_trigger_min_smart_followers": str(self.moltbook_trigger_min_smart_followers),
            "moltbook_trigger_min_karma": str(self.moltbook_trigger_min_karma),
            "moltbook_trigger_x_verified": "1" if self.moltbook_trigger_x_verified else "0",
            # Stats
            "total_buys": str(self.total_buys),
            "total_spent": str(self.total_spent),
        }
    
    @classmethod
    def from_redis(cls, data: Dict[str, str]) -> "UserConfig":
        """Create from Redis hash."""
        if not data:
            raise ValueError("Empty data")
        
        return cls(
            user_id=int(data.get("user_id", 0)),
            username=data.get("username", ""),
            wallet_address=data.get("wallet_address", ""),
            wallet_private_key=data.get("wallet_private_key", ""),
            eth_amount=float(data.get("eth_amount", 0.01)),
            block_offset=int(data.get("block_offset", 6)),
            gwei=float(data.get("gwei", 0.1)),
            slippage=float(data.get("slippage", 5.0)),
            auto_buy=data.get("auto_buy") == "1",
            alerts_enabled=data.get("alerts_enabled", "1") == "1",
            max_eth_approved=float(data.get("max_eth_approved", 0.0)),
            block_tolerance=int(data.get("block_tolerance", 1)),
            sniper_handles=[h.strip() for h in data.get("sniper_handles", "").split(",") if h.strip()],
            buy_ttl_minutes=int(data.get("buy_ttl_minutes", 60)),
            # MoltX triggers
            moltx_trigger_enabled=data.get("moltx_trigger_enabled") == "1",
            moltx_trigger_handles=[h.strip() for h in data.get("moltx_trigger_handles", "").split(",") if h.strip()],
            moltx_trigger_min_followers=int(data.get("moltx_trigger_min_followers", 0)),
            moltx_trigger_min_moltx_followers=int(data.get("moltx_trigger_min_moltx_followers", 0)),
            moltx_trigger_min_smart_followers=int(data.get("moltx_trigger_min_smart_followers", 0)),
            moltx_trigger_x_verified=data.get("moltx_trigger_x_verified") == "1",
            # MoltBook triggers
            moltbook_trigger_enabled=data.get("moltbook_trigger_enabled") == "1",
            moltbook_trigger_handles=[h.strip() for h in data.get("moltbook_trigger_handles", "").split(",") if h.strip()],
            moltbook_trigger_min_followers=int(data.get("moltbook_trigger_min_followers", 0)),
            moltbook_trigger_min_moltx_followers=int(data.get("moltbook_trigger_min_moltx_followers", 0)),
            moltbook_trigger_min_smart_followers=int(data.get("moltbook_trigger_min_smart_followers", 0)),
            moltbook_trigger_min_karma=int(data.get("moltbook_trigger_min_karma", 0)),
            moltbook_trigger_x_verified=data.get("moltbook_trigger_x_verified") == "1",
            # Stats
            total_buys=int(data.get("total_buys", 0)),
            total_spent=float(data.get("total_spent", 0.0)),
        )
    
    @staticmethod
    def redis_key(user_id: int) -> str:
        """Get Redis key for user."""
        return f"user:{user_id}"


# PENDING BUY

@dataclass
class PendingBuy:
    """
    A pending buy waiting for target block.
    
    Redis key: buy:{user_id}:{tweet_id}
    Index sets:
        - buys_by_user:{user_id}
        - buys_by_status:{status}
        - buys_by_target_block:{block}
        - buys_by_token:{token_address}
    """
    
    # Identifiers
    user_id: int
    tweet_id: str
    username: str = ""
    
    # Token info
    token_address: str = ""
    token_symbol: str = ""
    token_name: str = ""
    pool_id: str = ""
    
    # Trading params (copied from user config at approval time)
    eth_amount: float = 0.01
    block_offset: int = 6
    gwei: float = 0.1
    slippage: float = 5.0
    
    # Calculated
    auction_expire_block: int = 0
    target_block: int = 0
    fee_expected: float = 0.0
    
    # Status: APPROVED -> WAITING -> EXECUTING -> DONE/FAILED/CANCELLED
    status: str = "APPROVED"
    
    # Timestamps
    created_at: str = ""
    approved_at: str = ""
    linked_at: str = ""
    executed_at: str = ""
    
    # Result
    tx_hash: str = ""
    error: str = ""
    gas_used: int = 0
    actual_fee: float = 0.0
    executed_block: int = 0  # Block at which trade actually executed
    
    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
    
    @property
    def buy_key(self) -> str:
        """Unique key for this buy: user_id:tweet_id"""
        return f"{self.user_id}:{self.tweet_id}"
    
    def to_redis(self) -> Dict[str, str]:
        """Convert to Redis hash mapping."""
        return {
            "user_id": str(self.user_id),
            "tweet_id": self.tweet_id,
            "username": self.username,
            "token_address": self.token_address,
            "token_symbol": self.token_symbol,
            "token_name": self.token_name,
            "pool_id": self.pool_id,
            "eth_amount": str(self.eth_amount),
            "block_offset": str(self.block_offset),
            "gwei": str(self.gwei),
            "slippage": str(self.slippage),
            "auction_expire_block": str(self.auction_expire_block),
            "target_block": str(self.target_block),
            "fee_expected": str(self.fee_expected),
            "status": self.status,
            "created_at": self.created_at,
            "approved_at": self.approved_at,
            "linked_at": self.linked_at,
            "executed_at": self.executed_at,
            "tx_hash": self.tx_hash,
            "error": self.error,
            "gas_used": str(self.gas_used),
            "actual_fee": str(self.actual_fee),
            "executed_block": str(self.executed_block),
        }
    
    @classmethod
    def from_redis(cls, data: Dict[str, str]) -> "PendingBuy":
        """Create from Redis hash."""
        if not data:
            raise ValueError("Empty data")
        
        return cls(
            user_id=int(data.get("user_id", 0)),
            tweet_id=data.get("tweet_id", ""),
            username=data.get("username", ""),
            token_address=data.get("token_address", ""),
            token_symbol=data.get("token_symbol", ""),
            token_name=data.get("token_name", ""),
            pool_id=data.get("pool_id", ""),
            eth_amount=float(data.get("eth_amount", 0.01)),
            block_offset=int(data.get("block_offset", 6)),
            gwei=float(data.get("gwei", 0.1)),
            slippage=float(data.get("slippage", 5.0)),
            auction_expire_block=int(data.get("auction_expire_block", 0)),
            target_block=int(data.get("target_block", 0)),
            fee_expected=float(data.get("fee_expected", 0.0)),
            status=data.get("status", "APPROVED"),
            created_at=data.get("created_at", ""),
            approved_at=data.get("approved_at", ""),
            linked_at=data.get("linked_at", ""),
            executed_at=data.get("executed_at", ""),
            tx_hash=data.get("tx_hash", ""),
            error=data.get("error", ""),
            gas_used=int(data.get("gas_used", 0)),
            actual_fee=float(data.get("actual_fee", 0.0)),
            executed_block=int(data.get("executed_block", 0)),
        )
    
    @staticmethod
    def redis_key(user_id: int, tweet_id: str) -> str:
        """Get Redis key for buy."""
        return f"buy:{user_id}:{tweet_id}"


# TOKEN INFO (cached from on-chain/twitter)

@dataclass
class TokenInfo:
    """
    Token information cache.
    
    Redis key: token:{tweet_id}
    """
    
    tweet_id: str
    token_address: str = ""
    token_symbol: str = ""
    token_name: str = ""
    pool_id: str = ""
    
    # Creator info
    creator_wallet: str = ""
    platform: str = "X"  # X or farcaster
    
    # Tweet info
    tweet_url: str = ""
    from_username: str = ""
    to_usernames: List[str] = field(default_factory=list)
    
    # Auction info
    auction_expire_block: int = 0
    
    # Status
    on_chain: bool = False
    deployed: bool = False
    
    # Timestamps
    prebankr_at: str = ""
    onchain_at: str = ""
    deployed_at: str = ""
    
    def to_redis(self) -> Dict[str, str]:
        """Convert to Redis hash mapping."""
        return {
            "tweet_id": self.tweet_id,
            "token_address": self.token_address,
            "token_symbol": self.token_symbol,
            "token_name": self.token_name,
            "pool_id": self.pool_id,
            "creator_wallet": self.creator_wallet,
            "platform": self.platform,
            "tweet_url": self.tweet_url,
            "from_username": self.from_username,
            "to_usernames": json.dumps(self.to_usernames),
            "auction_expire_block": str(self.auction_expire_block),
            "on_chain": "1" if self.on_chain else "0",
            "deployed": "1" if self.deployed else "0",
            "prebankr_at": self.prebankr_at,
            "onchain_at": self.onchain_at,
            "deployed_at": self.deployed_at,
        }
    
    @classmethod
    def from_redis(cls, data: Dict[str, str]) -> "TokenInfo":
        """Create from Redis hash."""
        if not data:
            raise ValueError("Empty data")
        
        to_usernames = data.get("to_usernames", "[]")
        try:
            to_usernames = json.loads(to_usernames)
        except:
            to_usernames = []
        
        return cls(
            tweet_id=data.get("tweet_id", ""),
            token_address=data.get("token_address", ""),
            token_symbol=data.get("token_symbol", ""),
            token_name=data.get("token_name", ""),
            pool_id=data.get("pool_id", ""),
            creator_wallet=data.get("creator_wallet", ""),
            platform=data.get("platform", "X"),
            tweet_url=data.get("tweet_url", ""),
            from_username=data.get("from_username", ""),
            to_usernames=to_usernames,
            auction_expire_block=int(data.get("auction_expire_block", 0)),
            on_chain=data.get("on_chain") == "1",
            deployed=data.get("deployed") == "1",
            prebankr_at=data.get("prebankr_at", ""),
            onchain_at=data.get("onchain_at", ""),
            deployed_at=data.get("deployed_at", ""),
        )
    
    @staticmethod
    def redis_key(tweet_id: str) -> str:
        """Get Redis key for token."""
        return f"token:{tweet_id}"


# ENRICHED USER (cached API data)

@dataclass
class EnrichedUser:
    """
    Cached enriched user data from Twitter/AlphaGate/ENS.
    
    Redis key: enriched_user:{username}
    TTL: 1 hour
    """
    
    username: str
    user_id: str = ""
    name: str = ""
    followers: int = 0
    following: int = 0
    is_blue_verified: bool = False
    verified_type: str = ""
    created_at: str = ""
    bio: str = ""
    profile_image_url: str = ""
    
    # ENS
    ens: str = ""
    wallet: str = ""
    
    # AlphaGate
    smart_followers: int = 0
    prev_usernames_count: int = 0  # R
    contracts_count: int = 0        # C
    affiliate_label: str = ""
    
    # Cache metadata
    cached_at: str = ""
    
    def to_redis(self) -> Dict[str, str]:
        """Convert to Redis hash mapping."""
        return {
            "username": self.username,
            "user_id": self.user_id,
            "name": self.name,
            "followers": str(self.followers),
            "following": str(self.following),
            "is_blue_verified": "1" if self.is_blue_verified else "0",
            "verified_type": self.verified_type or "",
            "created_at": self.created_at,
            "bio": self.bio,
            "profile_image_url": self.profile_image_url,
            "ens": self.ens,
            "wallet": self.wallet,
            "smart_followers": str(self.smart_followers),
            "prev_usernames_count": str(self.prev_usernames_count),
            "contracts_count": str(self.contracts_count),
            "affiliate_label": self.affiliate_label,
            "cached_at": self.cached_at or datetime.now().isoformat(),
        }
    
    @classmethod
    def from_redis(cls, data: Dict[str, str]) -> "EnrichedUser":
        """Create from Redis hash."""
        if not data:
            raise ValueError("Empty data")
        
        return cls(
            username=data.get("username", ""),
            user_id=data.get("user_id", ""),
            name=data.get("name", ""),
            followers=int(data.get("followers", 0)),
            following=int(data.get("following", 0)),
            is_blue_verified=data.get("is_blue_verified") == "1",
            verified_type=data.get("verified_type", ""),
            created_at=data.get("created_at", ""),
            bio=data.get("bio", ""),
            profile_image_url=data.get("profile_image_url", ""),
            ens=data.get("ens", ""),
            wallet=data.get("wallet", ""),
            smart_followers=int(data.get("smart_followers", 0)),
            prev_usernames_count=int(data.get("prev_usernames_count", 0)),
            contracts_count=int(data.get("contracts_count", 0)),
            affiliate_label=data.get("affiliate_label", ""),
            cached_at=data.get("cached_at", ""),
        )
    
    @staticmethod
    def redis_key(username: str) -> str:
        """Get Redis key for enriched user."""
        return f"enriched_user:{username.lower()}"
