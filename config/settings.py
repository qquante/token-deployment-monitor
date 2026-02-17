"""
Configuration settings for Bankr Monitor.

This module centralizes all configuration parameters including:
- RPC endpoints
- Contract addresses
- Event signatures
- Query parameters
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class RPCConfig:
    """RPC connection configuration."""
    
    # Base mainnet RPC endpoints (fallback order)
    ENDPOINTS = [
        "https://mainnet.base.org",
        "https://base.drpc.org",
        "https://1rpc.io/base",
    ]
    
    # Default endpoint (can be overridden via env var)
    DEFAULT_ENDPOINT: str = os.getenv("BASE_RPC", ENDPOINTS[0])
    
    # Request configuration
    TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0


@dataclass
class ContractConfig:
    """Smart contract addresses on Base."""
    
    # Clanker v4.0.0 - Main deployer contract
    CLANKER_V4: str = "0xE85A59c628F7d27878ACeB4bf3b35733630083a9"
    
    # LP Locker Fee Conversion v4.0.0
    LP_LOCKER: str = "0x63D2DfEA64b3433F4071A98665bcD7Ca14d93496"
    
    # Hook Static Fee V2 v4.1.0
    POOL_HOOK: str = "0xb429d62f8f3bFFb98CdB9569533eA23bF0Ba28CC"
    
    # Sniper Auction V2 v4.1.0 (MEV Module)
    MEV_MODULE: str = "0xebB25BB797D82CB78E1bc70406b13233c0854413"
    
    # Uniswap V4 contracts
    UNISWAP_POOL_MANAGER: str = "0x498581fF718922c3f8e6A244956aF099B2652b2b"
    UNISWAP_POSITION_MANAGER: str = "0x7C5f5A4bBd8fD63184577525326123B519429bDc"
    UNISWAP_PERMIT2: str = "0x000000000022D473030F116dDEE9F6B43aC78BA3"
    
    # WETH on Base
    WETH: str = "0x4200000000000000000000000000000000000006"


@dataclass
class EventConfig:
    """Event signatures and topics."""
    
    # TokenCreated event from Clanker v4.0.0
    TOKEN_CREATED_TOPIC: str = "0x9299d1d1a88d8e1abdc591ae7a167a6bc63a8f17d695804e9091ee33aa89fb67"
    
    # TokenCreated event signature
    TOKEN_CREATED_SIGNATURE: str = (
        "TokenCreated("
        "address msgSender,"
        "address indexed tokenAddress,"
        "address indexed tokenAdmin,"
        "string tokenImage,"
        "string tokenName,"
        "string tokenSymbol,"
        "string tokenMetadata,"
        "string tokenContext,"
        "int24 startingTick,"
        "address poolHook,"
        "bytes32 poolId,"
        "address pairedToken,"
        "address locker,"
        "address mevModule,"
        "uint256 extensionsSupply,"
        "address[] extensions"
        ")"
    )
    
    # ABI types for decoding (non-indexed parameters)
    TOKEN_CREATED_ABI_TYPES: list = None
    
    def __post_init__(self):
        self.TOKEN_CREATED_ABI_TYPES = [
            'address',      # msgSender
            'string',       # tokenImage
            'string',       # tokenName
            'string',       # tokenSymbol
            'string',       # tokenMetadata
            'string',       # tokenContext
            'int24',        # startingTick
            'address',      # poolHook
            'bytes32',      # poolId
            'address',      # pairedToken
            'address',      # locker
            'address',      # mevModule
            'uint256',      # extensionsSupply
            'address[]',    # extensions
        ]


@dataclass
class QueryConfig:
    """Query parameters for log fetching."""
    
    # Block chunk size (Base RPC typically allows 2000-5000)
    CHUNK_SIZE: int = 2000
    
    # Rate limiting delay between requests (seconds)
    RATE_LIMIT_DELAY: float = 0.1
    
    # Default number of blocks to query
    DEFAULT_BLOCK_RANGE: int = 100_000
    
    # Target number of Bankr deployments to fetch
    TARGET_DEPLOYMENTS: Optional[int] = None


@dataclass
class BankrConfig:
    """Bankr-specific identification patterns."""
    
    # Interface identifier in tokenContext
    INTERFACE_NAME: str = "Bankr"
    
    # Known Bankr deployer addresses
    DEPLOYERS: list = None
    
    # Platform identifiers
    PLATFORMS: list = None
    
    # Description patterns
    DESCRIPTION_PATTERNS: list = None
    
    def __post_init__(self):
        self.DEPLOYERS = [
            "0x2112b8456ac07c15fa31ddf3bf713e77716ff3f9",  # Main Bankr deployer
            "0x10904ad5228820a81ece2661f40dbd987469fccb",  # Secondary
            "0x88075f206d78434c1b8c6b694581a09895f58ca9",  # X Bot deployer
        ]
        
        self.PLATFORMS = [
            "farcaster",
            "X",
            "x",
            "Bankr",
        ]
        
        self.DESCRIPTION_PATTERNS = [
            "Deployed through the Bankr Terminal @ bankr.bot",
            "Deployed using @bankrbot on X",
            "Deployed using Bankr on XMTP",
            "Deployed using Bankr on Farcaster",
            "Deployed using Bankr",
        ]


# Global configuration instances
rpc_config = RPCConfig()
contract_config = ContractConfig()
event_config = EventConfig()
query_config = QueryConfig()
bankr_config = BankrConfig()
