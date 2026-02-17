"""
Event decoder for Clanker TokenCreated events.

This module handles the decoding of raw blockchain logs into
structured Python objects. Supports both indexed (topics) and
non-indexed (data) event parameters.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

from eth_abi import decode
from web3.types import LogReceipt

try:
    from config import event_config
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from config import event_config

logger = logging.getLogger(__name__)


@dataclass
class DecodedTokenEvent:
    """
    Decoded TokenCreated event data.
    
    Contains all fields from the TokenCreated event including
    parsed JSON from tokenMetadata and tokenContext.
    
    Attributes:
        token_address: Deployed token contract address
        token_admin: Admin address for the token
        msg_sender: Address that initiated the deployment
        token_image: URL to token image (if provided)
        token_name: Token name
        token_symbol: Token symbol/ticker
        token_metadata: Raw JSON metadata string
        token_context: Raw JSON context string
        starting_tick: Uniswap pool starting tick
        pool_hook: Pool hook contract address
        pool_id: Uniswap pool identifier
        paired_token: Token paired in liquidity pool (usually WETH)
        locker: LP locker contract address
        mev_module: MEV protection module address
        extensions_supply: Token supply allocated to extensions
        extensions: List of extension contract addresses
        block_number: Block where event was emitted
        tx_hash: Transaction hash
        context_parsed: Parsed tokenContext JSON
        metadata_parsed: Parsed tokenMetadata JSON
    """
    
    # Indexed parameters (from topics)
    token_address: str
    token_admin: str
    
    # Non-indexed parameters (from data)
    msg_sender: str
    token_image: str
    token_name: str
    token_symbol: str
    token_metadata: str
    token_context: str
    starting_tick: int
    pool_hook: str
    pool_id: str
    paired_token: str
    locker: str
    mev_module: str
    extensions_supply: int
    extensions: List[str]
    
    # Block metadata
    block_number: int
    tx_hash: str
    
    # Parsed JSON fields
    context_parsed: Dict[str, Any] = field(default_factory=dict)
    metadata_parsed: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'token_address': self.token_address,
            'token_admin': self.token_admin,
            'msg_sender': self.msg_sender,
            'token_image': self.token_image,
            'token_name': self.token_name,
            'token_symbol': self.token_symbol,
            'token_metadata': self.token_metadata,
            'token_context': self.token_context,
            'starting_tick': self.starting_tick,
            'pool_hook': self.pool_hook,
            'pool_id': self.pool_id,
            'paired_token': self.paired_token,
            'locker': self.locker,
            'mev_module': self.mev_module,
            'extensions_supply': self.extensions_supply,
            'extensions': self.extensions,
            'block_number': self.block_number,
            'tx_hash': self.tx_hash,
            'context_parsed': self.context_parsed,
            'metadata_parsed': self.metadata_parsed,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DecodedTokenEvent':
        """Create instance from dictionary."""
        return cls(**data)


class TokenCreatedDecoder:
    """
    Decoder for TokenCreated events.
    
    Handles the low-level ABI decoding of event logs and
    extraction of both indexed and non-indexed parameters.
    
    Example:
        decoder = TokenCreatedDecoder()
        for log in logs:
            event = decoder.decode(log)
            if event:
                print(f"Token: {event.token_name}")
    """
    
    def __init__(self):
        """Initialize decoder with ABI types from config."""
        self.abi_types = event_config.TOKEN_CREATED_ABI_TYPES
    
    def decode(self, log: LogReceipt) -> Optional[DecodedTokenEvent]:
        """
        Decode a single TokenCreated log.
        
        Args:
            log: Raw log receipt from RPC
            
        Returns:
            DecodedTokenEvent or None if decoding fails
        """
        try:
            # Extract indexed parameters from topics
            # topics[0] = event signature
            # topics[1] = tokenAddress (indexed)
            # topics[2] = tokenAdmin (indexed)
            token_address = self._extract_address_from_topic(log['topics'][1])
            token_admin = self._extract_address_from_topic(log['topics'][2])
            
            # Decode non-indexed parameters from data
            decoded_data = decode(self.abi_types, log['data'])
            
            # Parse JSON fields
            context_parsed = self._parse_json(decoded_data[5])
            metadata_parsed = self._parse_json(decoded_data[4])
            
            return DecodedTokenEvent(
                token_address=token_address,
                token_admin=token_admin,
                msg_sender=decoded_data[0],
                token_image=decoded_data[1],
                token_name=decoded_data[2],
                token_symbol=decoded_data[3],
                token_metadata=decoded_data[4],
                token_context=decoded_data[5],
                starting_tick=decoded_data[6],
                pool_hook=decoded_data[7],
                pool_id=decoded_data[8].hex(),
                paired_token=decoded_data[9],
                locker=decoded_data[10],
                mev_module=decoded_data[11],
                extensions_supply=decoded_data[12],
                extensions=list(decoded_data[13]),
                block_number=log['blockNumber'],
                tx_hash=log['transactionHash'].hex(),
                context_parsed=context_parsed,
                metadata_parsed=metadata_parsed,
            )
            
        except Exception as e:
            logger.error(f"Failed to decode log: {e}")
            return None
    
    def decode_many(
        self,
        logs: List[LogReceipt],
        skip_errors: bool = True
    ) -> List[DecodedTokenEvent]:
        """
        Decode multiple logs.
        
        Args:
            logs: List of raw log receipts
            skip_errors: If True, skip failed decodes; if False, raise
            
        Returns:
            List of successfully decoded events
        """
        events = []
        for log in logs:
            event = self.decode(log)
            if event:
                events.append(event)
            elif not skip_errors:
                raise ValueError(f"Failed to decode log at block {log['blockNumber']}")
        return events
    
    @staticmethod
    def _extract_address_from_topic(topic: bytes) -> str:
        """Extract address from 32-byte topic."""
        return "0x" + topic.hex()[-40:]
    
    @staticmethod
    def _parse_json(json_string: str) -> Dict[str, Any]:
        """Safely parse JSON string."""
        try:
            return json.loads(json_string) if json_string else {}
        except json.JSONDecodeError:
            return {}
