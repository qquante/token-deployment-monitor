"""
Base blockchain RPC client.

This module provides a high-level interface for interacting with
the Base blockchain via JSON-RPC. Features include:
- Automatic connection management
- Paginated log queries
- Rate limiting and retry logic
"""

import time
import logging
from typing import List, Dict, Any, Optional, Generator

from web3 import Web3
from web3.types import LogReceipt

try:
    from config import rpc_config, contract_config, event_config, query_config
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from config import rpc_config, contract_config, event_config, query_config

logger = logging.getLogger(__name__)


class BaseRPCClient:
    """
    RPC client for Base blockchain interactions.
    
    Handles connection management, log queries, and pagination
    for efficient data retrieval from the Base L2 network.
    
    Attributes:
        w3: Web3 instance
        endpoint: Current RPC endpoint
        chunk_size: Block range per query
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        chunk_size: Optional[int] = None
    ):
        """
        Initialize RPC client.
        
        Args:
            endpoint: RPC endpoint URL (defaults to config)
            chunk_size: Blocks per query (defaults to config)
        """
        self.endpoint = endpoint or rpc_config.DEFAULT_ENDPOINT
        self.chunk_size = chunk_size or query_config.CHUNK_SIZE
        self._w3: Optional[Web3] = None
        
    @property
    def w3(self) -> Web3:
        """Lazy-loaded Web3 instance."""
        if self._w3 is None:
            self._w3 = Web3(Web3.HTTPProvider(self.endpoint))
        return self._w3
    
    def is_connected(self) -> bool:
        """Check if connected to RPC endpoint."""
        try:
            return self.w3.is_connected()
        except Exception:
            return False
    
    def get_block_number(self) -> int:
        """Get current block number."""
        return self.w3.eth.block_number
    
    def get_logs(
        self,
        contract_address: str,
        topics: List[str],
        from_block: int,
        to_block: int
    ) -> List[LogReceipt]:
        """
        Get logs for a single block range.
        
        Args:
            contract_address: Contract to query
            topics: Event topics filter
            from_block: Start block
            to_block: End block
            
        Returns:
            List of log receipts
        """
        return self.w3.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': to_block,
            'address': Web3.to_checksum_address(contract_address),
            'topics': topics
        })
    
    def get_logs_paginated(
        self,
        contract_address: str,
        topics: List[str],
        from_block: int,
        to_block: int,
        callback: Optional[callable] = None
    ) -> Generator[List[LogReceipt], None, None]:
        """
        Get logs with automatic pagination.
        
        Splits large block ranges into chunks to avoid RPC limits.
        Yields results incrementally for memory efficiency.
        
        Args:
            contract_address: Contract to query
            topics: Event topics filter
            from_block: Start block
            to_block: End block
            callback: Optional progress callback(current_block, total_logs)
            
        Yields:
            Batches of log receipts
        """
        current_from = from_block
        total_logs = 0
        chunk_size = self.chunk_size
        
        while current_from < to_block:
            current_to = min(current_from + chunk_size - 1, to_block)
            
            try:
                logs = self.get_logs(
                    contract_address,
                    topics,
                    current_from,
                    current_to
                )
                total_logs += len(logs)
                
                if callback:
                    callback(current_to, total_logs)
                
                yield logs
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Handle "block range too large" error
                if "too large" in error_msg:
                    chunk_size = chunk_size // 2
                    logger.warning(f"Reducing chunk size to {chunk_size}")
                    if chunk_size < 100:
                        raise ValueError("Chunk size too small, cannot continue")
                    continue
                    
                # Re-raise other errors
                raise
            
            current_from = current_to + 1
            time.sleep(query_config.RATE_LIMIT_DELAY)
    
    def get_all_logs(
        self,
        contract_address: str,
        topics: List[str],
        from_block: int,
        to_block: int,
        callback: Optional[callable] = None
    ) -> List[LogReceipt]:
        """
        Get all logs for a block range (non-generator version).
        
        Collects all paginated results into a single list.
        Use get_logs_paginated() for memory-efficient streaming.
        
        Args:
            contract_address: Contract to query
            topics: Event topics filter
            from_block: Start block
            to_block: End block
            callback: Optional progress callback
            
        Returns:
            Complete list of log receipts
        """
        all_logs = []
        for batch in self.get_logs_paginated(
            contract_address, topics, from_block, to_block, callback
        ):
            all_logs.extend(batch)
        return all_logs
    
    def get_token_created_logs(
        self,
        num_blocks: Optional[int] = None,
        from_block: Optional[int] = None,
        to_block: Optional[int] = None,
        callback: Optional[callable] = None
    ) -> List[LogReceipt]:
        """
        Get TokenCreated events from Clanker contract.
        
        Convenience method for fetching token deployment events.
        
        Args:
            num_blocks: Number of recent blocks to query
            from_block: Explicit start block (overrides num_blocks)
            to_block: Explicit end block (defaults to current)
            callback: Optional progress callback
            
        Returns:
            List of TokenCreated log receipts
        """
        if to_block is None:
            to_block = self.get_block_number()
            
        if from_block is None:
            if num_blocks is None:
                num_blocks = query_config.DEFAULT_BLOCK_RANGE
            from_block = to_block - num_blocks
        
        logger.info(f"Querying blocks {from_block} to {to_block}")
        
        return self.get_all_logs(
            contract_address=contract_config.CLANKER_V4,
            topics=[event_config.TOKEN_CREATED_TOPIC],
            from_block=from_block,
            to_block=to_block,
            callback=callback
        )
